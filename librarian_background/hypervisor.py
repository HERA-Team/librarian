"""
The hypervisor task that checks on the status of outgoing transfers.

If they are stale, we call up the downstream librarian to ask for an 
update on their status. This can lead to a:

a) Failure of the outgoing transfer
b) Successful transfer, if the file is found on the downstream
"""

import datetime

from sqlalchemy.orm import Session

from hera_librarian.exceptions import LibrarianHTTPError, LibrarianTimeoutError
from hera_librarian.utils import compare_checksums
from librarian_server.database import get_session
from librarian_server.logger import ErrorCategory, ErrorSeverity, log_to_database
from librarian_server.orm import (
    Librarian,
    OutgoingTransfer,
    RemoteInstance,
    TransferStatus,
)

from .task import Task


def get_stale_of_type(session: Session, age_in_days: int, transfer_type: object):
    """
    Get the stale transfers of a given type.
    """

    # Get the stale outgoing transfers
    stale_since = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        days=age_in_days
    )

    transfer_stmt = session.query(transfer_type)

    transfer_stmt = transfer_stmt.where(transfer_type.start_time < stale_since)

    transfer_stmt = transfer_stmt.where(
        transfer_type.status.in_(
            [TransferStatus.INITIATED, TransferStatus.ONGOING, TransferStatus.STAGED]
        )
    )

    return session.execute(transfer_stmt).scalars().all()


def handle_stale_outgoing_transfer(
    session: Session, transfer: OutgoingTransfer
) -> bool:
    """
    In all cases, we ask if the downstream has the file. If it does, we mark our
    transfer as completed as if we just completed a transfer, and mark our
    OutgoingTransfer as complete.

    If the downstream does not have the file, we will ask the downstream to cancel
    its incoming transfer.
    """

    downstream_librarian = (
        session.query(Librarian).filter_by(name=transfer.destination).one_or_none()
    )

    if not downstream_librarian:
        log_to_database(
            severity=ErrorSeverity.ERROR,
            category=ErrorCategory.DATA_INTEGRITY,
            message=f"Downstream librarian {transfer.destination} not found",
            session=session,
        )

        transfer.fail_transfer(session=session, commit=False)

        return False

    client = downstream_librarian.client()

    expected_file_name = transfer.file.name
    expected_file_checksum = transfer.file.checksum

    try:
        potential_files = client.search_files(
            name=expected_file_name,
        )
    except (LibrarianHTTPError, LibrarianTimeoutError) as e:
        log_to_database(
            severity=ErrorSeverity.ERROR,
            category=ErrorCategory.LIBRARIAN_NETWORK_AVAILABILITY,
            message=(
                f"Unacceptable error when trying to check if librarian {transfer.destination}"
                f"has file {expected_file_name} with exception {e}."
            ),
            session=session,
        )
        return False

    if not potential_files:
        # The downstream does not have the file. We should cancel the transfer.
        log_to_database(
            severity=ErrorSeverity.ERROR,
            category=ErrorCategory.DATA_INTEGRITY,
            message=f"Downstream librarian {transfer.destination} does "
            f"not have file {expected_file_name} and the transfer is stale. "
            "Cancelling the transfer.",
            session=session,
        )

        transfer.fail_transfer(session=session, commit=False)

        return False

    available_checksums = {f.checksum for f in potential_files}
    available_store_ids = {i.store_id for f in potential_files for i in f.instances}

    if len(available_checksums) != 1:
        log_to_database(
            severity=ErrorSeverity.ERROR,
            category=ErrorCategory.DATA_INTEGRITY,
            message=f"Multiple (or zero, actual {len(available_checksums)}) checksums "
            f"found for file {expected_file_name} "
            f"on downstream librarian {transfer.destination}.",
            session=session,
        )

        transfer.fail_transfer(session=session, commit=False)

        return False

    available_checksum = available_checksums.pop()
    available_store_id = available_store_ids.pop()

    if not compare_checksums(available_checksum, expected_file_checksum):
        log_to_database(
            severity=ErrorSeverity.ERROR,
            category=ErrorCategory.DATA_INTEGRITY,
            message=f"Checksum mismatch for file {expected_file_name} "
            f"on downstream librarian {transfer.destination}.",
            session=session,
        )

        transfer.fail_transfer(session=session, commit=False)

        return False

    # If we made it here, we succeeded, we just never heard back!

    remote_instance = RemoteInstance.new_instance(
        file=transfer.file,
        store_id=available_store_id,
        librarian=downstream_librarian,
    )

    session.add(remote_instance)
    transfer.status = TransferStatus.COMPLETED

    session.commit()

    log_to_database(
        severity=ErrorSeverity.INFO,
        category=ErrorCategory.TRANSFER,
        message=(
            f"Successfully registered remote instance for {transfer.destination} and "
            f"transfer {transfer.id} based upon stale transfer with "
            f"status {transfer.status}."
        ),
        session=session,
    )

    return True


class OutgoingTransferHypervisor(Task):
    """
    Checks in on stale outgoing transfers.
    """

    age_in_days: int
    "The age in days of the outgoing transfer before we consider it stale."

    def on_call(self):
        with get_session() as session:
            return self.core(session=session)

    def core(self, session):
        """
        Checks for stale outgoing transfers and updates their status.
        """

        start_time = datetime.datetime.now(datetime.timezone.utc)
        end_time = start_time + self.soft_timeout

        stale_transfers = get_stale_of_type(session, self.age_in_days, OutgoingTransfer)

        for transfer in stale_transfers:
            current_time = datetime.datetime.now(datetime.timezone.utc)

            if current_time > end_time:
                return False

            handle_stale_outgoing_transfer(session, transfer)

        return True
