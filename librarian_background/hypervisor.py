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
from hera_librarian.models.checkin import CheckinStatusRequest, CheckinStatusResponse
from hera_librarian.utils import compare_checksums
from librarian_server.database import get_session
from librarian_server.logger import ErrorCategory, ErrorSeverity, log, log_to_database
from librarian_server.orm import (
    Librarian,
    OutgoingTransfer,
    RemoteInstance,
    TransferStatus,
)
from librarian_server.orm.transfer import IncomingTransfer

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

        # Must commit; need to save this cancel state. The file will never get there
        transfer.fail_transfer(session=session, commit=True)

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

        transfer.fail_transfer(session=session, commit=True)

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

        transfer.fail_transfer(session=session, commit=True)

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


def handle_stale_incoming_transfer(
    session: Session,
    transfer: IncomingTransfer,
) -> bool:

    upstream_librarian = (
        session.query(Librarian).filter_by(name=transfer.source).one_or_none()
    )

    if not upstream_librarian:
        log_to_database(
            severity=ErrorSeverity.ERROR,
            category=ErrorCategory.DATA_INTEGRITY,
            message=f"Upstream librarian {transfer.source} not found",
            session=session,
        )

        transfer.fail_transfer(session=session, commit=True)
        return False

    # We have an upstream librarian. We can ask it about the status of its
    # own OutgoingTransfer.
    client = upstream_librarian.client()

    status_request = CheckinStatusRequest(
        source_transfer_ids=[transfer.source_transfer_id],
        destination_transfer_ids=[],
    )

    try:
        response: CheckinStatusResponse = client.post(
            "checkin/status", request=status_request, response=CheckinStatusResponse
        )

        source_status = response.source_transfer_status[transfer.source_transfer_id]
    except Exception as e:
        log_to_database(
            severity=ErrorSeverity.ERROR,
            category=ErrorCategory.TRANSFER,
            message=(
                f"Unsuccessfully tried to contact {transfer.source} for information on "
                f"transfer (local: {transfer.id}, remote: {transfer.source_transfer_id}). "
                f"Exception: {e}. We are failing {transfer.id}"
            ),
            session=session,
        )

        # This implies that the transfer doesn't exist on the remote.
        transfer.fail_transfer(session=session, commit=True)
        return False

    # Now we need to do some state matching.
    # If remote transfer is 'dead', we can just cancel.
    # If remote transfer is 'alive' and in same state as ours, we can
    # just leave it... For now. It is upstream's job to cancel these kind
    # of stale transfers.
    # If remote transfer is 'alive' and in a different state to ours, there
    # are two possibilities:
    # a) Remote is more advanced than us; we should update ourselves to be
    #    aligned with this to try to progress with the transfer.
    # b) Remote is less advanced than us; we should cancel the transfer - this
    #    is an explicitly bad state as we are a push-based system.

    if source_status in [TransferStatus.COMPLETED]:
        log_to_database(
            severity=ErrorSeverity.CRITICAL,
            category=ErrorCategory.PROGRAMMING,
            message=(
                f"Transfer (local: {transfer.id}, remote: {transfer.source_transfer_id}) has "
                f"COMPLETED status on remote but {transfer.status} on local. This is "
                f"an unreachable state for file {transfer.upload_name}. Requires manual check"
            ),
            session=session,
        )

        transfer.fail_transfer(session=session, commit=True)
        return False

    if source_status in [TransferStatus.CANCELLED, TransferStatus.FAILED]:
        # This one's a gimmie.
        log.error(
            f"Found end status for incoming transfer {transfer.id} on remote, cancelling"
        )
        transfer.fail_transfer(session=session, commit=True)
        return False

    if source_status == transfer.status:
        # This is the remote's responsibility.
        log.info(
            f"Found same for incoming transfer {transfer.id} on remote, continuing"
        )
        return True

    # We only get here in annoying scenarios.
    if transfer.status == TransferStatus.INITIATED:
        # Remote more advanced.
        log_to_database(
            severity=ErrorSeverity.INFO,
            category=ErrorCategory.TRANSFER,
            message=(
                f"Transfer (local: {transfer.id}, remote: {transfer.source_transfer_id}) has "
                f"more advanced state on remote ({source_status} > {transfer.status}). Catching"
                f"up our transfer."
            ),
            session=session,
        )

        transfer.status = source_status
        session.commit()
        return True

    if transfer.status == TransferStatus.STAGED:
        # Uh, this should be picked up by a different task (recv_clone)
        log_to_database(
            severity=ErrorSeverity.CRITICAL,
            category=ErrorCategory.CONFIGURATION,
            message=(
                f"Transfer (local: {transfer.id}, remote: {transfer.source_transfer_id}) has "
                "status STAGED and is being picked up by the hypervisor task. This should not "
                "occur; recommend manual check"
            ),
            session=session,
        )
        return False

    if transfer.status == TransferStatus.ONGOING:
        if source_status == TransferStatus.INITIATED:
            transfer.fail_transfer(session=session, commit=True)
            return False
        else:
            assert source_status == TransferStatus.STAGED
            # Remote more advanced (STAGED)
            log_to_database(
                severity=ErrorSeverity.INFO,
                category=ErrorCategory.TRANSFER,
                message=(
                    f"Transfer (local: {transfer.id}, remote: {transfer.source_transfer_id}) has "
                    f"more advanced state on remote ({source_status} > {transfer.status}). Catching"
                    f"up our transfer."
                ),
                session=session,
            )

            transfer.status = source_status
            session.commit()
            return True

    log_to_database(
        severity=ErrorSeverity.CRITICAL,
        category=ErrorCategory.PROGRAMMING,
        message=(
            f"Transfer (local: {transfer.id}, remote: {transfer.source_transfer_id}) has "
            "fallen through the hypervisor. Recommend manual check"
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


class IncomingTransferHypervisor(Task):
    """
    Checks on stale incoming transfers.
    """

    age_in_days: int
    "The age in days of the incoming transfer before we consider it stale."

    def on_call(self):
        with get_session() as session:
            return self.core(session=session)

    def core(self, session):
        """
        Checks for stale incoming transfers and updates their status.
        """

        start_time = datetime.datetime.now(datetime.timezone.utc)
        end_time = start_time + self.soft_timeout

        stale_transfers = get_stale_of_type(session, self.age_in_days, IncomingTransfer)

        for transfer in stale_transfers:
            current_time = datetime.datetime.now(datetime.timezone.utc)

            if current_time > end_time:
                return False

            handle_stale_incoming_transfer(session, transfer)

        return True
