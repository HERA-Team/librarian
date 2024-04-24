"""
Sends clones of files to a remote librarian.
"""

import datetime
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from schedule import CancelJob
from sqlalchemy import select

from hera_librarian.errors import ErrorCategory, ErrorSeverity
from hera_librarian.exceptions import LibrarianError
from hera_librarian.models.checkin import CheckinUpdateRequest, CheckinUpdateResponse
from hera_librarian.models.clone import (
    CloneBatchInitiationRequest,
    CloneBatchInitiationRequestFileItem,
    CloneBatchInitiationResponse,
)
from librarian_server.database import get_session
from librarian_server.logger import log_to_database
from librarian_server.orm import (
    File,
    Instance,
    Librarian,
    OutgoingTransfer,
    RemoteInstance,
    SendQueue,
    StoreMetadata,
    TransferStatus,
)
from librarian_server.settings import server_settings

from .task import Task

if TYPE_CHECKING:
    from hera_librarian import LibrarianClient

from sqlalchemy.orm import Session

logger = logging.getLogger("schedule")


def process_batch(
    files: list[File], destination, store_preference: str | None = None
) -> tuple[list[OutgoingTransfer], list[dict[str, Any]]]:
    """
    Process a batch of files to generate transfers for all of the valid
    ones. Returned is an uncommited list of outgoing transfers, that
    can all be added atomically, as well as all of the file info needed
    for the batch item.
    """

    # Try to keep the number of stores used as low as possible.
    # If we are already transfering a file from one store,
    # we should try to transfer the rest from it too. Stores may
    # be inaccessable, or not be able to use certain transfer methods,
    # and we want the most uniform batch possible.

    valid_stores = set()

    if store_preference is not None:
        valid_stores.add(store_preference)

    outgoing_transfers: list[OutgoingTransfer] = []
    outgoing_information: list[dict[str, Any]]

    for file in files:
        use_instance: Optional[Instance] = None

        if len(file.instances) == 0:
            logger.error(f"File {file.name} has no instances. Skipping.")
            continue

        for instance in file.instances:
            if instance.available:
                use_instance = instance

                if instance.store_name in valid_stores:
                    break

        if use_instance is None:
            logger.error(f"File {file.name} has no available instances. Skipping.")
            continue

        # If we really have to, we can add the store here.
        # But hopefully everything comes from our primary!
        if instance.store_name not in valid_stores:
            valid_stores.add(instance.store_name)

        outgoing_transfers.append(
            OutgoingTransfer.new_transfer(
                destination=destination, instance=use_instance, file=file
            )
        )

        outgoing_information.append(
            {
                "upload_size": file.size,
                "upload_checksum": file.checksum,
                "upload_name": file.name,
                "destination_location": file.name,
                "uploader": file.uploader,
            }
        )

    return outgoing_transfers


class SendClone(Task):
    """
    Launches clones of files to a remote librarian.

    These files are those that do not have a FileInstance in our database
    corresponding to that remote librarian.
    """

    destination_librarian: str
    "Name of the librarian to send the clone to. This must be a remote librarian already registered in our database."
    age_in_days: int
    "Age in days of the files to check. I.e. only check files younger than this (we assume older files are fine as they've been checked before)"
    store_preference: Optional[str]
    "Name of the store to prefer when sending files. If None, we will use whatever store is available for sending that file."
    send_batch_size: int = 128

    def on_call(self):
        with get_session() as session:
            return self.core(session=session)

    def core(self, session: Session):
        """
        Creates uploads to the remote librarian as specified.
        """
        # Before even attempting to do anything, get the information about the librarian and create
        # a client connection to it.
        librarian: Optional[Librarian] = (
            session.query(Librarian).filter_by(name=self.destination_librarian).first()
        )

        if librarian is None:
            log_to_database(
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.LIBRARIAN_NETWORK_AVAILABILITY,
                description=(
                    f"Librarian {self.destination_librarian} does not exist within database. "
                    "Cancelling job. Please update the configuration (and re-start the librarian)."
                ),
                session=session,
            )
            return CancelJob

        client: "LibrarianClient" = librarian.get_client()

        try:
            client.ping()
        except Exception as e:
            log_to_database(
                severity=ErrorSeverity.ERROR,
                category=ErrorCategory.LIBRARIAN_NETWORK_AVAILABILITY,
                description=(
                    f"Librarian {self.destination_librarian} is unreachable. Skipping sending clones."
                ),
                session=session,
            )

            # No point canceling job, our freind could just be down for a while.
            return

        current_time = datetime.datetime.now(datetime.UTC)
        age_in_days = datetime.timedelta(days=self.age_in_days)
        oldest_file_age = current_time - age_in_days

        stmt = select(File).filter(File.create_time > oldest_file_age)
        stmt = stmt.filter(
            File.remote_instances.any(RemoteInstance.librarian_id != librarian.id)
        )

        ongoing_transfer_stmt = select(OutgoingTransfer.file_name).filter(
            OutgoingTransfer.status.in_(
                [
                    TransferStatus.INITIATED,
                    TransferStatus.ONGOING,
                    TransferStatus.STAGED,
                ]
            )
        )

        stmt = stmt.filter(File.name.not_in(ongoing_transfer_stmt))

        files_without_remote_instances = list[File] = session.execute(
            stmt
        ).scalars.all()

        logger.info(
            f"Found {len(files_without_remote_instances)} files without remote instances, "
            "and without ongoing transfers."
        )

        if self.store_preference is not None:
            use_store: StoreMetadata = (
                session.query(StoreMetadata)
                .filter_by(name=self.store_preference)
                .first()
            )

            if use_store is None:
                log_to_database(
                    severity=ErrorSeverity.CRITICAL,
                    category=ErrorCategory.CONFIGURATION,
                    description=(
                        f"Store {self.store_preference} does not exist. Cancelling job. "
                        "Please update the configuration."
                    ),
                    session=session,
                )

                return CancelJob

        # To prepare a batch, we need to:
        # - Select N files that we want to transfer simultaneously.
        # - Make sure they all have instances
        # - Generate outgoing transfers
        # - Commit all new transfers simultaneously, to get IDs
        # - Call up downstream for the batch transfer.
        # - Generate a send queue
        # - Update the outgoing transfers with the send queue
        # - Update outgoing and remote incoming transfers to ONGOING status.
        # Then from there, the send queue takes care of everything.

        files_tried = 0

        while files_tried <= len(files_without_remote_instances):
            left_to_send = len(files_without_remote_instances) - files_tried
            this_batch_size = min(left_to_send, self.send_batch_size)

            files_to_try = files_without_remote_instances[
                files_tried : files_tried + this_batch_size
            ]

            files_tried += this_batch_size

            outgoing_transfers, outgoing_information = process_batch(
                files=files_to_try,
                destination=self.destination_librarian,
                store_preference=self.store_preference,
            )

            session.add_all(outgoing_transfers)
            session.commit()

            # Now the outgoing transfers all have IDs! We can create the batch
            # items.
            batch_items = [
                CloneBatchInitiationRequestFileItem(source_transfer_id=x.id, **y)
                for x, y in zip(outgoing_transfers, outgoing_information)
            ]

            batch = CloneBatchInitiationRequest(
                uploads=batch_items,
                source=server_settings.name,
                total_size=sum((x["upload_size"] for x in outgoing_information)),
            )

            try:
                response: CloneBatchInitiationResponse = client.post(
                    endpoint="/api/v2/clone/batch_stage",
                    request_model=batch,
                    response_model=CloneBatchInitiationResponse,
                )
            except Exception as e:
                # Oh no, we can't call up the librarian!
                log_to_database(
                    severity=ErrorSeverity.ERROR,
                    category=ErrorCategory.LIBRARIAN_NETWORK_AVAILABILITY,
                    description=(
                        f"Unable to communicate with remote librarian for batch "
                        f"to stage clone with exception {e}."
                    ),
                    session=session,
                )

                # What a waste...
                for transfer in outgoing_transfers:
                    transfer.fail_transfer(session=session, commit=False)

                session.commit()

                continue

            # Ok, they got out stuff. Need to do two things now:
            # - Create the queue send item
            # - Update the transfers with their information.

            transfer_map: dict[int:CloneBatchInitiationRequestFileItem] = {
                x.outgoing_transfer_id: x for x in response.uploads
            }

            # Our response may not have successfully staged all files.
            # What can we do in that scenario..? I guess we just drop any
            # failed transfers. This likely won't happen in practice,
            # but it does not hurt to guard against it.

            created_transfers = {x.id for x in outgoing_transfers}
            remote_accepted_transfers = set(transfer_map.keys())

            not_accepted_transfers = created_transfers ^ remote_accepted_transfers

            # In all liklehood, this loop will never run. If it does, that's probably a bug.
            for tid in not_accepted_transfers:
                log_to_database(
                    severity=ErrorSeverity.ERROR,
                    category=ErrorCategory.TRANSFER,
                    message=(
                        f"Transfer ID {tid} was not returned from the batch upload process. "
                        "Failing this transfer internally, and continuing, but this "
                        "should not happen."
                    ),
                    session=session,
                )

                # Because we want to re-use the list, need to iterate through it.
                matches = lambda x: x.id == tid

                for index, transfer in enumerate(outgoing_transfers):
                    if matches(transfer):
                        transfer.fail_transfer(session=session, commit=True)

                        outgoing_transfers.pop(index)

                        break

            # Clean list of outoging transfers that have matching incoming transfers on
            # the destination librarian.

            for transfer_provider in response.transfer_providers.values():
                if transfer_provider.valid:
                    break

            if not transfer_provider.valid:
                # We couldn't find a valid transfer manager. We will have to fail it all.
                log_to_database(
                    severity=ErrorSeverity.WARNING,
                    category=ErrorCategory.LIBRARIAN_NETWORK_AVAILABILITY,
                    message=(
                        f"No valid transfer manager found for transfer to {librarian}, "
                        f"was provided {list(response.transfer_providers.keys())}. Failing "
                        "all associated transfers."
                    ),
                )

                for transfer in outgoing_transfers:
                    transfer.fail_transfer(session=session, commit=False)

                session.commit()
                break

            send = SendQueue.new_item(
                priority=0,
                destination=self.destination_librarian,
                transfers=outgoing_transfers,
                async_transfer_manager=transfer_provider,
            )

            session.add(send)
            session.commit()

            # Now update the outgoing transfers with their information.

            destination_transfer_ids = []
            for transfer in outgoing_transfers:
                remote_transfer_info: CloneBatchInitiationRequestFileItem = (
                    transfer_map.get(transfer.id, None)
                )

                if remote_transfer_info is None:
                    # This is an unreachable state; we already purged these
                    # scenarios.
                    log_to_database(
                        severity=ErrorSeverity.CRITICAL,
                        category=ErrorCategory.PROGRAMMING,
                        message=(
                            "Trying to set parameters of a transfer that should not "
                            "exist; this should be an unreachable state."
                        ),
                        session=session,
                    )

                    # In this case, the best thing that we can do is fail this individual
                    # transfer and pick it up later.
                    transfer.fail_transfer(session=session, commit=False)

                transfer.remote_transfer_id = (
                    remote_transfer_info.destination_transfer_id
                )
                transfer.transfer_data = transfer_provider
                transfer.send_queue = send
                transfer.send_queue_id = send.id
                transfer.source_path = transfer.instance.path
                transfer.dest_path = remote_transfer_info.staging_location

            session.commit()

            # Finally, call up the destination again and tell them everything is on its
            # way.

            try:
                send.update_transfer_status(
                    new_status=TransferStatus.ONGOING,
                    session=session,
                )
            except AttributeError as e:
                # Incorrect downstream librarian. This is a weird programming error,
                # that is only reachable if someone deleted the librarian in the
                # database between this process starting and ending.
                log_to_database(
                    severity=ErrorSeverity.CRITICAL,
                    category=ErrorCategory.PROGRAMMING,
                    message=e.message,
                    session=session,
                )
            except LibrarianError as e:
                # Can't call up downstream librarian. Already been called in.
                pass
            except Exception as e:
                # Unhandled!!!
                pass

        return
