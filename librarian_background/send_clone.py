"""
Sends clones of files to a remote librarian.
"""

import datetime
import logging
from typing import TYPE_CHECKING, Any, Optional

from schedule import CancelJob
from sqlalchemy import select

from hera_librarian.client import LibrarianClient
from hera_librarian.errors import ErrorCategory, ErrorSeverity
from hera_librarian.exceptions import (
    LibrarianError,
    LibrarianHTTPError,
    LibrarianTimeoutError,
)
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
    outgoing_information: list[dict[str, Any]] = []

    for file in files:
        use_instance: Optional[Instance] = None

        if len(file.instances) == 0:
            logger.error(f"File {file.name} has no instances. Skipping.")
            continue

        for instance in file.instances:
            if instance.available:
                use_instance = instance

                if instance.store.name in valid_stores:
                    break

        if use_instance is None:
            logger.error(f"File {file.name} has no available instances. Skipping.")
            continue

        # If we really have to, we can add the store here.
        # But hopefully everything comes from our primary!
        if instance.store.name not in valid_stores:
            valid_stores.add(instance.store.name)

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

    return outgoing_transfers, outgoing_information


def use_batch_to_call_librarian(
    outgoing_transfers: list[OutgoingTransfer],
    outgoing_information: list[dict[str, Any]],
    client: LibrarianClient,
    librarian: Librarian | None,
    session: Session,
) -> bool | CloneBatchInitiationResponse:
    """
    Use the batch to call the librarian and stage the clone.
    Returns a boolean; if this call was unsucessful, the transfers
    are failed and we return False.

    Parameters
    ----------
    outgoing_transfers : list[OutgoingTransfer]
        List of outgoing transfers to send.
    outgoing_information : list[dict[str, Any]]
        List of information about the outgoing transfers. (from process_batch)
    client : LibrarianClient
        Client connection to the remote librarian.
    librarian : Librarian | None
        Librarian that we are sending to. If this is None, you won't be able
        to handle existing files on the downstream.
    session : Session
        SQLAlchemy session to use.

    Returns
    -------
    bool | CloneBatchInitiationResponse
        Truthy if the call was successful, False otherwise.

    """
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
            endpoint="clone/batch_stage",
            request=batch,
            response=CloneBatchInitiationResponse,
        )
    except LibrarianHTTPError as e:
        remedy_success = False

        if e.status_code == 409:
            # The librarian already has the file... Potentially.
            potential_id = e.full_response.get("source_transfer_id", None)

            if potential_id is None:
                log_to_database(
                    severity=ErrorSeverity.ERROR,
                    category=ErrorCategory.PROGRAMMING,
                    message=(
                        "Librarian told us that they have a file, but did not provide "
                        "the source transfer ID."
                    ),
                    session=session,
                )
            else:
                remedy_success = handle_existing_file(
                    session=session,
                    source_transfer_id=potential_id,
                    librarian=librarian,
                )

        # Oh no, we can't call up the librarian!
        if not remedy_success:
            log_to_database(
                severity=ErrorSeverity.ERROR,
                category=ErrorCategory.LIBRARIAN_NETWORK_AVAILABILITY,
                message=(
                    f"Unable to communicate with remote librarian for batch "
                    f"to stage clone with exception {e}."
                ),
                session=session,
            )

        # What a waste... Even if we did remedy the problem with the
        # already-existent file, we need to fail this over.
        for transfer in outgoing_transfers:
            transfer.fail_transfer(session=session, commit=False)

        session.commit()

        return False
    except LibrarianTimeoutError as e:
        # Can't connect to the librarian. Log and move on...

        log_to_database(
            severity=ErrorSeverity.ERROR,
            category=ErrorCategory.LIBRARIAN_NETWORK_AVAILABILITY,
            message=(
                f"Timeout when trying to communicate with remote librarian for batch "
                f"to stage clone with exception {e}."
            ),
            session=session,
        )

        for transfer in outgoing_transfers:
            transfer.fail_transfer(session=session, commit=False)

        session.commit()

        return False

    return response


def create_send_queue_item(
    response: CloneBatchInitiationResponse,
    outgoing_transfers: list[OutgoingTransfer],
    librarian: Librarian,
    session: Session,
) -> tuple[SendQueue | bool, Any, dict[int, CloneBatchInitiationRequestFileItem]]:
    """
    Create the send queue item for the transfers.

    Parameters
    ----------
    response : CloneBatchInitiationResponse
        Response from the librarian for the batch upload.
    outgoing_transfers : list[OutgoingTransfer]
        List of outgoing transfers that were sent.
    librarian : Librarian
        Librarian that we are sending to.
    session : Session
        SQLAlchemy session to use.

    Returns
    -------
    SendQueue | bool
        The send queue item that was created, or False if it was not created.
    Any
        Transfer provider that was used.
    dict[int, CloneBatchInitiationRequestFileItem]
        Mapping of source transfer IDs to the remote transfer information.
    """

    transfer_map: dict[int:CloneBatchInitiationRequestFileItem] = {
        x.source_transfer_id: x for x in response.uploads
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

    if len(response.async_transfer_providers) == 0:
        # No transfer providers are available at all for that librarian.
        log_to_database(
            severity=ErrorSeverity.WARNING,
            category=ErrorCategory.LIBRARIAN_NETWORK_AVAILABILITY,
            message=(
                f"No transfer providers to send to {librarian}, "
                f"were provided. Failing all associated transfers."
            ),
            session=session,
        )

        for transfer in outgoing_transfers:
            transfer.fail_transfer(session=session, commit=False)

        session.commit()

        # Break out of the loop.
        return False, None, None

    for transfer_provider in response.async_transfer_providers.values():
        if transfer_provider.valid:
            break

    if not transfer_provider.valid:
        # We couldn't find a valid transfer manager. We will have to fail it all.
        log_to_database(
            severity=ErrorSeverity.WARNING,
            category=ErrorCategory.LIBRARIAN_NETWORK_AVAILABILITY,
            message=(
                f"No valid transfer manager found for transfer to {librarian}, "
                f"was provided {list(response.async_transfer_providers.keys())}. Failing "
                "all associated transfers."
            ),
            session=session,
        )

        for transfer in outgoing_transfers:
            transfer.fail_transfer(session=session, commit=False)

        session.commit()

        # Break out of the loop.
        return False, None, None

    send = SendQueue.new_item(
        priority=0,
        destination=librarian.name,
        transfers=outgoing_transfers,
        async_transfer_manager=transfer_provider,
    )

    session.add(send)
    session.commit()

    return send, transfer_provider, transfer_map


def call_destination_and_state_ongoing(send: SendQueue, session: Session):
    """
    Call the destination librarian and state the transfer as ongoing.
    """

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


def handle_existing_file(
    session: Session,
    source_transfer_id: int,
    librarian: Librarian,
) -> bool:
    """
    Handles the case where the clone tells us that they already have
    the file.

    Does this buy calling up the downsteram librarian and asking for
    the checksum. If it has the file, and the checksum matches, we
    register a remote instance.

    NOTE: This may leave dangling STAGED files.
    """

    log_to_database(
        severity=ErrorSeverity.INFO,
        category=ErrorCategory.TRANSFER,
        message=(
            f"Librarian {librarian.name} told us that they already have the file "
            f"from transfer {source_transfer_id}, attempting to handle and create "
            "remote instance."
        ),
        session=session,
    )

    client = librarian.client()

    # Extract name and checksum from ongoing transfer.
    transfer = session.get(OutgoingTransfer, source_transfer_id)

    file_name = transfer.file.name
    file_checksum = transfer.file.checksum

    try:
        potential_files = client.search_files(name=file_name)

        if len(potential_files) == 0:
            # They don't have the file; this _really_ doesn't make sense.
            log_to_database(
                severity=ErrorSeverity.ERROR,
                category=ErrorCategory.PROGRAMMING,
                message=(
                    f"Librarian {librarian.name} told us that they have file {file_name}, "
                    "but we can't find it in their database."
                ),
                session=session,
            )
            return False
        else:
            # We have the file; we need to check the checksum.
            for potential_file in potential_files:
                if potential_file.checksum == file_checksum:
                    # We have a match; we can register the remote instance.
                    try:
                        potential_store_id = potential_file.instances[0].store_id

                        if potential_store_id is None:
                            raise ValueError
                    except (IndexError, ValueError):
                        # So, you're telling me, that you have a file but it doesn't
                        # have any instances..?
                        log_to_database(
                            severity=ErrorSeverity.ERROR,
                            category=ErrorCategory.PROGRAMMING,
                            message=(
                                f"Librarian {librarian.name} told us that they have file {file_name}, "
                                "but it has no instances."
                            ),
                            session=session,
                        )
                        return False

                    remote_instance = RemoteInstance.new_instance(
                        file=transfer.file,
                        store_id=potential_store_id,
                        librarian=librarian,
                    )

                    session.add(remote_instance)
                    session.commit()

                    log_to_database(
                        severity=ErrorSeverity.INFO,
                        category=ErrorCategory.TRANSFER,
                        message=(
                            f"Successfully registered remote instance for {librarian.name} and "
                            f"transfer {source_transfer_id}."
                        ),
                        session=session,
                    )

                    return True
                else:
                    # Checksums do not match; this is a problem.
                    log_to_database(
                        severity=ErrorSeverity.ERROR,
                        category=ErrorCategory.TRANSFER,
                        message=(
                            f"Librarian {librarian.name} told us that they have file {file_name}, "
                            "but the checksums do not match."
                        ),
                        session=session,
                    )

                    return False
    except Exception as e:
        log_to_database(
            severity=ErrorSeverity.ERROR,
            category=ErrorCategory.LIBRARIAN_NETWORK_AVAILABILITY,
            message=(
                f"Unacceptable error when trying to check if librarian {librarian.name} "
                f"has file {file_name} with exception {e}."
            ),
            session=session,
        )
        return False

    return True


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

    def on_call(self):  # pragma: no cover
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

        # Only used when there is a botched config.
        if librarian is None:  # pragma: no cover
            log_to_database(
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.LIBRARIAN_NETWORK_AVAILABILITY,
                message=(
                    f"Librarian {self.destination_librarian} does not exist within database. "
                    "Cancelling job. Please update the configuration (and re-start the librarian)."
                ),
                session=session,
            )
            return CancelJob

        client: "LibrarianClient" = librarian.client()

        try:
            client.ping()
        except Exception as e:
            log_to_database(
                severity=ErrorSeverity.ERROR,
                category=ErrorCategory.LIBRARIAN_NETWORK_AVAILABILITY,
                message=(
                    f"Librarian {self.destination_librarian} is unreachable. Skipping sending clones."
                ),
                session=session,
            )

            # No point canceling job, our freind could just be down for a while.
            return

        current_time = datetime.datetime.now(datetime.timezone.utc)
        age_in_days = datetime.timedelta(days=self.age_in_days)
        oldest_file_age = current_time - age_in_days

        file_stmt = select(File).filter(File.create_time > oldest_file_age)
        remote_instances_stmt = select(RemoteInstance.file_name).filter(
            RemoteInstance.librarian_id == librarian.id
        )
        outgoing_transfer_stmt = (
            select(OutgoingTransfer.file_name)
            .filter(OutgoingTransfer.destination == librarian.name)
            .filter(
                OutgoingTransfer.status.in_(
                    [
                        TransferStatus.INITIATED,
                        TransferStatus.ONGOING,
                        TransferStatus.STAGED,
                    ]
                )
            )
        )

        file_stmt = file_stmt.where(File.name.not_in(remote_instances_stmt))

        file_stmt = file_stmt.where(File.name.not_in(outgoing_transfer_stmt))

        import pdb

        pdb.set_trace()

        files_without_remote_instances: list[File] = (
            session.execute(file_stmt).scalars().all()
        )

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

            # Botched configuration!
            if use_store is None:  # pragma: no cover
                log_to_database(
                    severity=ErrorSeverity.CRITICAL,
                    category=ErrorCategory.CONFIGURATION,
                    message=(
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

        while files_tried < len(files_without_remote_instances):
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

            response = use_batch_to_call_librarian(
                outgoing_transfers=outgoing_transfers,
                outgoing_information=outgoing_information,
                client=client,
                librarian=librarian,
                session=session,
            )

            # We were unable to speak to the librarian, and have had our
            # transfers cancelled for us. Time to move on to the next
            # batch and hope for the best.

            # Tested outside of the main loop.
            if not response:  # pragma: no cover
                continue

            # Ok, they got out stuff. Need to do two things now:
            # - Create the queue send item
            # - Update the transfers with their information.

            send, transfer_provider, transfer_map = create_send_queue_item(
                response=response,
                outgoing_transfers=outgoing_transfers,
                librarian=librarian,
                session=session,
            )

            # Send is falsey if there was a problem in creating the send
            # queue item. In that, case we've failed everything, and should break
            # and come back later.

            # Tested outside of the main loop.
            if not send:  # pragma: no cover
                break

            # Now update the outgoing transfers with their information.
            for transfer in outgoing_transfers:
                remote_transfer_info: CloneBatchInitiationRequestFileItem = (
                    transfer_map.get(transfer.id, None)
                )

                if remote_transfer_info is None:  # pragma: no cover
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
                transfer.source_path = str(transfer.instance.path)
                transfer.dest_path = str(remote_transfer_info.staging_location)

            session.commit()

            # Finally, call up the destination again and tell them everything is on its
            # way.

            call_destination_and_state_ongoing(send=send, session=session)

        return
