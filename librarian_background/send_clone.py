"""
Sends clones of files to a remote librarian.
"""

import datetime
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from schedule import CancelJob

from hera_librarian.errors import ErrorCategory, ErrorSeverity
from hera_librarian.models.clone import (
    CloneInitiationRequest,
    CloneInitiationResponse,
    CloneOngoingRequest,
    CloneOngoingResponse,
)
from librarian_server.database import get_session
from librarian_server.logger import log_to_database
from librarian_server.orm import (
    File,
    Instance,
    Librarian,
    OutgoingTransfer,
    StoreMetadata,
    TransferStatus,
)
from librarian_server.settings import server_settings

from .task import Task

if TYPE_CHECKING:
    from hera_librarian import LibrarianClient

from sqlalchemy.orm import Session

logger = logging.getLogger("schedule")


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

        current_time = datetime.datetime.utcnow()
        age_in_days = datetime.timedelta(days=self.age_in_days)
        oldest_file_age = current_time - age_in_days

        files_without_remote_instances: list[File] = (
            session.query(File)
            .filter(
                File.create_time > oldest_file_age
                and File.remote_instances.any(librarian_name=self.destination_librarian)
            )
            .all()
        )

        logger.info(
            f"Found {len(files_without_remote_instances)} files without remote instances."
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

        for file in files_without_remote_instances:
            # Find the instance of the file we want to copy.
            use_instance: Optional[Instance] = None

            if len(file.instances) == 0:
                logger.error(f"File {file.name} has no instances. Skipping.")
                continue

            for instance in file.instances:
                if instance.available:
                    use_instance = instance

                    if instance.store_name == self.store_preference:
                        break

            if use_instance is None:
                logger.error(f"File {file.name} has no available instances. Skipping.")
                continue

            use_store: StoreMetadata = instance.store

            # Now we can create the outgoing transfer.
            transfer = OutgoingTransfer.new_transfer(
                destination=self.destination_librarian, instance=use_instance, file=file
            )

            session.add(transfer)
            # Need to do DB comms so we can pick up an ID.
            session.commit()

            logger.info(
                f"Created outgoing transfer {transfer.id} for file {file.name}."
            )

            # Now we can attempt to launch the transfer.

            request = CloneInitiationRequest(
                upload_size=file.size,
                upload_checksum=file.checksum,
                upload_name=file.name,
                destination_location=file.name,
                source_transfer_id=transfer.id,
                uploader=file.uploader,
                # TODO better identifier for this...
                source=server_settings.displayed_site_name,
            )

            try:
                response: CloneInitiationResponse = client.do_pydantic_http_post(
                    endpoint="/api/v2/clone/stage",
                    request_model=request,
                    response_model=CloneInitiationResponse,
                )
            except Exception as e:
                log_to_database(
                    severity=ErrorSeverity.ERROR,
                    category=ErrorCategory.LIBRARIAN_NETWORK_AVAILABILITY,
                    description=(
                        f"Unable to communicate with remote librarian for {transfer.id} "
                        f"to stage clone with exception {e}."
                    ),
                    session=session,
                )

                # Mark the transfer as failed.
                transfer.fail_transfer(session=session)
                continue

            # Great! Let's try to begin the (async) transfer of the file.
            success = False
            for tm_name, transfer_manager in response.transfer_providers:
                if not use_store.store_manager.can_transfer(transfer_manager):
                    continue

                try:
                    success = use_store.store_manager.transfer_out(
                        store_path=Path(instance.path),
                        destination_path=Path(response.staging_location),
                        using=transfer_manager,
                    )

                    if not success:
                        raise Exception(
                            f"Failed to transfer file {instance.path} using transfer manager {transfer_manager}."
                        )
                except Exception as e:
                    logger.debug(
                        f"Failed to transfer file {instance.path} using transfer manager {transfer_manager}."
                        f"Trying another transfer manager if available. Details: {e}"
                    )
                    continue

            if not success:
                log_to_database(
                    severity=ErrorSeverity.ERROR,
                    category=ErrorCategory.DATA_AVAILABILITY,
                    description=(
                        f"Unable to transfer file {instance.path} to remote store. "
                        "Skipping."
                    ),
                    session=session,
                )

                transfer.fail_transfer(session=session)
                continue

            # Great! We can now mark the transfer as ONGOING in the background.
            transfer.status = TransferStatus.ONGOING
            transfer.remote_transfer_id = response.destination_transfer_id
            transfer.transfer_manager_name = tm_name
            transfer.transfer_data = transfer_manager

            session.commit()

            # Now we tell the remote librarian that the transfer is ongoing.
            ongoing_request = CloneOngoingRequest(
                source_transfer_id=transfer.id,
                destination_transfer_id=response.destination_transfer_id,
            )

            try:
                ongoing_response: CloneOngoingResponse = client.do_pydantic_http_post(
                    endpoint="/api/v2/clone/ongoing",
                    request_model=ongoing_request,
                    response_model=CloneOngoingResponse,
                )
            except Exception as e:
                log_to_database(
                    severity=ErrorSeverity.ERROR,
                    category=ErrorCategory.LIBRARIAN_NETWORK_AVAILABILITY,
                    description=(
                        f"Unable to communicate with remote librarian for {transfer.id} "
                        f"to let it know that the transfer is ongoing with exception {e}."
                    ),
                    session=session,
                )

                # Don't fail the transfer... I mean, the data is on the way (it might
                # already be there for all we know) and we can't do anything about it.
                # transfer.fail_transfer()
                continue

        return