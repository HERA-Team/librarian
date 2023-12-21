"""
The twin of send_clone.py, this file contains the code for recieving a clone
from a remote librarian. We loop through the incoming transfers and check
to see if they have completed.
"""


import logging
import datetime
from typing import Optional

from .task import Task

from librarian_server.database import session, query
from librarian_server.orm import (
    File,
    Instance,
    StoreMetadata,
    IncomingTransfer,
    TransferStatus,
    Librarian,
)
from librarian_server.deletion import DeletionPolicy

from hera_librarian.models.clone import (
    CloneCompleteRequest,
    CloneCompleteResponse,
)

logger = logging.getLogger("schedule")


class RecieveClone(Task):
    """
    Recieves incoming files from other librarians.
    """

    deletion_policy: DeletionPolicy = DeletionPolicy.NEVER

    def on_call(self):
        """
        Checks for incoming transfers and processes them.
        """

        # Find incoming transfers that are ONGOING
        ongoing_transfers: list[IncomingTransfer] = query(
            IncomingTransfer, IncomingTransfer.status == TransferStatus.ONGOING
        ).all()

        if len(ongoing_transfers) == 0:
            logger.info("No ongoing transfers to process.")

        for transfer in ongoing_transfers:
            # Check if the transfer has completed

            store: StoreMetadata = transfer.store

            path_info = store.path_info(transfer.staging_path)

            # TODO: Make this check more robust?
            if (
                path_info.checksum == transfer.transfer_checksum
                and path_info.size == transfer.transfer_size
            ):
                # The transfer has completed. Create an instance for this file.
                logger.info(
                    f"Transfer {transfer.id} has completed. Moving file to store and creating instance."
                )

                # Move the file to the store.
                # TODO: Check where that store path is coming from!
                try:
                    store.store_manager.commit(
                        transfer.staging_path, transfer.store_path
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to move file {transfer.staging_path} to store "
                        f"{store.name} at {transfer.store_path}. Exception: {e}. Skipping for now."
                    )
                    continue

                # Create a new File object
                file = File.new_file(
                    name=transfer.upload_name,
                    checksum=transfer.upload_checksum,
                    size=transfer.upload_size,
                    uploader=transfer.uploader,
                    source=transfer.source,
                )

                # Create an instance for this file.
                instance = Instance.new_instance(
                    path=path_info.path,
                    file=file,
                    store=store,
                    deletion_policy=self.deletion_policy
                )

                session.add(file)
                session.add(instance)

                # Mark the transfer as completed.
                transfer.status = TransferStatus.COMPLETED
                transfer.end_time = datetime.datetime.utcnow()

                # Commit the changes.
                session.commit()

                # Callback to the source librarian.
                librarian: Optional[Librarian] = query(
                    Librarian, Librarian.name == transfer.source
                ).first()

                if librarian:
                    # Need to call back
                    logger.info(
                        f"Transfer {transfer.id} has completed. Calling back to librarian {librarian.name}."
                    )

                    request = CloneCompleteRequest(
                        source_transfer_id=transfer.id,
                        destination_instance_id=instance.id,
                    )

                    try:
                        response: CloneCompleteResponse = librarian.client.do_pydantic_http_post(
                            endpoint="/api/v2/clone/complete",
                            request_model=request,
                            response_model=CloneCompleteResponse,
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to call back to librarian {librarian.name} with exception {e}."
                        )
                else:
                    logger.error(
                        f"Transfer {transfer.id} has no source librarian. Cannot callback."
                    )

                # Can now delete the file
                store.store_manager.unstage(transfer.staging_path)
            else:
                logger.info(f"Transfer {transfer.id} has not yet completed. Skipping.")
                continue
