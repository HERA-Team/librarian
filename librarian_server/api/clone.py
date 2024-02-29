"""
API endpoint for cloning data from one librarian to another.
This endpoint is by construction asynchrnous; the two different
librarians' background tasks will call different instances of these APIs
to communicate with each other.

The following flow occurs when cloning data from librarian A to librarian B:

0. Librarian A's background task creates an OutgoingTransfer instance.
1. Librarian A calls the /stage endpoint on librarian B, with information
   about the incoming transfer. B creates an IncomingTransfer instance, and
   creates a staging area for the transfer.
2. Librarian A's background task uploads the file to the store asynchronously.
3. Librarian B's background task checks for completion of the uploads. When
   it is complete, it stores the file, updating the IncomingTransfer instance
   to completed.
4. Librarian B's background task calls the /complete endpoint on librarian A,
   which updates the OutgoingTransfer object.
"""

from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from hera_librarian.models.clone import (
    CloneCompleteRequest,
    CloneCompleteResponse,
    CloneFailedResponse,
    CloneFailRequest,
    CloneFailResponse,
    CloneInitiationRequest,
    CloneInitiationResponse,
    CloneOngoingRequest,
    CloneOngoingResponse,
)

from ..database import yield_session
from ..logger import log
from ..orm.file import File
from ..orm.instance import RemoteInstance
from ..orm.librarian import Librarian
from ..orm.storemetadata import StoreMetadata
from ..orm.transfer import IncomingTransfer, OutgoingTransfer, TransferStatus
from .auth import CallbackUserDependency, ReadappendUserDependency

router = APIRouter(prefix="/api/v2/clone")


@router.post("/stage", response_model=CloneInitiationResponse | CloneFailedResponse)
def stage(
    request: CloneInitiationRequest,
    response: Response,
    user: ReadappendUserDependency,
    session: Session = Depends(yield_session),
):
    """
    Recieved from a remote librarian to initiate a clone.

    This is somewhat similar to upload/stage, but that is a synchronous
    operation and the data transfer performed here is actually asynchrounous.

    Possible response codes:

    400 - Bad request. Upload size is negative.
        -> Make sure that the logic is correctly computing the size.
    406 - Not acceptable. You have already initiated or staged this transfer.
        -> You should fail your outgoing transfer. We failed ours.
    409 - Conflict. File already exists on librarian.
        -> You have a logic error, you should never have tried to upload this.
    413 - Request entity too large. No stores available for upload.
        -> There's a disk problem. Check the disk.
    425 - Too early. There is an active ongoing transfer with this checksum.
        -> You have a logic error, you should never have tried to re-upload this.
           Either fail the ongoing transfer, or wait for it to complete.
    201 - Created staging area.
        -> Success! Please stage the file.
    """

    log.debug(f"Received clone initiation request from {user.username}: {request}")

    # Figure out which store to use.
    if request.upload_size < 0:
        log.debug(f"Upload size is negative. Returning error.")
        response.status_code = status.HTTP_400_BAD_REQUEST
        return CloneFailedResponse(
            reason="Upload size must be positive.",
            suggested_remedy="Check you are trying to upload a valid file.",
            source_transfer_id=request.source_transfer_id,
            destination_transfer_id=-1,
        )

    # Check that the upload is not already on the librarian.
    # For clone operations, this should never happen. They should have already
    # checked if we have the files or not using a different operation.
    if File.file_exists(request.destination_location):
        log.debug(
            f"File {request.destination_location} already exists on librarian. Returning error."
        )
        response.status_code = status.HTTP_409_CONFLICT
        return CloneFailedResponse(
            reason="File already exists on librarian.",
            suggested_remedy=(
                "Error in sharing logic. Your librarain should "
                "never have tried to copy this. Check the background task ordering."
            ),
            source_transfer_id=request.source_transfer_id,
            destination_transfer_id=-1,
        )

    # Check for an existing transfer. If we find one with a status of ONGOING, that is
    # again a logic error. They should not have tried to send us that again! It's already
    # on its way.

    existing_transfer = (
        session.query(IncomingTransfer)
        .filter(
            (IncomingTransfer.transfer_checksum == request.upload_checksum)
            & (IncomingTransfer.status != TransferStatus.FAILED)
            & (IncomingTransfer.status != TransferStatus.COMPLETED)
            & (IncomingTransfer.status != TransferStatus.CANCELLED)
        )
        .all()
    )

    if len(existing_transfer) != 0:
        log.info(
            f"Found {len(existing_transfer)} existing transfers with checksum "
            f"{request.upload_checksum}. Checking existing transfer status."
        )

        for transfer in existing_transfer:
            if transfer.status == TransferStatus.ONGOING:
                log.info(
                    f"Found existing transfer with status ONGOING. Returning error."
                )

                response.status_code = status.HTTP_425_TOO_EARLY
                return CloneFailedResponse(
                    reason="Transfer is ongoing.",
                    suggested_remedy=(
                        "Error in sharing logic. Your librarain is trying to send us a copy of "
                        "a file with an ONGOING transfer. Check the background task ordering."
                    ),
                    source_transfer_id=request.source_transfer_id,
                    destination_transfer_id=transfer.id,
                )

            # Alternative is status' of STAGED and INITIATED. Unlike with uploads, this is a
            # more dangerous situation - the other librarian will probably have an
            # OutgoingTransfer that matches! So we need to fail the existing transfer,
            # and tell them to fail theirs too.

            log.info(
                f"Found existing transfer with status {transfer.status}. Failing existing transfer."
            )

            # Unstage the files.
            if transfer.store_id is not None:
                store = session.get(StoreMetadata, transfer.store_id)
            else:
                store = None

            if store is not None:
                store.store_manager.unstage(Path(transfer.staging_path))

            transfer.status = TransferStatus.FAILED
            session.commit()

            response.status_code = status.HTTP_406_NOT_ACCEPTABLE

            return CloneFailedResponse(
                reason="Transfer was already initiated or staged.",
                suggested_remedy=(
                    "Your librarian tried to upload a file again that we thought was already "
                    "coming to us. You should fail your outgoing transfer, we have failed ours."
                ),
                source_transfer_id=request.source_transfer_id,
                destination_transfer_id=transfer.id,
            )

    # No existing transfer.

    transfer = IncomingTransfer.new_transfer(
        source=user.username,
        uploader=request.uploader,
        upload_name=str(request.upload_name),
        transfer_size=request.upload_size,
        transfer_checksum=request.upload_checksum,
    )

    session.add(transfer)
    session.commit()

    use_store: Optional[StoreMetadata] = None

    for store in session.query(StoreMetadata).filter_by(ingestable=True).all():
        if not (store.store_manager.available and store.enabled):
            continue

        if store.store_manager.free_space > request.upload_size:
            use_store = store
            break

    if use_store is None:
        log.debug(
            f"No stores available for upload, they are all full!. Returning error."
        )

        transfer.status = TransferStatus.FAILED
        session.commit()

        response.status_code = status.HTTP_413_REQUEST_ENTITY_TOO_LARGE
        return CloneFailedResponse(
            reason="No stores available for upload. Your upload is too large.",
            suggested_remedy="Check that the disk is not full.",
            source_transfer_id=request.source_transfer_id,
            destination_transfer_id=transfer.id,
        )

    # We have a store! Create the staging area.

    file_name, file_location = use_store.store_manager.stage(
        file_size=request.upload_size, file_name=request.upload_name
    )

    transfer.store_id = use_store.id
    transfer.staging_path = str(file_location)

    session.commit()

    response.status_code = status.HTTP_201_CREATED

    # TODO: Figure out what to do when we have lots of incoming transfers
    # where sum(sizes) for all those transfers > store_manager.free_space.

    model_response = CloneInitiationResponse(
        available_bytes_on_store=use_store.store_manager.free_space,
        store_name=use_store.name,
        staging_name=file_name,
        staging_location=file_location,
        upload_name=request.upload_name,
        destination_location=request.destination_location,
        source_transfer_id=request.source_transfer_id,
        destination_transfer_id=transfer.id,
        transfer_providers=use_store.transfer_managers,
    )

    log.debug(f"Returning clone initiation response: {model_response}")

    return model_response


@router.post("/ongoing", response_model=CloneOngoingResponse | CloneFailedResponse)
def ongoing(
    request: CloneOngoingRequest,
    response: Response,
    user: ReadappendUserDependency,
    session: Session = Depends(yield_session),
):
    """
    Called when the remote librarian has started the transfer. We should
    update the status of the transfer to ONGOING.

    You must have the correct username to update the transfer.

    Possible response codes:

    200 - OK. Transfer status updated.
    404 - Not found. Could not find transfer.
    406 - Not acceptable. Transfer is not in INITIATED status.
    """

    log.debug(f"Received clone ongoing request from {user.username}: {request}")

    transfer = (
        session.query(IncomingTransfer)
        .filter_by(
            id=request.destination_transfer_id,
            source=user.username,
        )
        .first()
    )

    if transfer is None:
        log.debug(
            f"Could not find transfer with ID {request.destination_transfer_id}. Returning error."
        )

        response.status_code = status.HTTP_404_NOT_FOUND
        return CloneFailedResponse(
            reason="Could not find transfer.",
            suggested_remedy=(
                "Your librarian is trying to tell us that a transfer is ongoing, but we cannot "
                "find the transfer. Check the background task ordering and that you have the correct "
                "username in the request."
            ),
            source_transfer_id=request.source_transfer_id,
            destination_transfer_id=request.destination_transfer_id,
        )

    if transfer.status != TransferStatus.INITIATED:
        log.debug(
            f"Transfer with ID {request.source_transfer_id} has status {transfer.status}."
            f"Trying to set it to ONGOING. Returning error."
        )

        response.status_code = status.HTTP_406_NOT_ACCEPTABLE
        return CloneFailedResponse(
            reason="Transfer is not in INITIATED status.",
            suggested_remedy=(
                "Your librarian is trying to tell us that a transfer is ongoing, but the transfer "
                "is not in INITIATED status. Check the background task ordering."
            ),
            source_transfer_id=request.source_transfer_id,
            destination_transfer_id=request.destination_transfer_id,
        )

    transfer.status = TransferStatus.ONGOING
    session.commit()

    response.status_code = status.HTTP_200_OK
    return CloneOngoingResponse(
        source_transfer_id=request.source_transfer_id,
        destination_transfer_id=request.destination_transfer_id,
    )


@router.post("/complete", response_model=CloneCompleteResponse | CloneFailedResponse)
def complete(
    request: CloneCompleteRequest,
    response: Response,
    user: CallbackUserDependency,
    session: Session = Depends(yield_session),
):
    """
    The callback from librarian B to librarian A that it has completed the
    transfer. Used to update anything in our OutgiongTransfers that needs it,
    as well as create the appropriate remote instances.

    Possible response codes:

    200 - OK. Transfer status updated.
    400 - Not found. Could not find transfer.
    400 - Bad request. Could not find librarian.
    406 - Not acceptable. Transfer is not in STAGED status.
    """

    log.debug(f"Received clone complete request from {user.username}: {request}")

    transfer = (
        session.query(OutgoingTransfer)
        .filter_by(
            id=request.source_transfer_id,
        )
        .first()
    )

    if transfer is None:
        log.debug(
            f"Could not find transfer with ID {request.source_transfer_id}. Returning error."
        )

        response.status_code = status.HTTP_400_BAD_REQUEST
        return CloneFailedResponse(
            reason="Could not find transfer.",
            suggested_remedy=(
                "We are trying to update the status of a transfer, but we cannot find the transfer. "
                "Check the background task ordering."
            ),
            source_transfer_id=request.source_transfer_id,
            destination_transfer_id=request.destination_transfer_id,
        )

    if transfer.status != TransferStatus.ONGOING:
        log.debug(
            f"Transfer with ID {request.source_transfer_id} has status {transfer.status}."
            f"Trying to set it from ONGOING to COMPLETED. Returning error."
        )

        response.status_code = status.HTTP_406_NOT_ACCEPTABLE
        return CloneFailedResponse(
            reason="Transfer is not in ONGOING status.",
            suggested_remedy=(
                "We are trying to update the status of a transfer, but the transfer is not in ONGOING "
                "status. Check the background task ordering. We should have already updated this status."
            ),
            source_transfer_id=request.source_transfer_id,
            destination_transfer_id=request.destination_transfer_id,
        )

    librarian = session.query(Librarian).filter_by(name=transfer.destination).first()

    if librarian is None:
        log.debug(f"Could not find librarian {transfer.destination}. Returning error.")

        response.status_code = status.HTTP_400_BAD_REQUEST
        return CloneFailedResponse(
            reason=f"Could not find librarian {transfer.destination}.",
            suggested_remedy=(
                f"Check your librarian configuration. The librarian {transfer.destination} needs "
                "to be an entry in this database. No remote instances will be created, and the "
                "transfer status will not be updated, so you can try again afterwards."
            ),
            source_transfer_id=request.source_transfer_id,
            destination_transfer_id=request.destination_transfer_id,
        )

    transfer.status = TransferStatus.COMPLETED

    # Create new remote instance for this file that was just completed.
    remote_instance = RemoteInstance.new_instance(
        file=transfer.file,
        store_id=request.store_id,
        librarian=librarian,
    )

    session.add(remote_instance)

    session.commit()

    response.status_code = status.HTTP_200_OK
    return CloneCompleteResponse(
        source_transfer_id=request.source_transfer_id,
        destination_transfer_id=request.destination_transfer_id,
    )


@router.post("/fail", response_model=CloneFailResponse | CloneFailedResponse)
def fail(
    request: CloneFailRequest,
    response: Response,
    user: ReadappendUserDependency,
    session: Session = Depends(yield_session),
):
    """
    Endpoint to send to if you would like to fail a specific IncomingTransfer.

    You must have the correct username to update the transfer.

    Possible response codes:

    200 - OK. Transfer status updated.
    404 - Not found. Could not find transfer.
    """

    log.debug(f"Received clone fail request from {user.username}: {request}")

    transfer = (
        session.query(IncomingTransfer)
        .filter_by(
            id=request.destination_transfer_id,
            source=user.username,
        )
        .first()
    )

    if transfer is None:
        log.debug(
            f"Could not find transfer with ID {request.destination_transfer_id}. Returning error."
        )

        response.status_code = status.HTTP_404_NOT_FOUND
        return CloneFailedResponse(
            reason=f"Could not find transfer with ID {request.destination_transfer_id}.",
            source_transfer_id=request.source_transfer_id,
            destination_transfer_id=request.destination_transfer_id,
        )

    transfer.status = TransferStatus.FAILED
    session.commit()

    response.status_code = status.HTTP_200_OK
    return CloneFailResponse(
        source_transfer_id=request.source_transfer_id,
        destination_transfer_id=request.destination_transfer_id,
        success=True,
    )
