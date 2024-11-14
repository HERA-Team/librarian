"""
API endpoint for cloning data from one librarian to another.
This endpoint is by construction asynchronous; the two different
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

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from hera_librarian.models.clone import (
    CloneBatchFailedResponse,
    CloneBatchInitiationRequest,
    CloneBatchInitiationResponse,
    CloneBatchInitiationResponseFileItem,
    CloneCompleteRequest,
    CloneCompleteResponse,
    CloneFailedResponse,
    CloneFailRequest,
    CloneFailResponse,
    CloneInitiationRequest,
    CloneInitiationResponse,
    CloneOngoingRequest,
    CloneOngoingResponse,
    CloneStagedRequest,
    CloneStagedResponse,
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


def validate_staging(
    session: Session, upload_size: int, source_transfer_id: int, response: Response
) -> StoreMetadata:
    """
    Validates the upload size and returns a valid store that can fit
    the requested data size.
    """
    # Figure out which store to use.
    if upload_size < 0:
        log.debug(f"Upload size is negative. Returning error.")
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail=CloneFailedResponse(
                reason="Upload size must be positive.",
                suggested_remedy="Check you are trying to upload a valid file.",
                source_transfer_id=source_transfer_id,
                destination_transfer_id=-1,
            ).model_dump_json(),
        )

    use_store: Optional[StoreMetadata] = None

    for store in session.query(StoreMetadata).filter_by(ingestable=True).all():
        if not (store.store_manager.available and store.enabled):
            continue

        if store.store_manager.free_space > upload_size:
            use_store = store
            break

    if use_store is None:
        log.debug(
            f"No stores available for upload, they are all full!. Returning error."
        )

        raise HTTPException(
            status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=CloneFailedResponse(
                reason="No stores available for upload. Your upload is too large.",
                suggested_remedy="Check that the disk is not full.",
                source_transfer_id=source_transfer_id,
                destination_transfer_id=-1,
            ).model_dump_json(),
        )

    return use_store


def de_duplicate_file_and_transfer(
    session: Session,
    source_transfer_id: int,
    source: str,
    uploader: str,
    upload_size: int,
    upload_checksum: str,
    upload_name: str,
    destination_location: str,
) -> IncomingTransfer:
    """
    Search for already-existing files and transfers, and de-duplicate
    them as necessary. This is important in case somebody tries to upload
    a file twice, or they start an upload and the actual file transfer is
    cut-off, meaning they then try again.
    """

    # First, check if we already have this file; if we do, then cancel
    # the whole business.
    if session.get(File, str(destination_location)):
        log.debug(
            f"File {destination_location} already exists on librarian. Returning error."
        )

        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail=CloneFailedResponse(
                reason="File already exists on librarian.",
                suggested_remedy=(
                    "Error in sharing logic. Your librarain should "
                    "never have tried to copy this. Check the background task ordering."
                ),
                source_transfer_id=source_transfer_id,
                destination_transfer_id=-1,
            ).model_dump_json(),
        )

    # Reaching here, we do NOT already have the file. But maybe there is already
    # an existing transfer to us!

    stmt = select(IncomingTransfer)
    stmt = stmt.filter_by(
        transfer_checksum=upload_checksum,
        store_path=str(destination_location),
    )
    stmt = stmt.filter(
        IncomingTransfer.status.not_in(
            [TransferStatus.FAILED, TransferStatus.CANCELLED, TransferStatus.COMPLETED]
        )
    )

    existing_transfer = session.execute(stmt).scalars().one_or_none()

    if existing_transfer is not None:
        log.info(
            "Found an existing transfers with checksum "
            f"{upload_checksum}. Checking existing transfer status."
        )

        if existing_transfer.status == TransferStatus.ONGOING:
            log.info(f"Found existing transfer with status ONGOING. Returning error.")

            raise HTTPException(
                status.HTTP_425_TOO_EARLY,
                detail=CloneFailedResponse(
                    reason="Transfer is ongoing.",
                    suggested_remedy=(
                        "Error in sharing logic. Your librarain is trying to send us a copy of "
                        "a file with an ONGOING transfer. Check the background task ordering."
                    ),
                    source_transfer_id=source_transfer_id,
                    destination_transfer_id=existing_transfer.id,
                ).model_dump_json(),
            )

        # Alternative is status' of STAGED and INITIATED. Unlike with uploads, this is a
        # more dangerous situation - the other librarian will probably have an
        # OutgoingTransfer that matches! So we need to fail the existing transfer,
        # and tell them to fail theirs too.

        log.info(
            f"Found existing transfer with status {existing_transfer.status}. "
            "Failing existing transfer."
        )

        # Unstage the files.
        if existing_transfer.store_id is not None:
            store = session.get(StoreMetadata, existing_transfer.store_id)
            if store is not None:
                store.store_manager.unstage(Path(existing_transfer.staging_path))

        existing_transfer.status = TransferStatus.FAILED
        session.commit()

        raise HTTPException(
            status.HTTP_406_NOT_ACCEPTABLE,
            detail=CloneFailedResponse(
                reason="Transfer was already initiated or staged.",
                suggested_remedy=(
                    "Your librarian tried to upload a file again that we thought was already "
                    "coming to us. You should fail your outgoing transfer, we have failed ours."
                ),
                source_transfer_id=source_transfer_id,
                destination_transfer_id=existing_transfer.id,
            ).model_dump_json(),
        )

    # Ok, we don't have an existing transfer. We need to make a new one.

    transfer = IncomingTransfer.new_transfer(
        source=source,
        uploader=uploader,
        upload_name=str(upload_name),
        transfer_size=upload_size,
        transfer_checksum=upload_checksum,
    )

    transfer.source_transfer_id = source_transfer_id

    return transfer


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

    store = validate_staging(
        session=session,
        upload_size=request.upload_size,
        source_transfer_id=request.source_transfer_id,
        response=response,
    )

    transfer = de_duplicate_file_and_transfer(
        session=session,
        source_transfer_id=request.source_transfer_id,
        source=request.source,
        uploader=user.username,
        upload_size=request.upload_size,
        upload_checksum=request.upload_checksum,
        destination_location=request.destination_location,
        upload_name=request.upload_name,
    )

    session.add(transfer)
    session.commit()

    # We have a store! Create the staging area.

    file_name, file_location = store.store_manager.stage(
        file_size=request.upload_size, file_name=request.upload_name
    )

    transfer.store_id = store.id
    # Crucial to have this be the staging name, as is in the upload.
    transfer.staging_path = str(file_name)

    # Set store path now as it will not change.
    transfer.store_path = str(request.destination_location)

    session.commit()

    response.status_code = status.HTTP_201_CREATED

    model_response = CloneInitiationResponse(
        available_bytes_on_store=store.store_manager.free_space,
        store_name=store.name,
        staging_name=file_name,
        staging_location=file_location,
        upload_name=request.upload_name,
        destination_location=request.destination_location,
        source_transfer_id=request.source_transfer_id,
        destination_transfer_id=transfer.id,
        transfer_providers=store.transfer_managers,
    )

    log.debug(f"Returning clone initiation response: {model_response}")

    return model_response


@router.post(
    "/batch_stage",
    response_model=CloneBatchInitiationResponse | CloneBatchFailedResponse,
)
def batch_stage(
    request: CloneBatchInitiationRequest,
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

    log.debug(
        f"Recieved a batch clone initiation request for {len(request.uploads)} "
        f"({request.total_size} B) from {user.username}"
    )

    store = validate_staging(
        session=session,
        upload_size=request.total_size,
        # TODO: Figure out how to deal with this source_transfer_id being a single number
        # for bach uploads.
        source_transfer_id=-1,
        response=response,
    )

    clones = []
    bad_ids_exist = []
    bad_ids_transfer_exist = []
    bad_ids_ongoing = []
    transfers = []

    for upload in request.uploads:
        try:
            transfer = de_duplicate_file_and_transfer(
                session=session,
                source_transfer_id=upload.source_transfer_id,
                source=request.source,
                uploader=user.username,
                upload_size=upload.upload_size,
                upload_checksum=upload.upload_checksum,
                destination_location=upload.destination_location,
                upload_name=upload.upload_name,
            )
        except HTTPException as e:
            log.warning(f"Error in batch staging: {e}")

            if e.status_code == status.HTTP_409_CONFLICT:
                bad_ids_exist.append(upload.source_transfer_id)
            elif e.status_code == status.HTTP_406_NOT_ACCEPTABLE:
                bad_ids_transfer_exist.append(upload.source_transfer_id)
            elif e.status_code == status.HTTP_425_TOO_EARLY:
                bad_ids_ongoing.append(upload.source_transfer_id)
            else:
                log.error(f"Unknown error in batch staging: {e}")

            continue

        transfers.append(transfer)

    n_bad = len(bad_ids_exist) + len(bad_ids_transfer_exist) + len(bad_ids_ongoing)

    if n_bad > 0:
        # A bad batch. No worries, though, we never actually added
        # those transfer objects. We can only truly handle a single
        # error at once, though, so we will prioritize.
        if len(bad_ids_ongoing) > 0:
            response.status_code = status.HTTP_425_TOO_EARLY
            return CloneBatchFailedResponse(
                reason="Transfer is ongoing.",
                suggested_remedy=(
                    "Error in sharing logic. Your librarian is trying to send us a copy of "
                    "a file with an ONGOING transfer. Check the background task ordering."
                ),
                source_transfer_ids=bad_ids_ongoing,
                destination_transfer_ids=[-1] * len(bad_ids_ongoing),
            )
        elif len(bad_ids_exist) > 0:
            response.status_code = status.HTTP_409_CONFLICT
            return CloneBatchFailedResponse(
                reason="File already exists on librarian.",
                suggested_remedy=(
                    "Error in sharing logic. Your librarain should "
                    "never have tried to copy this. Check the background task ordering."
                ),
                source_transfer_ids=bad_ids_exist,
                destination_transfer_ids=[-1] * len(bad_ids_exist),
            )
        elif len(bad_ids_transfer_exist) > 0:
            response.status_code = status.HTTP_406_NOT_ACCEPTABLE
            return CloneBatchFailedResponse(
                reason="Transfer was already initiated or staged.",
                suggested_remedy=(
                    "Your librarian tried to upload a file again that we thought was already "
                    "coming to us. You should fail your outgoing transfer, we have failed ours."
                ),
                source_transfer_ids=bad_ids_transfer_exist,
                destination_transfer_ids=[-1] * len(bad_ids_transfer_exist),
            )
        else:
            log.error("Unknown error in batch staging.")
            response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            return CloneBatchFailedResponse(
                reason="Unknown error.",
                suggested_remedy="Check the logs.",
                source_transfer_ids=[-1],
                destination_transfer_ids=[-1],
            )

    session.add_all(transfers)
    session.commit()

    for upload, transfer in zip(request.uploads, transfers):
        # Now we have a handle on the transfer, let's stage it.
        file_name, file_location = store.store_manager.stage(
            file_size=upload.upload_size,
            file_name=upload.upload_name,
        )

        transfer.store_id = store.id
        # Crucial to have this be the staging name, as is in the upload.
        transfer.staging_path = str(file_name)

        # Set store path now as it will not change.
        transfer.store_path = str(upload.destination_location)

        # Don't bother comitting every time, that's a waste as the next
        # transfer creation will commit the session anyway.

        clones.append(
            CloneBatchInitiationResponseFileItem(
                staging_name=file_name,
                staging_location=file_location,
                upload_name=upload.upload_name,
                destination_location=upload.destination_location,
                source_transfer_id=upload.source_transfer_id,
                destination_transfer_id=transfer.id,
            )
        )

    # One final commit to make sure we got that last store ID in there.
    session.commit()

    log.debug(f"Returning batch clone initiation response for {len(clones)}.")

    response.status_code = status.HTTP_201_CREATED

    if store.async_transfer_managers == {}:
        log.error("Request to stage to a store that has no async transfer managers.")

    model_response = CloneBatchInitiationResponse(
        available_bytes_on_store=store.store_manager.free_space,
        store_name=store.name,
        uploads=clones,
        async_transfer_providers=store.async_transfer_managers,
    )

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


@router.post("/staged", response_model=CloneStagedResponse | CloneFailedResponse)
def staged(
    request: CloneStagedRequest,
    response: Response,
    user: CallbackUserDependency,
    session: Session = Depends(yield_session),
):
    """
    Called when the remote librarian has completed the transfer. We should
    update the status of the transfer to STAGED.

    You must have the correct username to update the transfer.

    Possible response codes:

    200 - OK. Transfer status updated.
    404 - Not found. Could not find transfer.
    406 - Not acceptable. Transfer is not in ONGOING status.
    """
    log.debug(f"Received clone staged request from {user.username}: {request}")

    transfer = (
        session.query(IncomingTransfer)
        .filter_by(
            id=request.destination_transfer_id,
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

    if transfer.status != TransferStatus.ONGOING:
        log.debug(
            f"Transfer with ID {request.source_transfer_id} has status {transfer.status}."
            f"Trying to set it to STAGED. Returning error."
        )

        response.status_code = status.HTTP_406_NOT_ACCEPTABLE
        return CloneFailedResponse(
            reason="Transfer is not in ONGOING status.",
            suggested_remedy=(
                "Your librarian is trying to tell us that a transfer is STAGED, but the transfer "
                "is not in ONGOING status. Check the background task ordering."
            ),
            source_transfer_id=request.source_transfer_id,
            destination_transfer_id=request.destination_transfer_id,
        )

    transfer.status = TransferStatus.STAGED
    session.commit()

    response.status_code = status.HTTP_200_OK
    return CloneStagedResponse(
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

    Administrator only: can complete other user's transfers.

    Possible response codes:

    200 - OK. Transfer status updated.
    400 - Not found. Could not find transfer.
    400 - Bad request. Could not find librarian.
    406 - Not acceptable. Transfer is not in STAGED status.
    """

    log.debug(f"Received clone complete request from {user.username}: {request}")

    query = select(OutgoingTransfer)

    query = query.where(OutgoingTransfer.id == request.source_transfer_id)

    if not user.is_admin:
        query = query.where(OutgoingTransfer.destination == user.username)

    transfer = session.execute(query).scalars().one_or_none()

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

    if transfer.status not in [TransferStatus.ONGOING, TransferStatus.STAGED]:
        log.debug(
            f"Transfer with ID {request.source_transfer_id} has status {transfer.status}."
            f"Trying to set it from ONGOING/STAGED to COMPLETED. Returning error."
        )

        response.status_code = status.HTTP_406_NOT_ACCEPTABLE
        return CloneFailedResponse(
            reason="Transfer is not in ONGOING or STAGED status.",
            suggested_remedy=(
                "We are trying to update the status of a transfer, but the transfer is not in ONGOING "
                "or STAGED status. Check the background task ordering. "
                "We should have already updated this status."
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

    You must have the correct username to update the transfer if you are not an
    administrator of this instance.

    Possible response codes:

    200 - OK. Transfer status updated.
    404 - Not found. Could not find transfer.
    """

    log.debug(f"Received clone fail request from {user.username}: {request}")

    query = select(IncomingTransfer)

    query = query.where(IncomingTransfer.id == request.destination_transfer_id)

    if not user.is_admin:
        query = query.where(IncomingTransfer.uploader == user.username)

    transfer = session.execute(query).scalars().one_or_none()

    log.debug(
        f"Result of transfer query for ID {request.destination_transfer_id}: {transfer}"
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
