"""
Contains API endpoints for uploading data to the Librarian and its
stores.
"""

from ..webutil import ServerError
from ..orm.storemetadata import StoreMetadata
from ..orm.transfer import TransferStatus, IncomingTransfer
from ..orm.file import File
from ..database import session, query
from ..logger import log

from hera_librarian.models.uploads import (
    UploadInitiationRequest,
    UploadInitiationResponse,
    UploadCompletionRequest,
    UploadFailedResponse,
)

from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Response, status

router = APIRouter(prefix="/api/v2/upload")


@router.post("/stage", response_model=UploadInitiationResponse | UploadFailedResponse)
def stage(request: UploadInitiationRequest, response: Response):
    """
    Initiates an upload to a store.

    Stages a file, and returns information about the transfer
    providers that can be used by the client to upload the file.

    Possible response codes:

    400 - Bad request. Upload size is negative.
    409 - Conflict. File already exists on librarian.
    413 - Request entity too large. No stores available for upload.
    201 - Created staging area.
    """

    log.debug(f"Received upload initiation request: {request}")

    # Figure out which store to use.
    if request.upload_size < 0:
        log.debug(f"Upload size is negative. Returning error.")
        response.status_code = status.HTTP_400_BAD_REQUEST
        return UploadFailedResponse(
            reason="Upload size must be positive.",
            suggested_remedy="Check you are trying to upload a valid file.",
        )

    # Check that the upload is not already on the librarian.
    if File.file_exists(request.destination_location):
        log.debug(
            f"File {request.destination_location} already exists on librarian. Returning error."
        )
        response.status_code = status.HTTP_409_CONFLICT
        return UploadFailedResponse(
            reason="File already exists on librarian.",
            suggested_remedy="Check that you are not trying to upload a file that already exists on the librarian, and if it does not choose a unique filename that does not already exist.",
        )

    # First, try to see if this is someone trying to re-start an existing transfer!
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
            f"Found {len(existing_transfer)} existing transfers with checksum {request.upload_checksum}. Failing existing transfer."
        )

        for transfer in existing_transfer:
            # Unstage the files.
            store = StoreMetadata.from_id(transfer.store_id)
            store.store_manager.unstage(Path(transfer.staging_path))

            transfer.status = TransferStatus.FAILED

        session.commit()

    # Now we can write to the database.
    transfer = IncomingTransfer.new_transfer(
        source=request.uploader,
        uploader=request.uploader,
        transfer_size=request.upload_size,
        transfer_checksum=request.upload_checksum,
    )

    session.add(transfer)
    session.commit()

    use_store: Optional[StoreMetadata] = None

    for store in query(StoreMetadata, ingestable=True).all():
        if not store.store_manager.available:
            continue

        if store.store_manager.free_space > request.upload_size:
            use_store = store
            break

    if use_store is None:
        log.debug(
            f"No stores available for upload, they are all full!. Returning error."
        )
        response.status_code = status.HTTP_413_REQUEST_ENTITY_TOO_LARGE

        # Fail the transfer
        transfer.status = TransferStatus.FAILED
        session.commit()

        return UploadFailedResponse(
            reason="No stores available for upload. Your upload is too large.",
            suggested_remedy="Try again later, or try to upload a smaller file. Contact the administrator of this librarian instance.",
        )

    # Now generate the response; tell client to use this store, and keep a record.

    # Stage the file
    file_name, file_location = use_store.store_manager.stage(
        file_size=request.upload_size, file_name=request.upload_name
    )

    transfer.store_id = use_store.id
    # SQLAlchemy cannot handle path objects; serialize to string.
    transfer.staging_path = str(file_name)

    session.commit()

    response.status_code = status.HTTP_201_CREATED

    model_response = UploadInitiationResponse(
        available_bytes_on_store=use_store.store_manager.free_space,
        store_name=use_store.name,
        staging_name=file_name,
        staging_location=file_location,
        upload_name=request.upload_name,
        destination_location=request.destination_location,
        transfer_providers=use_store.transfer_managers,
        transfer_id=transfer.id,
    )

    log.debug(f"Returning upload initiation response: {model_response}")

    return model_response


@router.post("/commit")
def commit(request: UploadCompletionRequest, response: Response):
    """
    Commits a file to a store, called once it has been uploaded.

    Possible response codes:
    409 - Conflict. File already exists on store.
    500 - Internal server error. Database communication issue.
    200 - OK. Upload succeeded.
    """

    log.debug(f"Received upload completion request: {request}")

    store: StoreMetadata = StoreMetadata.from_name(request.store_name)

    # Go grab the transfer from the database.
    transfer = query(IncomingTransfer, id=request.transfer_id).first()
    transfer.status = TransferStatus.STAGED
    transfer.transfer_manager_name = request.transfer_provider_name
    # DB cannot handle path objects; serialize to string.
    transfer.store_path = str(request.destination_location)

    session.commit()

    try:
        store.ingest_staged_file(
            request=request,
            transfer=transfer,
        )
    except FileExistsError:
        log.debug(
            f"File {request.destination_location} already exists on store. Returning error."
        )
        response.status_code = status.HTTP_409_CONFLICT
        return UploadFailedResponse(
            reason="File already exists on store.",
            suggested_remedy="Check that you are not trying to upload a file that already exists on the store, and if it does not choose a unique filename that does not already exist.",
        )
    except ServerError as e:
        log.debug(
            "Extremely bad internal server error. Likley a database communication issue."
        )
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return UploadFailedResponse(
            reason="Internal server error.",
            suggested_remedy="Contact the administrator of this librarian instance.",
        )

    log.debug(f"Returning upload completion response. Upload succeeded.")

    response.status_code = status.HTTP_200_OK

    return response
