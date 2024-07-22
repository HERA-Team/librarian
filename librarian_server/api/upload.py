"""
Contains API endpoints for uploading data to the Librarian and its
stores.
"""

from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from hera_librarian.deletion import DeletionPolicy
from hera_librarian.models.uploads import (
    UploadCompletionRequest,
    UploadFailedResponse,
    UploadInitiationRequest,
    UploadInitiationResponse,
)

from ..database import yield_session
from ..logger import log
from ..orm.file import File
from ..orm.storemetadata import StoreMetadata
from ..orm.transfer import IncomingTransfer, TransferStatus
from ..settings import server_settings
from .auth import ReadappendUserDependency, UnauthorizedError

router = APIRouter(prefix="/api/v2/upload")


@router.post("/stage", response_model=UploadInitiationResponse | UploadFailedResponse)
def stage(
    request: UploadInitiationRequest,
    response: Response,
    user: ReadappendUserDependency,
    session: Session = Depends(yield_session),
):
    """
    Initiates an upload to a store.

    Stages a file, and returns information about the transfer
    providers that can be used by the client to upload the file.

    Possible response codes:

    400 - Bad request. Upload size is negative.
    409 - Conflict. File already exists on librarian.
    413 - Request entity too large. No stores available for upload, or it is
          larger than the requested maximum by the server.
    201 - Created staging area.
    """

    log.debug(f"Received upload initiation request from {user.username}: {request}")

    # Figure out which store to use.
    if request.upload_size < 0:
        log.debug(f"Upload size is negative. Returning error.")
        response.status_code = status.HTTP_400_BAD_REQUEST
        return UploadFailedResponse(
            reason="Upload size must be positive.",
            suggested_remedy="Check you are trying to upload a valid file.",
        )

    if request.upload_size > server_settings.maximal_upload_size_bytes:
        log.debug(f"Upload size is too large. Returning error.")
        response.status_code = status.HTTP_413_REQUEST_ENTITY_TOO_LARGE
        return UploadFailedResponse(
            reason="Upload size is too large.",
            suggested_remedy=(
                "Try again later, or try to upload a smaller file. Contact "
                "the administrator of this librarian instance."
            ),
        )

    # Check that the upload is not already on the librarian.
    if File.file_exists(request.destination_location):
        log.debug(
            f"File {request.destination_location} already exists on librarian. Returning error."
        )
        response.status_code = status.HTTP_409_CONFLICT
        return UploadFailedResponse(
            reason="File already exists on librarian.",
            suggested_remedy=(
                "Check that you are not trying to upload a file that already exists on the librarian, "
                "and if it does not choose a unique filename that does not already exist."
            ),
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
            f"Found {len(existing_transfer)} existing transfers with "
            f"checksum {request.upload_checksum}. Failing existing transfer."
        )

        for transfer in existing_transfer:
            # Unstage the files.
            if transfer.store_id is not None:
                store = session.get(StoreMetadata, transfer.store_id)
            else:
                store = None

            if store is not None:
                store.store_manager.unstage(Path(transfer.staging_path))

            transfer.status = TransferStatus.FAILED

        session.commit()

    # Now we can write to the database.
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
        response.status_code = status.HTTP_413_REQUEST_ENTITY_TOO_LARGE

        # Fail the transfer
        transfer.status = TransferStatus.FAILED
        session.commit()

        return UploadFailedResponse(
            reason="No stores available for upload. Your upload is too large.",
            suggested_remedy=(
                "Try again later, or try to upload a smaller file. Contact "
                "the administrator of this librarian instance."
            ),
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
def commit(
    request: UploadCompletionRequest,
    response: Response,
    user: ReadappendUserDependency,
    session: Session = Depends(yield_session),
):
    """
    Commits a file to a store, called once it has been uploaded.

    Possible response codes:
    406 - Not acceptable. File does not have a valid checksum or size.
    409 - Conflict. File already exists on store.
    500 - Internal server error. Database communication issue.
    200 - OK. Upload succeeded.
    """

    log.debug(f"Received upload completion request from {user.username}: {request}")

    store: StoreMetadata = (
        session.query(StoreMetadata).filter_by(name=request.store_name).first()
    )

    # Go grab the transfer from the database.
    transfer = session.get(IncomingTransfer, request.transfer_id)

    if not transfer.source == user.username:
        raise UnauthorizedError

    transfer.status = TransferStatus.STAGED
    transfer.transfer_manager_name = request.transfer_provider_name
    # DB cannot handle path objects; serialize to string.
    transfer.store_path = str(request.destination_location)

    session.commit()

    try:
        # This function handles failing the transfer in the case where
        # that needs to happen. All we need to do is return the appropriate
        # HTTP response code and data.
        store.ingest_staged_file(
            transfer=transfer,
            session=session,
            deletion_policy=DeletionPolicy.from_str(request.deletion_policy),
        )
    except FileNotFoundError:
        log.error(
            f"File {request.staging_location} not found in staging area. Returning error"
        )
        response.status_code = status.HTTP_404_NOT_FOUND
        return UploadFailedResponse(
            reason="File not found in staging area.",
            suggested_remedy="Try to transfer the file again by creating a new transfer, "
            "this one was failed. If the problem persists, "
            "contact the administrator of this librarian instance.",
        )
    except FileExistsError:
        log.error(
            f"File {request.destination_location} already exists on store. Returning error."
        )
        response.status_code = status.HTTP_409_CONFLICT
        return UploadFailedResponse(
            reason="File already exists on store.",
            suggested_remedy=(
                "Check that you are not trying to upload a file that "
                "already exists on the store, and if it does not choose a "
                "unique filename that does not already exist."
            ),
        )
    except ValueError as e:
        log.error(
            f"File {request.destination_location} does not have a valid "
            f"checksum or size. Returning error: {e}"
        )
        response.status_code = status.HTTP_406_NOT_ACCEPTABLE
        return UploadFailedResponse(
            reason="File does not have a valid checksum or size",
            suggested_remedy="Try to transfer the file again. If the problem persists, "
            "contact the administrator of this librarian instance.",
        )
    except Exception as e:
        import traceback

        log.error(
            "Extremely bad internal server error. Likley a database communication issue. "
            f"Error: {e}, Traceback:\n{traceback.format_exc()}"
        )
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return UploadFailedResponse(
            reason="Internal server error.",
            suggested_remedy="Contact the administrator of this librarian instance.",
        )

    log.debug(f"Returning upload completion response. Upload succeeded.")

    response.status_code = status.HTTP_200_OK

    return response
