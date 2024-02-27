"""
Administration endpoints. Used for managing the librarian server,
and handling in-place updates to the server (e.g. adding File and
Instance objects to the database, updating the database, etc. without
actually ingesting files).
"""

from pathlib import Path

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from hera_librarian.deletion import DeletionPolicy
from hera_librarian.models.admin import (
    AdminCreateFileRequest,
    AdminCreateFileResponse,
    AdminRequestFailedResponse,
    AdminStoreListItem,
    AdminStoreListResponse,
    AdminStoreManifestRequest,
    AdminStoreManifestResponse,
    ManifestEntry,
)

from ..database import yield_session
from ..orm import File, Instance, StoreMetadata
from ..stores import InvertedStoreNames, StoreNames
from .auth import AdminUserDependency

router = APIRouter(prefix="/api/v2/admin")


@router.post("/add_file")
def add_file(
    request: AdminCreateFileRequest,
    user: AdminUserDependency,
    response: Response,
    session: Session = Depends(yield_session),
):
    """
    Creates a new file and instance in the database, assuming
    that a file already exists. If the file does not exist on the
    store already, we error out.
    """

    # First, get the store.
    store = (
        session.query(StoreMetadata).filter_by(name=request.store_name).one_or_none()
    )

    if store is None:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return AdminRequestFailedResponse(
            reason=f"Store {request.store_name} does not exist.",
            suggested_remedy="Create the store first. Maybe you need to run DB migration?",
        )

    # TODO: Can't do code coverage until we add nonlocal stores.
    if store.store_type != StoreNames["local"]:  # pragma: no cover
        response.status_code = status.HTTP_400_BAD_REQUEST
        return AdminRequestFailedResponse(
            reason=f"Store {request.store_name} is not a local store.",
            suggested_remedy="Use a local store for this operation.",
        )

    # Check if the file exists already.
    existing_file = session.get(File, request.name)

    if existing_file is not None:
        return AdminCreateFileResponse(already_exists=True, success=True)

    # Check the file instance exists.
    full_path = Path(request.path)

    if not full_path.exists():
        response.status_code = status.HTTP_400_BAD_REQUEST
        return AdminRequestFailedResponse(
            reason=f"File {full_path} does not exist.",
            suggested_remedy="Create the file first, or make sure that you are using a local store.",
        )

    # Create the file and instance.
    new_file = File.new_file(
        filename=request.name,
        size=request.size,
        checksum=request.checksum,
        uploader=request.uploader,
        source=request.source,
    )

    new_instance = Instance.new_instance(
        path=request.path,
        file=new_file,
        deletion_policy=DeletionPolicy.DISALLOWED,
        store=store,
    )

    session.add_all([new_file, new_instance])

    session.commit()

    return AdminCreateFileResponse(success=True, file_exists=True)


@router.post("/store_list")
def store_list(
    user: AdminUserDependency,
    response: Response,
    session: Session = Depends(yield_session),
):
    """
    Returns a list of all stores in the database with some basic information
    about them.
    """

    stores = session.query(StoreMetadata).all()

    return AdminStoreListResponse(
        [
            AdminStoreListItem(
                name=store.name,
                store_type=InvertedStoreNames[store.store_type],
                free_space=store.store_manager.free_space,
                ingestable=store.ingestable,
                available=store.store_manager.available,
            )
            for store in stores
        ]
    )


@router.post(
    "/store_manifest",
    response_model=AdminStoreManifestResponse | AdminRequestFailedResponse,
)
def store_manifest(
    request: AdminStoreManifestRequest,
    user: AdminUserDependency,
    response: Response,
    session: Session = Depends(yield_session),
):
    """
    Retrives the manifest of an entire store. Returns as JSON. This will
    be a very large request and response, so use with caution.
    """

    # First, get the store.
    store = (
        session.query(StoreMetadata).filter_by(name=request.store_name).one_or_none()
    )

    if store is None:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return AdminRequestFailedResponse(
            reason=f"Store {request.store_name} does not exist.",
            suggested_remedy="Create the store first. Maybe you need to run DB migration?",
        )

    # Get the list of instances.
    instances = session.query(Instance).filter_by(store=store).all()

    return AdminStoreManifestResponse(
        store_name=store.name,
        store_files=[
            ManifestEntry(
                name=instance.file.name,
                create_time=instance.file.create_time,
                size=instance.file.size,
                checksum=instance.file.checksum,
                uploader=instance.file.uploader,
                source=instance.file.source,
                instance_path=instance.path,
                deletion_policy=instance.deletion_policy,
                instance_create_time=instance.created_time,
                instance_available=instance.available,
            )
            for instance in instances
        ],
    )
