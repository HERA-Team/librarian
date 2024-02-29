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
    AdminStoreStateChangeRequest,
    AdminStoreStateChangeResponse,
    ManifestEntry,
)

from ..database import yield_session
from ..logger import log
from ..orm import File, Instance, Librarian, OutgoingTransfer, StoreMetadata
from ..settings import server_settings
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


@router.post("/stores/list")
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
                enabled=store.enabled,
            )
            for store in stores
        ]
    )


@router.post(
    "/stores/state_change",
    response_model=AdminStoreStateChangeResponse | AdminRequestFailedResponse,
)
def store_state_change(
    request: AdminStoreStateChangeRequest,
    user: AdminUserDependency,
    response: Response,
    session: Session = Depends(yield_session),
) -> AdminStoreStateChangeResponse:
    """
    Endpoint for changing the 'enabled' state of a store. You can use
    this to disable stores that you temporarly want to disable access
    to (such as swapping out sneaker net disks, etc.).
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

    # Now, change the state.
    store.enabled = request.enabled

    session.commit()

    # Get that store again.
    store = (
        session.query(StoreMetadata).filter_by(name=request.store_name).one_or_none()
    )

    return AdminStoreStateChangeResponse(
        store_name=store.name, success=True, enabled=store.enabled
    )


@router.post(
    "/stores/manifest",
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
    be a very large request and response, so use with caution. You can then
    ingest the manifest items individually into a different instance of
    the librarian, thus completing the 'sneakernet' process.

    This is a very powerful endpoint, and has the following options
    configured with its request:

    - create_outgoing_transfers: If true, will create outgoing transfers
        for each file in the manifest. This is useful for sneakernetting
        files to a different librarian.

    - destination_librarian: The name of the librarian to send the files to,
        if create_outgoing_transfers is true. This is required if you are
        creating outgoing transfers.

    - disable_store: If true, will disable the store after creating the
        outgoing transfers. This is useful for sneakernetting files to a
        different librarian, as it allows you to (in one transaction)
        generate all the outgoing transfers, then disable the store.

    - mark_local_instances_as_unavailable: If true, will mark the local
        instances as unavailable after creating the outgoing transfers.

    An easy sneakernet workflow is to set all of these to true. This will
    generate a complete manifest, create outgoing transfers for each file,
    then disable the store and mark the local instances as unavailable
    (as we are assuming you are going to then remove the files from the
    disks entirely).
    """

    log.debug(f"Recieved manifest request from {user.username}: {request}.")

    # First, get the store.
    store = (
        session.query(StoreMetadata).filter_by(name=request.store_name).one_or_none()
    )

    if store is None:
        response.status_code = status.HTTP_400_BAD_REQUEST

        log.warning(f"Store {request.store_name} does not exist. Returning 400.")

        return AdminRequestFailedResponse(
            reason=f"Store {request.store_name} does not exist.",
            suggested_remedy="Create the store first. Maybe you need to run DB migration?",
        )

    # Now, stop anyone from ingesting any new files if we want the store
    # to be disabled at the end of this process.

    if request.disable_store:
        store.enabled = False
        session.commit()
        log.info(f"Disabled store {store.name}.")

    # If we are going to create outgoing transfers, we need to make sure
    # that the destination librarian exists.

    outgoing_librarian: Librarian | None = None

    if request.create_outgoing_transfers:
        outgoing_librarian = (
            session.query(Librarian)
            .filter_by(name=request.destination_librarian)
            .one_or_none()
        )

        if outgoing_librarian is None:
            response.status_code = status.HTTP_400_BAD_REQUEST

            log.warning(
                f"Librarian {request.destination_librarian} does not exist, and "
                "user requested transfers to be created. Returning 400."
            )

            return AdminRequestFailedResponse(
                reason=f"Librarian {request.destination_librarian} does not exist.",
                suggested_remedy="Create the librarian first in the database.",
            )

    # Get the list of instances.
    instances = session.query(Instance).filter_by(store=store).all()

    def create_manifest_entry(instance: Instance) -> ManifestEntry:
        "Create a linked transfer and manifest entry based on logic."

        if request.create_outgoing_transfers:
            transfer = OutgoingTransfer.new_transfer(
                destination=outgoing_librarian.name,
                instance=instance,
                file=instance.file,
            )

            session.add(transfer)
            session.commit()

            transfer_id = transfer.id
        else:
            transfer_id = -1

        entry = ManifestEntry(
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
            outgoing_transfer_id=transfer_id,
        )

        if request.mark_local_instances_as_unavailable:
            instance.available = False
            session.commit()

        return entry

    response = AdminStoreManifestResponse(
        librarian_name=server_settings.name,
        store_name=store.name,
        store_files=[
            create_manifest_entry(instance)
            for instance in instances
            if instance.available
        ],
    )

    log.info(
        f"Generated manifest for store {store.name} containing {len(response.store_files)} files."
    )

    return response
