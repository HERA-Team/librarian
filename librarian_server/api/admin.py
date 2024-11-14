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
from hera_librarian.exceptions import LibrarianHTTPError
from hera_librarian.models.admin import (
    AdminAddLibrarianRequest,
    AdminAddLibrarianResponse,
    AdminChangeLibrarianTransferStatusRequest,
    AdminCreateFileRequest,
    AdminCreateFileResponse,
    AdminDeleteInstanceRequest,
    AdminDeleteInstanceResponse,
    AdminLibrarianTransferStatusResponse,
    AdminListLibrariansRequest,
    AdminListLibrariansResponse,
    AdminRemoveLibrarianRequest,
    AdminRemoveLibrarianResponse,
    AdminRequestFailedResponse,
    AdminStoreListItem,
    AdminStoreListResponse,
    AdminStoreManifestRequest,
    AdminStoreManifestResponse,
    AdminStoreStateChangeRequest,
    AdminStoreStateChangeResponse,
    LibrarianListResponseItem,
    ManifestEntry,
)
from hera_librarian.transfer import TransferStatus

from ..database import yield_session
from ..logger import log
from ..orm import (
    File,
    Instance,
    Librarian,
    OutgoingTransfer,
    RemoteInstance,
    StoreMetadata,
)
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

    def get_free_space(store: StoreMetadata) -> int:
        "Get the free space for a store."
        try:
            return store.store_manager.free_space
        except FileNotFoundError:
            # Store is actually not available!
            return -1

    return AdminStoreListResponse(
        [
            AdminStoreListItem(
                name=store.name,
                store_type=InvertedStoreNames[store.store_type],
                free_space=get_free_space(store),
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
    instances = session.query(Instance).filter_by(store_id=store.id).all()

    def create_manifest_entry(instance: Instance) -> ManifestEntry:
        "Create a linked transfer and manifest entry based on logic."

        if request.create_outgoing_transfers:
            transfer = OutgoingTransfer.new_transfer(
                destination=outgoing_librarian.name,
                instance=instance,
                file=instance.file,
            )

            # Need to set this as ongoing already for sneakernet transfers.
            transfer.status = TransferStatus.ONGOING

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


@router.post("/librarians/list", response_model=AdminListLibrariansResponse)
def list_librarians(
    request: AdminListLibrariansRequest,
    user: AdminUserDependency,
    response: Response,
    session: Session = Depends(yield_session),
):
    """
    Returns a list of all librarians in the database, and optionally
    tries to ping them to verify that the connection is successful.
    """

    log.debug(f"Recieved list librarians request from {user.username}: {request}.")

    librarians = session.query(Librarian).all()

    responses = []

    for librarian in librarians:
        if request.ping:
            try:
                ping = bool(librarian.client().ping())
            except LibrarianHTTPError:
                log.warning(f"Librarian {librarian.name} did not respond to ping.")
                ping = False

        responses.append(
            LibrarianListResponseItem(
                name=librarian.name,
                url=librarian.url,
                port=librarian.port,
                available=ping if request.ping else None,
                enabled=librarian.transfers_enabled,
            )
        )

    return AdminListLibrariansResponse(librarians=responses)


@router.post(
    "/librarians/add",
    response_model=AdminAddLibrarianResponse | AdminRequestFailedResponse,
)
def add_librarian(
    request: AdminAddLibrarianRequest,
    user: AdminUserDependency,
    response: Response,
    session: Session = Depends(yield_session),
):
    """
    Adds a new librarian to the database. By default, it pings the librarian
    to make sure that the connection works before accepting that connection.
    """

    log.debug(f"Recieved add librarian request from {user.username}.")

    # Check if the librarian already exists.

    existing_librarian = (
        session.query(Librarian).filter_by(name=request.librarian_name).one_or_none()
    )

    if existing_librarian is not None:
        return AdminAddLibrarianResponse(
            success=False,
            already_exists=True,
            ping_success=False,
        )

    try:
        new_librarian = Librarian.new_librarian(
            name=request.librarian_name,
            url=request.url,
            port=request.port,
            authenticator=request.authenticator,
            check_connection=request.check_connection,
        )

        session.add(new_librarian)
        session.commit()

        ping_success = True
        success = True
    except ValueError as e:
        log.error(f"Failed to add librarian {request.librarian_name}.")
        log.error(f"Error: {e}.")
        ping_success = False
        success = False

    return AdminAddLibrarianResponse(
        success=success,
        already_exists=False,
        ping_success=ping_success if request.check_connection else None,
    )


@router.post(
    "/librarians/remove",
    response_model=AdminRemoveLibrarianResponse | AdminRequestFailedResponse,
)
def remove_librarian(
    request: AdminRemoveLibrarianRequest,
    user: AdminUserDependency,
    response: Response,
    session: Session = Depends(yield_session),
):
    """
    Removes a librarian from the database. This will also, optionally,
    remove all outgoing transfers to that librarian.
    """

    log.debug(f"Recieved remove librarian request from {user.username}: {request}.")

    # Check if the librarian exists.
    librarian = (
        session.query(Librarian).filter_by(name=request.librarian_name).one_or_none()
    )

    if librarian is None:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return AdminRequestFailedResponse(
            reason=f"Librarian {request.librarian_name} does not exist.",
            suggested_remedy="You do not need to remove it.",
        )

    # Remove the outgoing transfers if requested.
    number_of_transfers_removed = 0

    if request.remove_outgoing_transfers:
        for transfer in session.query(OutgoingTransfer).filter_by(
            destination=librarian.name
        ):
            transfer.fail_transfer(session=session)

            number_of_transfers_removed += 1

    # Remove the librarian.
    session.delete(librarian)

    session.commit()

    return AdminRemoveLibrarianResponse(
        success=True,
        number_of_transfers_removed=number_of_transfers_removed,
    )


@router.post(path="/instance/delete_remote", response_model=AdminDeleteInstanceResponse)
def delete_remote_instance(
    request: AdminDeleteInstanceRequest,
    user: AdminUserDependency,
    response: Response,
    session: Session = Depends(yield_session),
) -> AdminDeleteInstanceResponse:
    """
    Delete a remote instance.

    Must be an admin to use this endpoint

    Possible responses codes:
    - 201: The instance has been deleted
    - 400: The instance does not exist
    """

    log.info(
        f"Request from {user.username} to delete remote "
        f"instance {request.instance_id}"
    )

    instance = session.get(RemoteInstance, request.instance_id)

    if instance is None:
        log.error(f"Instance does not exist: {request.instance_id}")
        response.status_code = status.HTTP_400_BAD_REQUEST
        return AdminDeleteInstanceResponse(
            success=False, instance_id=request.instance_id
        )

    session.delete(instance)
    session.commit()

    return AdminDeleteInstanceResponse(success=True, instance_id=request.instance_id)


@router.post(path="/instance/delete_local", response_model=AdminDeleteInstanceResponse)
def delete_local_instance(
    request: AdminDeleteInstanceRequest,
    user: AdminUserDependency,
    response: Response,
    session: Session = Depends(yield_session),
) -> AdminDeleteInstanceResponse:
    """
    Delete a local instance.

    Must be an admin to use this endpoint

    Possible responses codes:
    - 201: The instance has been deleted
    - 400: The instance does not exist
    """

    log.info(
        f"Request from {user.username} to delete remote "
        f"instance {request.instance_id}"
    )

    instance = session.get(Instance, request.instance_id)

    if instance is None:
        log.error(f"Instance does not exist: {request.instance_id}")
        response.status_code = status.HTTP_400_BAD_REQUEST
        return AdminDeleteInstanceResponse(
            success=False, instance_id=request.instance_id
        )

    session.delete(instance)
    session.commit()
    # If the file is not associated with anything and marked for deletion
    # then delete it. If there is a local instance or remote instance, leave the file
    instance_file = session.get(File, instance.file_name)

    if not instance_file.instances and request.delete_file:
        store = session.get(StoreMetadata, instance.store_id)
        store.store_manager.delete(Path(instance.path))

    return AdminDeleteInstanceResponse(success=True, instance_id=request.instance_id)


@router.post(
    path="/librarians/transfer_status/change",
    response_model=AdminLibrarianTransferStatusResponse,
)
def change_librarian_transfer_status(
    request: AdminChangeLibrarianTransferStatusRequest,
    user: AdminUserDependency,
    response: Response,
    session: Session = Depends(yield_session),
) -> AdminLibrarianTransferStatusResponse:
    """
    Change the transfer status of a librarian. This will enable or disable
    outbound transfers to the librarian.
    """

    librarian = (
        session.query(Librarian).filter_by(name=request.librarian_name).one_or_none()
    )

    if librarian is None:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return AdminRequestFailedResponse(
            reason=f"Librarian {request.librarian_name} does not exist",
            suggested_remedy="Please verify that the requested librarian exists",
        )

    librarian.transfers_enabled = request.transfers_enabled

    session.commit()

    return AdminLibrarianTransferStatusResponse(
        librarian_name=librarian.name,
        transfers_enabled=librarian.transfers_enabled,
    )
