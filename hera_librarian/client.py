"""
The public-facing LibrarianClient object.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Optional
from urllib.parse import urlparse

import requests
from pydantic import BaseModel

from hera_librarian.models.clone import (
    CloneCompleteRequest,
    CloneCompleteResponse,
    CloneInitiationRequest,
    CloneInitiationResponse,
    CloneOngoingRequest,
    CloneOngoingResponse,
    CloneStagedRequest,
    CloneStagedResponse,
)

from .authlevel import AuthLevel
from .deletion import DeletionPolicy
from .errors import ErrorCategory, ErrorSeverity
from .exceptions import LibrarianError, LibrarianHTTPError, LibrarianTimeoutError
from .models.admin import (
    AdminAddLibrarianRequest,
    AdminAddLibrarianResponse,
    AdminCreateFileRequest,
    AdminCreateFileResponse,
    AdminDeleteInstanceRequest,
    AdminDeleteInstanceResponse,
    AdminListLibrariansRequest,
    AdminListLibrariansResponse,
    AdminRemoveLibrarianRequest,
    AdminRemoveLibrarianResponse,
    AdminStoreListItem,
    AdminStoreListResponse,
    AdminStoreManifestRequest,
    AdminStoreManifestResponse,
    AdminStoreStateChangeRequest,
    AdminStoreStateChangeResponse,
)
from .models.errors import (
    ErrorClearRequest,
    ErrorClearResponse,
    ErrorSearchFailedResponse,
    ErrorSearchRequest,
    ErrorSearchResponse,
    ErrorSearchResponses,
)
from .models.ping import PingRequest, PingResponse
from .models.search import FileSearchRequest, FileSearchResponse, FileSearchResponses
from .models.uploads import (
    UploadCompletionRequest,
    UploadInitiationRequest,
    UploadInitiationResponse,
)
from .models.users import (
    UserAdministrationChangeResponse,
    UserAdministrationCreationRequest,
    UserAdministrationDeleteRequest,
    UserAdministrationGetRequest,
    UserAdministrationGetResponse,
    UserAdministrationPasswordChange,
    UserAdministrationUpdateRequest,
)
from .settings import ClientInfo
from .utils import (
    get_checksum_from_path,
    get_hash_function_from_hash,
    get_size_from_path,
)

if TYPE_CHECKING:
    from .transfers import CoreTransferManager


class LibrarianClient:
    """
    A client for the Librarian API.
    """

    host: str
    port: int
    user: str
    password: str

    def __init__(self, host: str, port: int, user: str, password: str):
        """
        Create a new LibrarianClient.

        Parameters
        ----------
        host : str
            The hostname of the Librarian server.
        port : int
            The port of the Librarian server.
        user : str
            The name of the user.
        password : str
            The password of the user.
        """

        if host[-1] == "/":
            self.host = host[:-1]
        else:
            self.host = host

        self.port = port
        self.user = user
        self.password = password

    def __repr__(self):
        return f"Librarian Client ({self.user}) for {self.host}:{self.port}"

    @classmethod
    def from_info(cls, client_info: ClientInfo):
        """
        Create a LibrarianClient from a ClientInfo object.

        Parameters
        ----------
        client_info : ClientInfo
            The ClientInfo object.

        Returns
        -------
        LibrarianClient
            The LibrarianClient.
        """

        return cls(
            host=client_info.host,
            port=client_info.port,
            user=client_info.user,
            password=client_info.password,
        )

    @property
    def hostname(self):
        # Grab the url with /api/v2 appended.
        parsed = urlparse(url=self.host)

        if parsed.port is not None:
            raise LibrarianHTTPError(
                url=self.host,
                status_code=None,
                reason="Host should not include port.",
                suggested_remedy="Use the `port` parameter.",
            )

        parsed = parsed._replace(
            netloc=f"{parsed.netloc}:{self.port}", path=f"{parsed.path}/api/v2"
        )

        return parsed.geturl()

    def resolve(self, path: str):
        """
        Resolve a path to a URL.

        Parameters
        ----------
        path : str
            The path to resolve.

        Returns
        -------
        str
            The resolved URL.
        """

        parsed = urlparse(url=self.hostname)

        parsed = parsed._replace(path=f"{parsed.path}/{path}")

        return parsed.geturl()

    def post(
        self,
        endpoint: str,
        request: Optional[BaseModel] = None,
        response: Optional[BaseModel] = None,
    ) -> Optional[BaseModel]:
        """
        Do a POST operation, passing a JSON version of the request and expecting a
        JSON reply; return the decoded version of the latter.

        Parameters
        ----------
        endpoint : str
            The endpoint to post to.
        request : pydantic.BaseModel, optional
            The request model to send. If None, we don't ask for anything.
        response : pydantic.BaseModel, optional
            The response model to expect. If None, we don't return anything.

        Returns
        -------
        response, optional
            The decoded response model, or None.

        Raises
        ------

        LibrarianHTTPError
            If the HTTP request fails.

        pydantic.ValidationError
            If the remote librarian returns an invalid response.
        """

        data = None if request is None else request.model_dump_json()

        try:
            r = requests.post(
                self.resolve(endpoint),
                data=data,
                headers={"Content-Type": "application/json"},
                auth=(self.user, self.password),
            )
        except (TimeoutError, requests.exceptions.ConnectionError):
            raise LibrarianTimeoutError(url=self.resolve(endpoint))

        if str(r.status_code)[0] != "2":
            try:
                response_json = r.json()
            except requests.exceptions.JSONDecodeError:
                response_json = {}

            # HTTPException
            if "detail" in response_json:
                try:
                    response_json = json.loads(response_json["detail"])
                except json.JSONDecodeError:
                    response_json = {}

            raise LibrarianHTTPError(
                url=endpoint,
                status_code=r.status_code,
                reason=response_json.get("reason", "<no reason provided>"),
                suggested_remedy=response_json.get(
                    "suggested_remedy", "<no suggested remedy provided>"
                ),
                full_response=response_json,
            )

        if response is None:
            return None
        else:
            # Note that the pydantic model wants the full bytes content
            # not the deserialized r.json()
            return response.model_validate_json(r.content)

    def ping(self, require_login: bool = False) -> PingResponse:
        """
        Ping the remote librarian to see if it exists.

        Arguments
        ---------

        require_login : bool, optional
            If True, we require the user to be logged in. If False, we don't.

        Returns
        -------

        PingResponse
            The response from the remote librarian.

        Raises
        ------

        LibrarianHTTPError
            If the remote librarian is unreachable.

        pydantic.ValidationError
            If the remote librarian returns an invalid response.
        """

        response: PingResponse = self.post(
            endpoint="ping/logged" if require_login else "ping/",
            request=PingRequest(),
            response=PingResponse,
        )

        return response

    def _copy_file(
        self,
        transfer_managers: dict[str, "CoreTransferManager"],
        local_path: Path,
        remote_path: Path,
    ) -> str:
        """
        Copy a file to the librarian. Used by the underlying upload and
        cloning functions.

        Parameters
        ----------
        transfer_managers : dict[str, CoreTransferManager]
            The transfer managers to use.
        local_path : Path
            The path of the local file.
        remote_path : Path
            The path of the remote file.

        Returns
        -------
        str
            The name of the transfer manager that was used.

        Raises
        ------
        LibrarianError
            If no valid transfer managers are found.
        """

        # Now try all the transfer managers. If they're valid, we try to use them.
        # If they fail, we should probably catch the exception.
        # TODO: Catch the exception on failure.
        used_transfer_manager_name: Optional[str] = None

        # TODO: Should probably have some manual ordering here.
        for name, transfer_manager in transfer_managers.items():
            if transfer_manager.valid:
                try:
                    transfer_manager.transfer(
                        local_path=local_path, remote_path=remote_path
                    )

                    # We used this.
                    used_transfer_manager_name = name
                    break
                except PermissionError:
                    raise LibrarianError(f"Could not set permissions on {remote_path}")
            else:
                print(f"Warning: transfer manager {name} is not valid.")

        if used_transfer_manager_name is None:
            raise LibrarianError("No valid transfer managers found.")

        return used_transfer_manager_name

    def upload(
        self,
        local_path: Path,
        dest_path: Path,
        deletion_policy: DeletionPolicy | str = DeletionPolicy.DISALLOWED,
    ):
        """
        Upload a file or directory to the librarian.

        Parameters
        ----------
        local_path : Path
            Path of the file or directory to upload.
        dest_path : Path
            The destination 'path' on the librarian store (often the same as your
            filename, but may be under some root directory).
        deletion_policy : DeletionPolicy | str, optional
            Whether or not this file may be deleted, by default
            DeletionPolicy.DISALLOWED

        Returns
        -------
        dict
            _description_

        Raises
        ------
        ValueError
            If the provided path is incorrect.
        LibrarianError:
            If the remote librarian cannot be transferred to.
        """

        if isinstance(deletion_policy, str):
            deletion_policy = DeletionPolicy.from_str(deletion_policy)

        if dest_path.is_absolute():
            raise ValueError(f"Destination path may not be absolute; got {dest_path}")

        # Ask the librarian for a staging directory, and a list of transfer managers
        # to try.

        response: UploadInitiationResponse = self.post(
            endpoint="upload/stage",
            request=UploadInitiationRequest(
                upload_size=get_size_from_path(local_path),
                upload_checksum=get_checksum_from_path(local_path),
                upload_name=dest_path.name,
                destination_location=dest_path,
                uploader=self.user,
            ),
            response=UploadInitiationResponse,
        )

        transfer_managers = response.transfer_providers

        used_transfer_manager_name = self._copy_file(
            transfer_managers=transfer_managers,
            local_path=local_path,
            remote_path=response.staging_location,
        )

        # If we made it here, the file is successfully on the store!
        request = UploadCompletionRequest(
            store_name=response.store_name,
            staging_name=response.staging_name,
            staging_location=response.staging_location,
            upload_name=response.upload_name,
            destination_location=dest_path,
            transfer_provider_name=used_transfer_manager_name,
            transfer_provider=transfer_managers[used_transfer_manager_name],
            # Note: meta_mode is used in current status
            meta_mode="infer",
            deletion_policy=deletion_policy,
            source_name=self.user,
            # Note: we ALWAYS use null_obsid
            null_obsid=True,
            uploader=self.user,
            transfer_id=response.transfer_id,
        )

        self.post(
            endpoint="upload/commit",
            request=request,
        )

        return

    def search_files(
        self,
        name: Optional[str] = None,
        create_time_window: Optional[tuple[datetime, ...]] = None,
        uploader: Optional[str] = None,
        source: Optional[str] = None,
        max_results: int = 64,
    ) -> list[FileSearchResponse]:
        """
        Search for files on this librarain.

        Parameters
        ----------
        name : Optional[str], optional
            The name o files to search for, by default None
        create_time_window : Optional[tuple[datetime, ...]], optional
            A time window to search files within (make sure these are UTC
            times), by default None
        uploader : Optional[str], optional
            The person who uploaded this file, by default None
        source : Optional[str], optional
            The source of this file, could be another librarian, by default None
        max_results : int, optional
            The maximal number of results., by default 64. Note that this can be
            lower as it is also set by the server.

        Returns
        -------
        list[FileSearchResponse]
            A list of files that match the query.
        """

        try:
            response: FileSearchResponses = self.post(
                endpoint="search/file",
                request=FileSearchRequest(
                    name=name,
                    create_time_window=create_time_window,
                    uploader=uploader,
                    source=source,
                    max_results=max_results,
                ),
                response=FileSearchResponses,
            )
        except LibrarianHTTPError as e:
            if e.status_code == 404 and e.reason == "No files found.":
                return []
            else:
                raise e

        return response.root

    def change_password(self, current_password: str, new_password: str):
        """
        Change the password of the user.

        Parameters
        ----------
        current_password: str
            The current password.
        new_password : str
            The new password.
        """

        if current_password != self.password:
            raise ValueError("The current password is incorrect.")

        response = self.post(
            endpoint="users/password_update",
            request=UserAdministrationPasswordChange(
                password=current_password, new_password=new_password
            ),
            response=UserAdministrationChangeResponse,
        )

        return response.success


class AdminClient(LibrarianClient):
    """
    A client for the Librarian API with admin privileges.
    """

    def __init__(self, host: str, port: int, user: str, password: str):
        """
        Create a new AdminClient.

        Parameters
        ----------
        host : str
            The hostname of the Librarian server.
        port : int
            The port of the Librarian server.
        user : str
            The name of the user.
        password : str
            The password of the user.
        """

        super().__init__(host, port, user, password)

    def __repr__(self):
        return f"Admin Client ({self.user}) for {self.host}:{self.port}"

    def search_errors(
        self,
        id: Optional[int] = None,
        category: Optional[ErrorCategory] = None,
        severity: Optional[ErrorSeverity] = None,
        create_time_window: Optional[tuple[datetime, ...]] = None,
        include_resolved: bool = False,
        max_results: int = 64,
    ) -> list[ErrorSearchResponse]:
        """
        Search for files on this librarain.

        Parameters
        ----------
        id : Optional[int], optional
            The ID of the error to search for. If left empty, all errors will be
            returned., by default None
        category : Optional[ErrorCategory], optional
            The category of errors to return. If left empty, all errors will be
            returned., by default None
        severity : Optional[ErrorSeverity], optional
            The severity of errors to return. If left empty, all errors will be
            returned., by default None
        create_time_window : Optional[tuple[datetime, ...]], optional
            The time window to search for files in. This is a tuple of two
            datetimes, the first being the start and the second being the end.
            Note that the datetimes should be in UTC., by default None
        include_resolved : bool, optional
            Whether or not to include resolved errors in the response. By
            default, we do not., by default False
        max_results : int, optional
            The number of errors to return., by default 64

        Returns
        -------
        list[ErrorSearchResponse]
            A list of errors that match the query.
        """

        try:
            response: ErrorSearchResponses = self.post(
                endpoint="search/error",
                request=ErrorSearchRequest(
                    id=id,
                    category=category,
                    severity=severity,
                    create_time_window=create_time_window,
                    include_resolved=include_resolved,
                    max_results=max_results,
                ),
                response=ErrorSearchResponses,
            )
        except LibrarianHTTPError as e:
            if e.status_code == 404 and e.reason == "No errors found.":
                return []
            else:  # pragma: no cover
                raise e

        return response.root

    def clear_error(
        self,
        id: int,
    ):
        """
        Clear an error on this librarain.

        Parameters
        ----------
        id : int
            The ID of the error to clear.

        Raises
        ------

        ValueError
            If the provided error ID is not found, or if the error has already been cleared.
        """

        try:
            self.post(
                endpoint="error/clear",
                request=ErrorClearRequest(id=id),
                response=ErrorClearResponse,
            )
        except LibrarianHTTPError as e:
            if e.status_code == 404 and "No error found with ID" in e.reason:
                raise ValueError(e.reason)
            elif e.status_code == 400 and "Error with ID" in e.reason:
                raise ValueError(e.reason)
            else:  # pragma: no cover
                raise e

    def create_user(
        self,
        username: str,
        password: str,
        auth_level: AuthLevel,
    ):
        """
        Create a user on this librarian.

        Parameters
        ----------
        username : str
            The username of the new user.
        password : str
            The password of the new user.
        auth_level : AuthLevel
            The authentication level of the new user.
        """

        try:
            self.post(
                endpoint="users/create",
                request=UserAdministrationCreationRequest(
                    username=username,
                    password=password,
                    permission=auth_level,
                ),
                response=UserAdministrationChangeResponse,
            )
        except LibrarianHTTPError as e:
            if e.status_code == 400 and "User already exists" in e.reason:
                raise ValueError(e.reason)
            else:  # pragma: no cover
                raise e

    def delete_user(
        self,
        username: str,
    ):
        """
        Delete a user on this librarian.

        Parameters
        ----------
        username : str
            The username of the user to delete.
        """

        try:
            self.post(
                endpoint="users/delete",
                request=UserAdministrationDeleteRequest(
                    username=username,
                ),
                response=UserAdministrationChangeResponse,
            )
        except LibrarianHTTPError as e:
            if e.status_code == 400 and "User not found" in e.reason:
                raise ValueError(e.reason)
            else:  # pragma: no cover
                raise e

    def update_user(
        self,
        username: str,
        new_password: Optional[str] = None,
        auth_level: Optional[AuthLevel] = None,
    ):
        """
        Update a user on this librarian.

        Parameters
        ----------
        username : str
            The username of the user to update.
        new_password : str, optional
            The new password of the user.
        auth_level : AuthLevel, optional
            The new authentication level of the user.
        """

        try:
            self.post(
                endpoint="users/update",
                request=UserAdministrationUpdateRequest(
                    username=username,
                    password=new_password,
                    permission=auth_level,
                ),
                response=UserAdministrationChangeResponse,
            )
        except LibrarianHTTPError as e:
            if e.status_code == 400 and "User not found" in e.reason:
                raise ValueError(e.reason)
            else:  # pragma: no cover
                raise e

    def get_user(self, username: str) -> UserAdministrationGetResponse:
        """
        Get a user on this librarian.

        Parameters
        ----------
        username : str
            The username of the user to get.

        Returns
        -------
        UserAdministrationGetResponse
            The user.
        """

        try:
            response: UserAdministrationGetResponse = self.post(
                endpoint="users/get",
                request=UserAdministrationGetRequest(username=username),
                response=UserAdministrationGetResponse,
            )

            return response
        except LibrarianHTTPError as e:
            if e.status_code == 400 and "User not found" in e.reason:
                raise ValueError(e.reason)
            else:  # pragma: no cover
                raise e

    def add_file_row(
        self,
        name: str,
        create_time: datetime,
        size: int,
        checksum: str,
        uploader: str,
        path: str,
        store_name: str,
    ):
        """
        Add a file row for an already existing file on the store.
        This is useful in the case that you need to re-build the
        librarian database in place. This is inherrently a lossy process.

        Parameters
        ----------
        name : str
            The unique filename of this file.
        create_time : datetime
            The time at which this file was placed on the store.
        size : int
            Size in bytes of the file
        checksum : str
            Checksum (MD5 hash) of the file.
        uploader : str
            Uploader of the file.
        path : str
            Path to the instance (full) on the store.
        store_name : str
            The name of the store that this file is on.

        Returns
        -------
        AdminCreateFileResponse
            The response from the server.

        Raises
        ------
        LibrarianError
            If the file already exists on the store.
        """

        try:
            response: AdminCreateFileResponse = self.post(
                endpoint="admin/add_file",
                request=AdminCreateFileRequest(
                    name=name,
                    create_time=create_time,
                    size=size,
                    checksum=checksum,
                    uploader=uploader,
                    source=self.user,
                    path=path,
                    store_name=store_name,
                ),
                response=AdminCreateFileResponse,
            )
        except LibrarianHTTPError as e:
            if e.status_code == 400 and "Store" in e.reason:
                raise LibrarianError(e.reason)
            if e.status_code == 400 and "File" in e.reason:
                raise LibrarianError(e.reason)
            else:
                raise LibrarianError(f"Unknown error. {e}")

        return response

    def delete_instance(
        self, instance_id: str, instance_type: Literal["local", "remote"] = "local"
    ) -> AdminDeleteInstanceResponse:
        """
        Deletes an instance.

        Parameters
        ----------
        instance_id : str
            The unique instance identifier of this instance
        instance_type : str
            The type of the instance to delete. Accepted values are local and
            remote. Default is local.
        """
        if instance_type == "local":
            endpoint = "admin/instance/delete_local"
        elif instance_type == "remote":
            endpoint = "admin/instance/delete_remote"
        else:
            raise LibrarianError(
                f"Instance type {instance_type} not supported."
                "Please choose either 'local' or 'remote'."
            )

        try:
            response: AdminDeleteInstanceResponse = self.post(
                endpoint=endpoint,
                request=AdminDeleteInstanceRequest(instance_id=instance_id),
                response=AdminDeleteInstanceResponse,
            )
        except LibrarianHTTPError as e:
            if e.status_code == 400 and "Instance does not exist" in e.reason:
                raise LibrarianError(e.reason)
            else:
                raise LibrarianError(f"Unknown error. {e}")

        return response

    def get_store_list(
        self,
    ) -> list[AdminStoreListItem]:
        """
        Get the list of stores on this librarian.

        Returns
        -------
        list[AdminStoreListResponse]
            The list of stores.
        """

        response: AdminStoreListResponse = self.post(
            endpoint="admin/stores/list",
            response=AdminStoreListResponse,
        )

        return response.root

    def set_store_state(
        self,
        store_name: str,
        enabled: bool,
    ) -> bool:
        """
        Sets the enabled (or disabled) state of a store on this librarian.

        Parameters
        ----------
        store_name : str
            The name of the store to change the state of.
        enabled : bool
            The new state of the store.

        Returns
        -------
        bool
            The new (confirmed) state of the store.

        Raises
        ------
        LibrarianError
            If the store does not exist.
        """

        try:
            response: AdminStoreStateChangeResponse = self.post(
                endpoint="admin/stores/state_change",
                request=AdminStoreStateChangeRequest(
                    store_name=store_name,
                    enabled=enabled,
                ),
                response=AdminStoreStateChangeResponse,
            )
        except LibrarianHTTPError as e:
            if e.status_code == 400 and "Store" in e.reason:
                raise LibrarianError(e.reason)
            else:
                raise e

        return response.enabled

    def get_store_manifest(
        self,
        store_name: str,
        create_outgoing_transfers: bool = False,
        destination_librarian: str | None = None,
        disable_store: bool = False,
        mark_local_instances_as_unavailable: bool = False,
    ) -> AdminStoreManifestResponse:
        """
        Get the manifest of a store on this librarian.

        Parameters
        ----------
        store_name : str
            The name of the store to get the manifest for.
        create_outgoing_transfers : bool, optional
            Whether to create outgoing transfers for the files in the
            manifest, by default False
        destination_librarian : str, optional
            The name of the librarian to send the files to, if
            create_outgoing_transfers is true, by default None
        disable_store : bool, optional
            Whether to disable the store after creating the outgoing
            transfers, by default False
        mark_local_instances_as_unavailable : bool, optional
            Mark the local instances as unavailable after creating the
            outgoing transfers, by default False

        Returns
        -------
        AdminStoreManifestResponse
            The manifest of the store.
        """

        try:
            response: AdminStoreManifestResponse = self.post(
                endpoint="admin/stores/manifest",
                request=AdminStoreManifestRequest(
                    store_name=store_name,
                    create_outgoing_transfers=create_outgoing_transfers,
                    destination_librarian=(
                        destination_librarian
                        if destination_librarian is not None
                        else ""
                    ),
                    disable_store=disable_store,
                    mark_local_instances_as_unavailable=mark_local_instances_as_unavailable,
                ),
                response=AdminStoreManifestResponse,
            )
        except LibrarianHTTPError as e:
            if e.status_code == 400 and "Store" in e.reason:
                raise LibrarianError(e.reason)
            else:
                raise e

        return response

    def ingest_manifest_entry(
        self,
        name: Path,
        create_time: datetime,
        size: int,
        checksum: str,
        uploader: str,
        source: str,
        deletion_policy: DeletionPolicy,
        source_transfer_id: int,
        local_path: Path,
    ):
        """
        Ingest a manifest entry into the librarian. This is used for
        sneakernet transfers, and aims to be a lossless process. You should
        use the same user as the original librarian for this to be
        entirely seamless.

        Parameters
        ----------

        name : Path
            The name of the file.
        create_time : datetime
            The time the file was created.
        size : int
            The size of the file in bytes.
        checksum : str
            The checksum of the file.
        uploader : str
            The uploader of the file.
        source : str
            The source of the file.
        deletion_policy : DeletionPolicy
            The deletion policy of the instance.
        source_transfer_id : int
            The ID of the outgoing transfer.
        local_path : Path
            The path to the instance on the store.

        """

        # TODO: Use the batch clone endpoints now to perform this ingestion
        # as we can dramatically reduce the number of requests.

        # We will use the clone endpoints on the server for this process, as
        # it is effectively a self-managed clone.

        initiation_request = CloneInitiationRequest(
            upload_size=size,
            upload_checksum=checksum,
            upload_name=name.name,
            destination_location=name,
            # Uploader is SPECIFICALLY kept as a param here because it is the
            # source librarian, not us doing the ingestion.
            uploader=uploader,
            source=source,
            source_transfer_id=source_transfer_id,
        )

        try:
            initiaton_response: CloneInitiationResponse = self.post(
                endpoint="clone/stage",
                request=initiation_request,
                response=CloneInitiationResponse,
            )
        except LibrarianHTTPError as e:
            if e.status_code == 409 and "already exists on librarian" in e.reason:
                # This is ok, but that person needs to know so they can callback
                # to the source librarian.
                raise LibrarianError(e.reason)
            else:
                raise e

        # Now set as ongoing...

        ongoing_request = CloneOngoingRequest(
            source_transfer_id=initiaton_response.source_transfer_id,
            destination_transfer_id=initiaton_response.destination_transfer_id,
        )

        try:
            ongoing_response: CloneOngoingResponse = self.post(
                endpoint="clone/ongoing",
                request=ongoing_request,
                response=CloneOngoingResponse,
            )
        except LibrarianHTTPError as e:
            if e.status_code == 400 and "Transfer" in e.reason:
                raise LibrarianError(e.reason)
            else:
                raise e

        transfer_managers = initiaton_response.transfer_providers

        used_transfer_manager_name = self._copy_file(
            transfer_managers=transfer_managers,
            local_path=local_path,
            remote_path=initiaton_response.staging_location,
        )

        # Because this is a syncronous transfer from now on, we need to set
        # the status as "staged" on the server so that it can be found by the
        # recv_clone task.

        try:
            staged_request = CloneStagedRequest(
                source_transfer_id=initiaton_response.source_transfer_id,
                destination_transfer_id=initiaton_response.destination_transfer_id,
            )
        except LibrarianHTTPError as e:
            if e.status_code == 400 and "Transfer" in e.reason:
                raise LibrarianError(e.reason)
            else:
                raise e

        staged_response = self.post(
            endpoint="clone/staged",
            request=staged_request,
            response=CloneStagedResponse,
        )

        return

    def complete_outgoing_transfer(
        self,
        outgoing_transfer_id: int,
        store_id: int,
    ) -> bool:
        """
        Complete a transfer on this librarian.

        Parameters
        ----------
        outgoing_transfer_id : int
            The ID of the outgoing transfer to complete.
        store_id: int
            The ID of the store that the transfer ended up on.

        Returns
        -------
        bool
            Whether or not the transfer was completed.
        """

        try:
            response = self.post(
                endpoint="clone/complete",
                request=CloneCompleteRequest(
                    source_transfer_id=outgoing_transfer_id,
                    destination_transfer_id=-1,
                    store_id=store_id,
                ),
                response=CloneCompleteResponse,
            )
        except LibrarianHTTPError as e:
            if e.status_code == 400 and "Transfer" in e.reason:
                raise LibrarianError(e.reason)
            else:
                raise e

        return True

    def get_librarian_list(self, ping: bool = False) -> AdminListLibrariansResponse:
        """
        Get a list of librarians on this librarian.

        Parameters
        ----------
        ping : bool, optional
            Whether to ping the librarians before returning them, by default False

        Returns
        -------
        AdminListLibrariansResponse
            The list of librarians is available under .librarians.

        Raises
        ------

        LibrarianError
            If the user is not an admin or there is some other issue
            communicating.
        """

        try:
            response = self.post(
                endpoint="admin/librarians/list",
                request=AdminListLibrariansRequest(ping=ping),
                response=AdminListLibrariansResponse,
            )
        except LibrarianHTTPError as e:
            if e.status_code == 400 and "User" in e.reason:
                raise LibrarianError(e.reason)
            else:
                raise e

        return response

    def add_librarian(
        self,
        name: str,
        url: str,
        port: int,
        authenticator: str,
        check_connection: bool = True,
    ) -> bool:
        """
        Add a remote librarian to this librarian.

        Parameters
        ----------
        name : str
            The name of the librarian to add.
        url : str
            The URL of the librarian to add.
        port : int
            The port of the librarian to add.
        authenticator : str
            The authenticator for the librarian to add.
        check_connection : bool, optional
            Whether to check the connection to this librarian before
            returning it, by default True

        Returns
        -------
        bool
            Whether the librarian was successfully added.

        Raises
        ------
        LibrarianError
            If the librarian already exists on this librarian.
        """

        try:
            response = self.post(
                endpoint="admin/librarians/add",
                request=AdminAddLibrarianRequest(
                    librarian_name=name,
                    url=url,
                    port=port,
                    authenticator=authenticator,
                    check_connection=check_connection,
                ),
                response=AdminAddLibrarianResponse,
            )
        except LibrarianHTTPError as e:
            if e.status_code == 400 and "Librarian" in e.reason:
                raise LibrarianError(e.reason)
            else:
                raise e

        return response.success

    def remove_librarian(
        self, name: str, remove_outgoing_transfers: bool = False
    ) -> tuple[bool, int]:
        """
        Remove a remote librarian from this librarian.

        Parameters
        ----------
        name : str
            The name of the librarian to remove.
        remove_outgoing_transfers : bool, optional
            Whether to remove (mark as failed) outgoing transfers to this
            librarian, by default False

        Returns
        -------
        tuple[bool, int]
            The first element is whether the librarian was successfully
            removed, and the second is the number of transfers removed.

        Raises
        ------
        LibrarianError
            If the librarian does not exist on this librarian.
        """

        try:
            response = self.post(
                endpoint="admin/librarians/remove",
                request=AdminRemoveLibrarianRequest(
                    librarian_name=name,
                    remove_outgoing_transfers=remove_outgoing_transfers,
                ),
                response=AdminRemoveLibrarianResponse,
            )
        except LibrarianHTTPError as e:
            if (
                e.status_code == 400
                and "Librarian" in e.reason
                and "does not exist" in e.reason
            ):
                raise LibrarianError(e.reason)
            else:
                raise e

        return response.success, response.number_of_transfers_removed
