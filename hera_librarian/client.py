"""
The public-facing LibrarianClient object.
"""

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import requests
from pydantic import BaseModel

from .authlevel import AuthLevel
from .deletion import DeletionPolicy
from .errors import ErrorCategory, ErrorSeverity
from .exceptions import LibrarianError, LibrarianHTTPError
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
from .utils import get_md5_from_path, get_size_from_path

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
        return f"{self.host}:{self.port}/api/v2"

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

        if path[0] == "/":
            return f"{self.hostname}{path}"
        else:
            return f"{self.hostname}/{path}"

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

        r = requests.post(
            self.resolve(endpoint),
            data=data,
            headers={"Content-Type": "application/json"},
            auth=(self.user, self.password),
        )

        if str(r.status_code)[0] != "2":
            try:
                json = r.json()
            except requests.exceptions.JSONDecodeError:
                json = {}

            raise LibrarianHTTPError(
                url=endpoint,
                status_code=r.status_code,
                reason=json.get("reason", "<no reason provided>"),
                suggested_remedy=json.get(
                    "suggested_remedy", "<no suggested remedy provided>"
                ),
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
            endpoint="ping/logged" if require_login else "ping",
            request=PingRequest(),
            response=PingResponse,
        )

        return response

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
                upload_checksum=get_md5_from_path(local_path),
                upload_name=dest_path.name,
                destination_location=dest_path,
                uploader=self.user,
            ),
            response=UploadInitiationResponse,
        )

        transfer_managers = response.transfer_providers

        # Now try all the transfer managers. If they're valid, we try to use them.
        # If they fail, we should probably catch the exception.
        # TODO: Catch the exception on failure.
        used_transfer_manager: Optional["CoreTransferManager"] = None
        used_transfer_manager_name: Optional[str] = None

        # TODO: Should probably have some manual ordering here.
        for name, transfer_manager in transfer_managers.items():
            if transfer_manager.valid:
                transfer_manager.transfer(
                    local_path=local_path, remote_path=response.staging_location
                )

                # We used this.
                used_transfer_manager = transfer_manager
                used_transfer_manager_name = name

                break
            else:
                print(f"Warning: transfer manager {name} is not valid.")

        if used_transfer_manager is None:
            raise LibrarianError("No valid transfer managers found.")

        # If we made it here, the file is successfully on the store!
        request = UploadCompletionRequest(
            store_name=response.store_name,
            staging_name=response.staging_name,
            staging_location=response.staging_location,
            upload_name=response.upload_name,
            destination_location=dest_path,
            transfer_provider_name=used_transfer_manager_name,
            transfer_provider=used_transfer_manager,
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
