"""
Pydantic modems for the admin endpoints
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, RootModel

from hera_librarian.deletion import DeletionPolicy


class AdminCreateFileRequest(BaseModel):
    # File properties
    name: str
    "The unique filename of this file."
    create_time: datetime
    "The time at which this file was placed on the stcaore."
    size: int
    "Size in bytes of the file"
    checksum: str
    "Checksum (MD5 hash) of the file."

    uploader: str
    "Uploader of the file."
    source: str
    "Source of the file."

    # Instance properties
    path: str
    "Path to the instance (full) on the store."
    store_name: str
    "The name of the store that this file is on."


class AdminCreateFileResponse(BaseModel):
    already_exists: bool = False
    "In the case that the file already exists, this will be true."

    file_exists: bool = False
    "If the file exists or not."

    success: bool = False
    "Whether we were totally successful."


class AdminRequestFailedResponse(BaseModel):
    reason: str
    "The reason why the search failed."

    suggested_remedy: str
    "A suggested remedy for the failure."


class AdminStoreListItem(BaseModel):
    name: str
    "The name of the store."

    store_type: str
    "The type of the store."

    free_space: int
    "The amount of space available on the store (in bytes)."

    ingestable: bool
    "Whether this store is ingestable or not."

    available: bool
    "Whether this store is available or not."

    enabled: bool
    "Whether this store is enabled or not."


AdminStoreListResponse = RootModel[list[AdminStoreListItem]]


class ManifestEntry(BaseModel):
    name: str
    "The name of the file."
    create_time: datetime
    "The time the file was created."
    size: int
    "The size of the file in bytes."
    checksum: str
    "The checksum of the file."
    uploader: str
    "The uploader of the file."
    source: str
    "The source of the file."

    instance_path: str
    "The path to the instance on the store."
    deletion_policy: DeletionPolicy
    "The deletion policy of the instance."
    instance_create_time: datetime
    "The time the instance was created."
    instance_available: bool
    "Whether the instance is available or not. If not, no outgoing transfer is created."

    outgoing_transfer_id: int
    "The ID of the outgoing transfer, if it exists."


class AdminStoreManifestRequest(BaseModel):
    store_name: str
    "The name of the store to get the manifest for."

    create_outgoing_transfers: bool = False
    "Whether to create outgoing transfers for the files in the manifest."

    destination_librarian: str = ""
    "The name of the librarian to send the files to, if create_outgoing_transfers is true."

    disable_store: bool = False
    "Whether to disable the store after creating the outgoing transfers."

    mark_local_instances_as_unavailable: bool = False
    "Mark the local instances as unavailable after creating the outgoing transfers."


class AdminStoreManifestResponse(BaseModel):
    librarian_name: str
    "The name of the librarian that generated this manifest."

    store_name: str
    "The name of the store."

    store_files: list[ManifestEntry]
    "The files on the store."


class AdminStoreStateChangeRequest(BaseModel):
    store_name: str
    "The name of the store to enable or disable."

    enabled: bool
    "Whether to enable or disable the store. If true, it will be set to enabled, else disabled."


class AdminStoreStateChangeResponse(BaseModel):
    store_name: str
    "The name of the store."

    enabled: bool
    "The current state of the store."

    success: bool
    "Whether your transaction was ultimately successful."


class AdminListLibrariansRequest(BaseModel):
    ping: bool = True
    "Whether to ping the librarians or not."


class LibrarianListResponseItem(BaseModel):
    name: str
    "The name of the librarian."

    url: str
    "The URL of the librarian."

    port: int
    "The port of the librarian."

    available: bool | None
    "Whether the librarian is available or not, only if ping is true."

    enabled: bool
    "Whether transfers the librarian is enabled or not."


class AdminListLibrariansResponse(BaseModel):
    librarians: list[LibrarianListResponseItem]
    "The list of librarians."


class AdminAddLibrarianRequest(BaseModel):
    librarian_name: str
    "The name of the librarian to add."

    url: str
    "The URL of the librarian to add."

    port: int
    "The port of the librarian to add."

    authenticator: str
    "The authenticator for the librarian to add."

    check_connection: bool = True
    "Whether to check the connection to the librarian before adding it. It is strongly recommended that you do this."


class AdminAddLibrarianResponse(BaseModel):
    success: bool
    "Whether the librarian was added successfully."

    already_exists: bool
    "Whether the librarian already exists."

    ping_success: bool | None
    "Whether the ping to the librarian was successful."


class AdminRemoveLibrarianRequest(BaseModel):
    librarian_name: str
    "The name of the librarian to remove."

    remove_outgoing_transfers: bool = False
    "Whether to remove (mark as failed) outgoing transfers to this librarian."


class AdminRemoveLibrarianResponse(BaseModel):
    success: bool
    "Whether it was removed sucessfully."

    number_of_transfers_removed: int
    "The number of transfers removed (marked as failed)."


class AdminDeleteInstanceRequest(BaseModel):
    """
    A request to delete a instance.
    """

    "The instance id of the instance to delete."
    instance_id: int
    "Delete the associated file"
    delete_file: Optional[bool] = False


class AdminDeleteInstanceResponse(BaseModel):
    """
    A response to a user change request.
    """

    "Whether the change was successful."
    success: bool

    "The instance name of the instance that was changed."
    instance_id: int


class AdminChangeLibrarianTransferStatusRequest(BaseModel):
    """
    A request to change the transfer status of a librarian, either
    to enable or disable outbound transfers.
    """

    "The name of the librarian to change the transfer status of."
    librarian_name: str

    "Whether to enable or disable outbound transfers."
    transfers_enabled: bool


class AdminLibrarianTransferStatusResponse(BaseModel):
    """
    A response to a user change request.
    """

    "The name of the librarian that was changed."
    librarian_name: str

    "Whether the librarian has outbound transfers enabled."
    transfers_enabled: bool
