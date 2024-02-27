"""
Pydantic modems for the admin endpoints
"""

from datetime import datetime

from pydantic import BaseModel, RootModel


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
    deletion_policy: str
    "The deletion policy of the instance."
    instance_create_time: datetime
    "The time the instance was created."
    instance_available: bool


class AdminStoreManifestRequest(BaseModel):
    store_name: str
    "The name of the store to get the manifest for."


class AdminStoreManifestResponse(BaseModel):
    store_name: str
    "The name of the store."

    store_files: list[ManifestEntry]
    "The files on the store."
