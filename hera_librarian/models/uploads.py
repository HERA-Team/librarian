"""
Models for uploads.
"""

from pydantic import BaseModel

from ..stores import CoreStore
from ..transfers import CoreTransferManager

from pathlib import Path

class UploadInitiationRequest(BaseModel):
    """
    Model sent from client to server to initiate an upload.
    """

    upload_size: int


class UploadInitiationResponse(BaseModel):
    """
    Model for the response to initiating an upload. Gives the
    client the information it needs to upload the file to a store.
    """

    available_bytes_on_store: int
    store_name: str
    staging_location: Path
    transfer_providers: dict[str, CoreTransferManager]


class UploadCompletionRequest(BaseModel):
    """
    Model sent from client to server to complete an upload,
    once the client has uploaded the file to the staging area on
    the store.
    """

    store_name: str
    staging_location: Path
    destination_location: Path
    transfer_provider_name: str
    transfer_provider: CoreTransferManager

    meta_mode: str
    deletion_policy: str
    source_name: str

    null_obsid: bool = False