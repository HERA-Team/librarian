"""
Models for uploads.
"""

from pydantic import BaseModel, field_validator, SerializeAsAny

from ..stores import CoreStore
from ..transfers import CoreTransferManager, LocalTransferManager

from pathlib import Path
from typing import Union

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
    staging_name: Path
    # Note that transfer_providers will be tried in  the order specified here. So
    # you should put the one that requires the most arguments _first_ (otherwise everything
    # will come out as a CoreTransferManager!)
    transfer_providers: dict[str, Union[LocalTransferManager, CoreTransferManager]]


class UploadCompletionRequest(BaseModel):
    """
    Model sent from client to server to complete an upload,
    once the client has uploaded the file to the staging area on
    the store.
    """

    store_name: str
    staging_location: Path
    staging_name: Path
    destination_location: Path
    transfer_provider_name: str
    # See note on ordering in UploadInitiationResponse.
    transfer_provider: Union[LocalTransferManager, CoreTransferManager]

    meta_mode: str
    deletion_policy: str
    source_name: str

    null_obsid: bool = False