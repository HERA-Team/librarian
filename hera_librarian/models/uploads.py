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
    "Size of the upload in bytes."
    upload_checksum: str
    "Checksum of the upload."
    upload_name: Path
    "Name of the upload. You will be furnished a staging location absolute path with this included."
    destination_location: Path
    "The final location of the file on the store. Is usually the same as upload_name, but could include extra paths (e.g. unique_id/{upload_name})"
    uploader: str
    "Name of the uploader (previously source_name)."


class UploadInitiationResponse(BaseModel):
    """
    Model for the response to initiating an upload. Gives the
    client the information it needs to upload the file to a store.
    """

    available_bytes_on_store: int
    "Number of bytes available on the store, for information."
    store_name: str
    "Name of the store that will be used."
    staging_name: Path
    "The name of the staging area. E.g. on a POSIX filesystem this will be the name of the staging directory."
    staging_location: Path
    "Absolute path to the staging location on the store. This includes your upload name."
    upload_name: Path
    "Name of the upload."
    destination_location: Path
    "Where you asked for the file to be uploaded to. Is usually the same as upload_name."
    # Note that transfer_providers will be tried in  the order specified here. So
    # you should put the one that requires the most arguments _first_ (otherwise everything
    # will come out as a CoreTransferManager!)
    transfer_providers: dict[str, Union[LocalTransferManager, CoreTransferManager]]
    "The available transfer providers for the client to communicate with the store."
    transfer_id: int
    "The ID of the transfer. This is used to identify the transfer when completing it."


class UploadCompletionRequest(BaseModel):
    """
    Model sent from client to server to complete an upload,
    once the client has uploaded the file to the staging area on
    the store.
    """

    store_name: str
    "The store that the file was uploaded to."
    staging_name: Path
    "Name of the staging area."
    staging_location: Path
    "Staging location where the file was uploaded."
    upload_name: Path
    "Name of the file in the staging location."
    destination_location: Path
    "Final (relative) location of the file on the store."
    transfer_provider_name: str
    "Name of the transfer provider that was used to upload the file."
    # See note on ordering in UploadInitiationResponse.
    transfer_provider: Union[LocalTransferManager, CoreTransferManager]
    "Transfer provider that was used to upload the file."

    meta_mode: str
    "Metadata extraction mode."
    deletion_policy: str
    "Deletion policy for the uploaded file."
    uploader: str
    "Name of the uploader (previously source_name)."

    null_obsid: bool = False
    "Whether the file has an observation ID."

    transfer_id: int
    "The transfer ID of the transfer that was used to upload the file."