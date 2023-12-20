"""
Models for cloning to a remote librarian.
"""

from pathlib import Path
from typing import Union
from pydantic import BaseModel, Field

from hera_librarian.transfers.local import LocalTransferManager, CoreTransferManager



class CloneInitiationRequest(BaseModel):
    """
    In a librarian A -> librarian B transfer, this is the request
    that librarian A sends to librarian B to request that librarian
    B stage a transfer.
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

    file_id: int
    "Local file ID (on librarian A)"
    source_store_name: str
    "Name of the store that the file is on (on librarian A)"
    source_store_id: int
    "Local store ID (on librarian A)"

    pass


class CloneInitiationResponse(BaseModel):
    """
    In a librarian A -> librarian B transfer, this is the response
    that librarian B sends to librarian A to indicate that it has
    created a space to stage the transfer.
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
    pass


class CloneCompleteRequest(BaseModel):
    """
    In a librarian A -> librarian B transfer, this is the request
    that librarian B sends to librarian A to indicate that it has
    completed the transfer, and that it is ok to set the transfer
    status to completed.
    """
    pass

class CloneFailedResponse(BaseModel):
    """
    Model for response when the clone failed.
    """

    reason: str
    "Reason for failure."
    suggested_remedy: str = "Please try again later."

    pass