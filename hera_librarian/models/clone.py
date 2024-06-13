"""
Models for cloning to a remote librarian.
"""

from pathlib import Path
from typing import Union

from pydantic import BaseModel, Field

from hera_librarian.async_transfers import (
    GlobusAsyncTransferManager,
    LocalAsyncTransferManager,
    RsyncAsyncTransferManager,
)
from hera_librarian.transfers import LocalTransferManager


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
    "Name of the uploader (previously source_name). The person who initially uploaded this to any store."
    source: str
    "The librarian or person who is sending you this specific instance. For clones this is the source librarian name."

    source_transfer_id: int
    "The ID of the transfer. Note that this is the OutgoingTransfer ID."

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
    transfer_providers: dict[str, Union[LocalTransferManager]]
    "The available synchronous transfer providers for the client to communicate with the store."

    source_transfer_id: int
    "OutgoingTransfer ID"
    destination_transfer_id: int
    "IncomingTransfer ID"


class CloneBatchInitiationRequestFileItem(BaseModel):
    """
    An individual clone item for the batched clone initiation request.
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
    "Name of the uploader (previously source_name). The person who initially uploaded this to any store."

    source_transfer_id: int
    "The ID of the transfer. Note that this is the OutgoingTransfer ID."


class CloneBatchInitiationRequest(BaseModel):
    """
    Similar to CloneInitationRequest, but for asynchronous (and hence batched)
    transfers. The transfer_providers here are all asynchronous.
    """

    uploads: list[CloneBatchInitiationRequestFileItem]
    "The list of files to be uploaded."
    source: str
    "The librarian or person who is sending you this specific instance. For clones this is the source librarian name."
    total_size: int
    "Total number of bytes for the entire upload."


class CloneBatchInitiationResponseFileItem(BaseModel):
    """
    An individual response item for each file in a batched upload.
    """

    staging_name: Path
    "The name of the staging area. E.g. on a POSIX filesystem this will be the name of the staging directory."
    staging_location: Path
    "Absolute path to the staging location on the store. This includes your upload name."
    upload_name: Path
    "Name of the upload."
    destination_location: Path
    "Where you asked for the file to be uploaded to. Is usually the same as upload_name."

    source_transfer_id: int
    "OutgoingTransfer ID"
    destination_transfer_id: int
    "IncomingTransfer ID"


class CloneBatchInitiationResponse(BaseModel):
    """
    The batched response for a clone initiation.
    """

    available_bytes_on_store: int
    "Number of bytes available on the store, for information."
    store_name: str
    "Name of the store that will be used."
    uploads: list[CloneBatchInitiationResponseFileItem]
    "Each individual upload is tagged with its own {Outgoing,Incoming}Transfer ID."
    async_transfer_providers: dict[
        str,
        Union[
            GlobusAsyncTransferManager,
            RsyncAsyncTransferManager,
            LocalAsyncTransferManager,
        ],
    ]
    "The transfer managers/providers for the batch of uploads. They should all use this same transfer manager."


class CloneOngoingRequest(BaseModel):
    """
    In a librarian A -> librarian B transfer, this is the request
    from librarian A to tell librarian B that the transfer is ongoing.

    Librarian B should use this to update the progress of the transfer.
    """

    source_transfer_id: int
    "The ID of the transfer. Note that this is the OutgoingTransfer ID."
    destination_transfer_id: int
    "The ID of the transfer. Note that this is the IncomingTransfer ID."


class CloneOngoingResponse(BaseModel):
    """
    In a librarian A -> librarian B transfer, this is the response
    from librarian B to librarian A after CloneOngoingRequest is accepted.
    """

    source_transfer_id: int
    "The ID of the transfer. Note that this is the OutgoingTransfer ID."
    destination_transfer_id: int
    "The ID of the transfer. Note that this is the IncomingTransfer ID."


class CloneStagedRequest(BaseModel):
    """
    In a librarian A -> librarian B transfer, this is the request
    from librarian A to tell librarian B that the transfer is staged and
    ready for ingest.

    Librarian B should use this to update the progress of the transfer
    and (asyncronously) ingest the file.
    """

    source_transfer_id: int
    "The ID of the transfer. Note that this is the OutgoingTransfer ID."
    destination_transfer_id: int
    "The ID of the transfer. Note that this is the IncomingTransfer ID."


class CloneStagedResponse(BaseModel):
    """
    The response after CloneStagedRequest is accepted.
    """

    source_transfer_id: int
    "The ID of the transfer. Note that this is the OutgoingTransfer ID."
    destination_transfer_id: int
    "The ID of the transfer. Note that this is the IncomingTransfer ID."
    success: bool = True
    "Whether the database changes were successful"


class CloneCompleteRequest(BaseModel):
    """
    In a librarian A -> librarian B transfer, this is the request
    that librarian B sends to librarian A to indicate that it has
    completed the transfer, and that it is ok to set the transfer
    status to completed.
    """

    source_transfer_id: int
    "The ID of the transfer. Note that this is the OutgoingTransfer ID."
    destination_transfer_id: int
    "The ID of the transfer. Note that this is the IncomingTransfer ID."
    store_id: int
    "The ID of the store that was the ultimate destination of the transfer."


class CloneCompleteResponse(BaseModel):
    """
    In a librarian A -> librarian B transfer, this is the response
    that librarian B sends to librarian A in response to a CloneCompleteRequest,
    if it was successful.
    """

    source_transfer_id: int
    "The ID of the transfer. Note that this is the OutgoingTransfer ID."
    destination_transfer_id: int
    "The ID of the transfer. Note that this is the IncomingTransfer ID."


class CloneFailedResponse(BaseModel):
    """
    (Generic) model for response when the clone failed.
    """

    reason: str
    "Reason for failure."
    suggested_remedy: str = "Please try again later."
    "Suggested remedy for failure."
    source_transfer_id: int
    "The ID of the transfer. Note that this is the OutgoingTransfer ID."
    destination_transfer_id: int
    "The ID of the transfer. Note that this is the IncomingTransfer ID."


class CloneFailRequest(BaseModel):
    """
    In a librarian A -> librarian B transfer, this is the request
    that librarian A sends to librarian B to indicate that it has
    failed the transfer, and that it is ok to set the transfer
    status to failed.
    """

    source_transfer_id: int
    "The ID of the transfer. Note that this is the OutgoingTransfer ID."
    destination_transfer_id: int
    "The ID of the transfer. Note that this is the IncomingTransfer ID."
    reason: str
    "Reason for failure."


class CloneFailResponse(BaseModel):
    """
    Response from the destination librarian to the source librarian
    indicating whether or not it was successful in failing the transfer.
    """

    source_transfer_id: int
    "The ID of the transfer. Note that this is the OutgoingTransfer ID."
    destination_transfer_id: int
    "The ID of the transfer. Note that this is the IncomingTransfer ID."
    success: bool
    "Whether or not the transfer was successfully failed."
