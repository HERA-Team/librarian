"""
Check in models.

Allows for the modification of the state of inbound and outbound
transfers on request.
"""

from pydantic import BaseModel

from hera_librarian.transfer import TransferStatus


class CheckinStatusRequest(BaseModel):
    """
    Requests the status of a transfer.
    """

    source_transfer_ids: list[int]
    "Source (Outgoing) transfer IDs to modify. Can be empty."
    destination_transfer_ids: list[int]
    "Destination (Incoming) transfer IDs to modify. Can be empty."


class CheckinStatusResponse(BaseModel):
    """
    Responds with the status of the requested transfers.

    None may indicate that this transfer does not exist, or
    that you do not have access.
    """

    source_transfer_status: dict[int, TransferStatus | None]
    "Dictionary mapping integer transfer IDs to their status."
    destination_transfer_status: dict[int, TransferStatus | None]
    "Dictionary mapping integer transfer IDs to their status."


class CheckinUpdateRequest(BaseModel):
    """
    Requests a change of status.
    """

    source_transfer_ids: list[int]
    "Source (Outgoing) transfer IDs to modify. Can be empty."
    destination_transfer_ids: list[int]
    "Destination (Incoming) transfer IDs to modify. Can be empty."

    new_status: TransferStatus
    "The status that you would like to change your transfers to."


class CheckinUpdateResponse(BaseModel):
    """
    Responds to the request for change in status.
    """

    modified_source_transfer_ids: list[int]
    "The source (Outgoing) transfer IDs that were modified."
    modified_destination_transfer_ids: list[int]
    "The destination (Incoming) transfer IDs that were modified."

    unmodified_source_transfer_ids: list[int]
    "The source (Outgoing) transfer IDs that were not modified."
    unmodified_destination_transfer_ids: list[int]
    "The destination (Incoming) transfer IDs that were not modified."

    reasons: list[str]
    "Reasons for why a transfer was not modified."
