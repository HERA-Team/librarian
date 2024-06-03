"""
TransferStatus enum.
"""

from enum import Enum


class TransferStatus(Enum):
    """
    The status of a transfer.
    """

    INITIATED = 0
    "Transfer has been initiated, but client has not yet started moving data"
    ONGOING = 1
    "Client is currently (asynchronously) moving data to or from us. This is not possible with all transfer managers."
    STAGED = 2
    "Transfer has been staged, server is ready to complete the transfer."
    COMPLETED = 3
    "Transfer is completed"
    FAILED = 4
    "Transfer has been confirmed to have failed."
    CANCELLED = 5
    "Transfer has been cancelled by the client."
