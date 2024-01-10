"""
Transfer managers; the only thing that the client _really_ needs to
know about the stores.
"""

from .core import CoreTransferManager
from .local import LocalTransferManager

TransferManagers: dict[int, CoreTransferManager] = {
    0: CoreTransferManager,
    1: LocalTransferManager,
}

TransferManagerNames: dict[str, int] = {
    "core": 0,
    "local": 1,
}

def transfer_manager_from_name(name: str) -> CoreTransferManager:
    """
    Get a transfer manager from its name.
    """
    return TransferManagers[TransferManagerNames[name]]