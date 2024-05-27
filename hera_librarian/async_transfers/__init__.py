"""
Async transfer managers. The only thing that the remote librarians
really need to know about the stores.
"""

from .core import CoreAsyncTransferManager
from .local import LocalAsyncTransferManager
from .rsync import RsyncAsyncTransferManager
from .globus import GlobusAsyncTransferManager

AsyncTransferManagers: dict[int, CoreAsyncTransferManager] = {
    0: CoreAsyncTransferManager,
    1: LocalAsyncTransferManager,
    2: RsyncAsyncTransferManager,
    3: GlobusAsyncTransferManager,
}

AsyncTransferManagerNames: dict[str, int] = {
    "core": 0,
    "local": 1,
    "rsync": 2,
    "globus": 3,
}


def async_transfer_manager_from_name(name: str) -> CoreAsyncTransferManager:
    """
    Get an async transfer manager from its name.
    """
    return AsyncTransferManagers[AsyncTransferManagerNames[name]]
