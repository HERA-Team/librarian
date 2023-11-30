"""
Local transfer. Basically just a wrapper around `cp`.
"""

import shutil
from pathlib import Path
from socket import gethostname

from .core import CoreTransferManager

class LocalTransferManager(CoreTransferManager):
    hostname: str 
    "The hostname of the machine being transferred to."

    def transfer(self, local_path: Path, remote_path: Path):
        # TODO: Verify that the location we are trying to copy to exists.
        return shutil.copy(local_path, remote_path)

    @property
    def valid(self) -> bool:
        return gethostname() == self.hostname
    