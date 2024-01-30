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
        if local_path.is_dir():
            return shutil.copytree(local_path, remote_path)
        else:
            # Copy2 copies more metadata.
            return shutil.copy2(local_path, remote_path)

    @property
    def valid(self) -> bool:
        return gethostname() == self.hostname
