"""
Local transfer. Basically just a wrapper around `cp`.
"""

import shutil
from pathlib import Path
from socket import gethostname

from .core import CoreTransferManager

class LocalTransferManager(CoreTransferManager):
    # The hostname of the machine being transferred to.
    hostname: str 

    def __init__(self, hostname: str):
        super().__init__()

        self.hostname = hostname

        return

    def transfer(self, local_path: Path, remote_path: Path):
        # TODO: Verify that the location we are trying to copy to exists.
        return shutil.copy(local_path, remote_path)

    @property
    def valid(self) -> bool:
        return gethostname() == self.hostname
    
    def to_dict(self) -> dict:
        return {
            "hostname": self.hostname,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "LocalTransferManager":
        return cls(hostname=d["hostname"])