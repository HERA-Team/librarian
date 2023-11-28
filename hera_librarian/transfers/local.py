"""
Local transfer. Basically just a wrapper around `cp`.
"""

import shutil
from pathlib import Path

from .core import CoreTransferManager

class LocalTransferManager(CoreTransferManager):
    def __init__(self):
        super().__init__()

    def transfer(self, local_path: Path, remote_path: Path):
        return shutil.copy(local_path, remote_path)
    