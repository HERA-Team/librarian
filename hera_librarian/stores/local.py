"""
The simplest possible store is a local store, which just executes
all commands on the machine local to the librarian server.
"""

from .core import CoreStore

from pathlib import Path

import os
import shutil
import uuid

class LocalStore(CoreStore):
    staging_path: Path
    store_path: Path

    def __init__(self, name: str, staging_path: Path, store_path: Path):
        super().__init__(name=name)

        self.staging_path = staging_path
        self.store_path = store_path

    @property
    def available(self) -> bool:
        # Look, if we don't have a filesystem we have tons of problems.
        return True

    @property
    def free_space(self) -> int:
        return min(
            shutil.disk_usage(self.store_path).free,
            shutil.disk_usage(self.staging_path).free
        )
    
    def stage(self, file_size: int) -> Path:
        if file_size > self.free_space:
            raise ValueError("Not enough free space on store")

        # TODO: Do we want to actually keep track of files we have staged?
        #       Also maybe we want to check if the staging area is clear at startup?

        stage_path = self.staging_path / f"{uuid.uuid4()}.tmp"

        # Create the empty file.
        stage_path.touch()

        return stage_path

    def unstage(self, path: Path):
        if os.path.exists(path):
            os.remove(path)

        return
    
    def commit(self, staging_path: Path, store_path: Path):
        shutil.move(staging_path, store_path)
    


