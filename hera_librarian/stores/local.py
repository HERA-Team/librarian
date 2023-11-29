"""
The simplest possible store is a local store, which just executes
all commands on the machine local to the librarian server.
"""

from .core import CoreStore
from .pathinfo import PathInfo
from ..utils import get_md5_from_path, get_size_from_path, get_type_from_path

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
            shutil.disk_usage(self.staging_path).free,
        )

    def _resolved_path_staging(self, path: Path) -> Path:
        complete_path = (self.staging_path / path).resolve()

        # Check if the file is validly in our staging area. Someone
        # could pass us ../../../../../../etc/passwd or something.

        if not (self.staging_path.resolve() in complete_path.parents):
            raise ValueError(f"Provided path {path} resolves outside staging area.")

        return complete_path

    def _resolved_path_store(self, path: Path) -> Path:
        complete_path = (self.store_path / path).resolve()

        if not (self.store_path.resolve() in complete_path.parents):
            raise ValueError(f"Provided path {path} resolves outside store area.")

        return complete_path

    def stage(self, file_size: int) -> Path:
        if file_size > self.free_space:
            raise ValueError("Not enough free space on store")

        # TODO: Do we want to actually keep track of files we have staged?
        #       Also maybe we want to check if the staging area is clear at startup?

        stage_path = Path(f"{uuid.uuid4()}.tmp")

        # Create the empty file.
        (self.staging_path / stage_path).touch()

        return stage_path

    def unstage(self, path: Path):
        complete_path = self._resolved_path_staging(path)

        if os.path.exists(complete_path):
            os.remove(complete_path)

        return

    def commit(self, staging_path: Path, store_path: Path):
        shutil.move(
            self._resolved_path_staging(staging_path),
            self._resolved_path_store(store_path),
        )

    def to_dict(self):
        return {
            "name": self.name,
            "staging_path": str(self.staging_path),
            "store_path": str(self.store_path),
        }

    @classmethod
    def from_dict(cls, d):
        return cls(
            name=d["name"],
            staging_path=Path(d["staging_path"]),
            store_path=Path(d["store_path"]),
        )

    def path_info(self, path: Path) -> PathInfo:
        # Promote path to object if required
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"Path {path} does not exist")

        return PathInfo(
            # Use the old functions for consistency.
            path=path,
            filetype=get_type_from_path(str(path)),
            md5=get_md5_from_path(path),
            size=get_size_from_path(path),
        )
