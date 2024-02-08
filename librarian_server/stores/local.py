"""
The simplest possible store is a local store, which just executes
all commands on the machine local to the librarian server.
"""

import os
import shutil
import stat
import uuid
from pathlib import Path

from hera_librarian.transfers.core import CoreTransferManager
from hera_librarian.utils import (
    get_md5_from_path,
    get_size_from_path,
    get_type_from_path,
)

from .core import CoreStore
from .pathinfo import PathInfo


class LocalStore(CoreStore):
    staging_path: Path
    store_path: Path

    group_write_after_stage: bool = False
    "If true, the user running the server will chmod the stage directories to 775 after creating."
    own_after_commit: bool = False
    "If true, the user running the server will chown the files after committing."
    readonly_after_commit: bool = False
    "If true, the user running the server will chmod the files to 444 and folders to 555 after commit."

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
        if not path.is_absolute():
            complete_path = (self.staging_path / path).resolve()
        else:
            complete_path = path.resolve()

        # Check if the file is validly in our staging area. Someone
        # could pass us ../../../../../../etc/passwd or something.

        if not (self.staging_path.resolve() in complete_path.parents):
            raise ValueError(f"Provided path {path} resolves outside staging area.")

        return complete_path

    def _resolved_path_store(self, path: Path) -> Path:
        if not path.is_absolute():
            complete_path = (self.store_path / path).resolve()
        else:
            complete_path = path.resolve()

        if not (self.store_path.resolve() in complete_path.parents):
            raise ValueError(f"Provided path {path} resolves outside store area.")

        return complete_path

    def stage(self, file_size: int, file_name: Path) -> tuple[Path]:
        if file_size > self.free_space:
            raise ValueError("Not enough free space on store")

        # TODO: Do we want to actually keep track of files we have staged?
        #       Also maybe we want to check if the staging area is clear at startup?

        stage_path = Path(f"{uuid.uuid4()}")

        # Create the empty directory.
        resolved_path = self._resolved_path_staging(stage_path)

        if self.group_write_after_stage:
            resolved_path.mkdir(mode=0o775)
            os.chmod(resolved_path, 0o775)
        else:
            resolved_path.mkdir()

        return stage_path, resolved_path / file_name

    def unstage(self, path: Path):
        complete_path = self._resolved_path_staging(path)

        if os.path.exists(complete_path):
            try:
                os.rmdir(complete_path)
            except NotADirectoryError:
                # It's not a directory. Delete it.
                os.remove(complete_path)
            except OSError:
                # Directory is not empty. Delete it and all its contents. Unfortunately we can't log this..
                shutil.rmtree(complete_path)

        # Check if the parent is still in the staging area. We don't want
        # to leave random dregs around!

        if os.path.exists(complete_path.parent):
            try:
                resolved_path = self._resolved_path_staging(complete_path.parent)
                resolved_path.rmdir()
            except ValueError:
                # The parent is not in the staging area. We can't delete it.
                pass

        return

    def commit(self, staging_path: Path, store_path: Path):
        need_ownership_changes = self.own_after_commit or self.readonly_after_commit

        resolved_path_staging = self._resolved_path_staging(staging_path)
        resolved_path_store = self._resolved_path_store(store_path)

        if not need_ownership_changes:
            # We can just move the file.
            shutil.move(
                resolved_path_staging,
                resolved_path_store,
            )

            return
        else:
            # We need to copy the file and then set the permissions.
            if resolved_path_store.is_dir():
                shutil.copytree(resolved_path_staging, resolved_path_store)
            else:
                shutil.copy2(resolved_path_staging, resolved_path_store)

        try:
            # Set permissions and ownership.
            def set_for_file(file: Path):
                if self.own_after_commit:
                    shutil.chown(file, user=os.getuid(), group=os.getgid())

                if self.readonly_after_commit:
                    if file.is_dir():
                        os.chmod(
                            file,
                            stat.S_IREAD
                            | stat.S_IRGRP
                            | stat.S_IROTH
                            | stat.S_IXUSR
                            | stat.S_IXGRP
                            | stat.S_IXOTH,
                        )
                    else:
                        os.chmod(file, stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH)

            # Set for the top-level file.
            set_for_file(resolved_path_store)

            # If this is a directory, walk.
            if resolved_path_store.is_dir():
                for root, dirs, files in os.walk(resolved_path_store):
                    for x in dirs + files:
                        set_for_file(Path(root) / x)
        except ValueError:
            raise PermissionError(f"Could not set permissions on {resolved_path_store}")

        return

    def store(self, path: Path) -> Path:
        # First, check if the file already exists, and it's within our store
        resolved_path = self._resolved_path_store(path)

        if resolved_path.exists():
            raise FileExistsError(f"File {path} already exists on store.")

        # Now create any directory structure that is required to store the file.
        resolved_path.parent.mkdir(parents=True, exist_ok=True)

        return resolved_path

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

    def can_transfer(self, using: CoreTransferManager):
        return using.valid

    def transfer_out(
        self, store_path: Path, destination_path: Path, using: CoreTransferManager
    ) -> bool:
        # First, check if the file exists on the store.
        resolved_store_path = self._resolved_path_store(store_path)

        if not resolved_store_path.exists():
            raise FileNotFoundError(f"File {store_path} does not exist on store.")

        if using.valid:
            # We can transfer it out!
            return using.transfer(resolved_store_path, destination_path)

        return False
