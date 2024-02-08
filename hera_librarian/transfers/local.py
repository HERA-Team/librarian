"""
Local transfer. Basically just a wrapper around `cp`.
"""

import os
import shutil
from pathlib import Path
from socket import gethostname

from .core import CoreTransferManager


class LocalTransferManager(CoreTransferManager):
    hostnames: list[str]
    "The hostname(s) of the machine being transferred to."

    def transfer(self, local_path: Path, remote_path: Path):
        """
        Raises
        ------

        ValueError
            If the transfer fails.
        PermissionError
            If the permissions cannot be set.
        """
        # Need to make sure that the the permissions are correctly
        # set on all files and directories that we copy over.
        # They should have rw-rw-r-- and rwxrwxr-x permissions.

        # Get the group of the parent.
        parent_group = remote_path.parent.stat().st_gid
        # Get this user's uid.
        uid = os.getuid()

        def set_for_file(file: Path):
            if file.is_dir():
                os.chmod(
                    file,
                    0o775,
                )
            else:
                os.chmod(file, 0o664)

            os.chown(file, uid=uid, gid=parent_group)

            return

        copy_success = False

        if local_path.is_dir():
            copy_success = shutil.copytree(local_path, remote_path)
        else:
            # Copy2 copies more metadata.
            copy_success = shutil.copy2(local_path, remote_path)

        if not copy_success:
            raise ValueError(f"Could not copy {local_path} to {remote_path}")

        # Set base permission
        set_for_file(remote_path)

        if remote_path.is_dir():
            for root, dirs, files in os.walk(remote_path):
                for x in dirs + files:
                    set_for_file(Path(root) / x)

        return copy_success

    @property
    def valid(self) -> bool:
        return gethostname() in self.hostnames
