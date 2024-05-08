"""
The local async transfer manager.
"""

import os
import shutil
from pathlib import Path
from socket import gethostname

from hera_librarian.transfer import TransferStatus

from ..queues import Queue
from .core import CoreAsyncTransferManager


class LocalAsyncTransferManager(CoreAsyncTransferManager):
    hostnames: list[str]

    transfer_attempted: bool = False
    transfer_complete: bool = False

    def batch_transfer(self, paths: list[tuple[Path]]):
        copy_success = True

        for local_path, remote_path in paths:
            copy_success = copy_success and self.transfer(
                local_path=local_path, remote_path=remote_path
            )

        # Set local
        self.transfer_attempted = True
        self.transfer_complete = copy_success

        return copy_success

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
            # Note that dirs_exist_ok is not acceptable here for the
            # case where there is a folder that is being used as the File.
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

        return True

    @property
    def valid(self) -> bool:
        return gethostname() in self.hostnames

    @property
    def transfer_status(self) -> TransferStatus:
        if self.transfer_complete:
            return TransferStatus.COMPLETED
        else:
            if not self.transfer_attempted:
                return TransferStatus.INITIATED
            else:
                return TransferStatus.FAILED
