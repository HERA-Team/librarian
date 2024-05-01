"""
A transfer manager for rsync transfers.
"""

from pathlib import Path
from socket import gethostname

import sysrsync

from hera_librarian.transfer import TransferStatus

from ..queues import Queue
from .core import CoreAsyncTransferManager


class RsyncAsyncTransferManager(CoreAsyncTransferManager):
    hostname: str

    transfer_attempted: bool = False
    transfer_complete: bool = False

    @property
    def valid(self) -> bool:
        if self.hostname == gethostname():
            return True

        return False

    def transfer(self, local_path: Path, remote_path: Path):
        try:
            sysrsync.run(
                source=local_path,
                destination=remote_path,
                destination_ssh=(
                    self.hostname if self.hostname != gethostname() else None
                ),
                strict=True,
            )

            return True
        except sysrsync.RsyncError as e:
            return False

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

    @property
    def transfer_status(self) -> TransferStatus:
        if self.transfer_complete:
            return TransferStatus.COMPLETED
        else:
            if not self.transfer_attempted:
                return TransferStatus.INITIATED
            else:
                return TransferStatus.FAILED
