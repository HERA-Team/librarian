"""
A transfer manager for rsync transfers.
"""

from pathlib import Path

import sysrsync

from ..queues import Queue
from .core import CoreAsyncTransferManager


class RsyncAsyncTransferManager(CoreAsyncTransferManager):
    queue: Queue = Queue.RSYNC
    hostname: str

    def valid(self) -> bool:
        # TODO: figure out how to check we can rsync to a hostname.
        return False

    def transfer(self, local_path: Path, remote_path: Path):
        try:
            sysrsync.run(
                source=local_path,
                destination=remote_path,
                destination_ssh=self.hostname,
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

        return copy_success
