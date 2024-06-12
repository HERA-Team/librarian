"""
A transfer manager for rsync transfers.
"""

from pathlib import Path
from socket import gethostname

import sysrsync

from hera_librarian.transfer import TransferStatus

from .core import CoreAsyncTransferManager


class RsyncAsyncTransferManager(CoreAsyncTransferManager):
    """
    A transfer manager that uses rsync. For now, this only
    allows for transfers on the current hostname (as it is
    not intended to be used in production; for that Globus
    is the main supported method).
    """

    hostname: str

    transfer_attempted: bool = False
    transfer_complete: bool = False

    def valid(self, settings: "ServerSettings") -> bool:
        if self.hostname == gethostname():
            return True

        return False

    def transfer(self, local_path: Path, remote_path: Path, settings: "ServerSettings"):
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

    def batch_transfer(self, paths: list[tuple[Path]], settings: "ServerSettings"):
        copy_success = True

        self.transfer_attempted = True

        # This is a syncronous loop over these, but the transfers
        # are performed in an entirely separate thread.
        for local_path, remote_path in paths:
            copy_success = copy_success and self.transfer(
                local_path=local_path, remote_path=remote_path
            )

        # Set local
        self.transfer_complete = copy_success

        return copy_success

    def transfer_status(self, settings: "ServerSettings") -> TransferStatus:
        if self.transfer_complete:
            return TransferStatus.COMPLETED
        else:
            if not self.transfer_attempted:
                return TransferStatus.INITIATED
            else:
                return TransferStatus.FAILED
