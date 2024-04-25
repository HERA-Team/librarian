"""
Core async transfer manager (prototype)
"""

import abc
from pathlib import Path

from pydantic import BaseModel

from hera_librarian.transfer import TransferStatus

from ..queues import Queue


class CoreAsyncTransferManager(BaseModel, abc.ABC):
    """
    The core async transfer manager. This is the base class for all
    async transfer managers. It provides the basic interface for
    transferring data asynchronously.
    """

    @abc.abstractmethod
    def batch_transfer(self, paths: list[tuple[Path]]):
        """
        Perform a batch transfer of many paths simultaneously.

        In the simplest form, this is just a for loop over all the files.

        Parameters
        ----------
        paths : list[list[Path]]
            A list of path tuples of the form:
                [
                   (local_a, remote_a),
                   (local_b, remote_b),
                   ...
                   (local_z, remote_z)
                ]
        """
        raise NotImplementedError

    @abc.abstractmethod
    def transfer(self, local_path: Path, remote_path: Path):
        """
        Transfer a single file from the local machine to the store. This
        function can be handled synchronously or asynchronously.

        Parameters
        ----------
        local_path : Path
            Path to the local file to upload.
        remote : Path
            Path to store file at on destination host.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def valid(self) -> bool:
        """
        Whether or not this transfer manager is valid for the
        current system we are running on.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def transfer_status(self) -> TransferStatus:
        """
        Gets the current in-flight status of the transfer. For some
        methods, this is simple (they are synchronous from the perspective
        of the AsyncTransferManager!), but for others (e.g. GLOBUS) this will
        require interaction with an external API. Note that this is only
        valid for BATCH transfers, individual transfers are not guaranteed
        to set (e.g.) internal flags as required.
        """
        raise NotImplementedError
