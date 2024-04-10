"""
Core async transfer manager (prototype)
"""

import abc
from pathlib import Path

from pydantic import BaseModel

from ..queues import Queue


class CoreAsyncTransferManager(BaseModel, abc.ABC):
    queue: Queue
    "The type of queue that this transfer manager is associated with."

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

    @abc.abstractmethod
    @property
    def valid(self) -> bool:
        """
        Whether or not this transfer manager is valid for the
        current system we are running on.
        """
        raise NotImplementedError
