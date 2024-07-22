"""
Core store (prototype).
"""

import abc
from pathlib import Path

from pydantic import BaseModel

from hera_librarian.transfers.core import CoreTransferManager

from .pathinfo import PathInfo


class CoreStore(BaseModel, abc.ABC):
    """
    Prototype for store management. Should never be used directly
    (other than for type hints, and obviously cannot due to being an ABC!).
    All derived classes must implement all of the functions defined in
    this abstract base class. We use ABC not prototypes as Pydantic is not
    compatible with prototypes.

    All functions should be executed 'on' the store. That may involve
    connecting to remote machines!
    """

    name: str

    @abc.abstractmethod
    def resolve_path_store(self, path: Path | str) -> Path:
        """
        Resolve a path relative to the store.

        Parameters
        ----------
        path : Path
            Path to resolve.

        Returns
        -------
        Path
            Resolved path.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def resolve_path_staging(self, path: Path | str) -> Path:
        """
        Resolve a path relative to the staging area.

        Parameters
        ----------
        path : Path
            Path to resolve.

        Returns
        -------
        Path
            Resolved path.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def available(self) -> bool:
        """
        Is the store available?
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def free_space(self) -> int:
        """
        How much free space is available on the store?

        nbytes: int
            Number of bytes available.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def stage(self, file_size: int, file_name: Path) -> tuple[Path]:
        """
        Creates space in the staging area for a file of size file_size.

        Parameters
        ----------
        file_size: int
            Size of the file to be staged in bytes.
        file_name: Path
            Name of the file to be staged.

        Returns
        -------
        Path
            Relative path on the staging store. Does not include file_name.
        Path
            Absolute path on the staging store. Use this to upload files to the store.
            Includes file_name.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def unstage(self, path: Path):
        """
        Remove a file from the staging area.

        Parameters
        ----------
        Path
            Relative path to the staging store.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def commit(self, staging_path: Path, store_path: Path):
        """
        Commit a file from the staging area to the store.

        Use staging_path from stage()'s file_name and store_path from store().

        Parameters
        ----------
        staging_path: str
            Absolute path on the staging machine.
        store_path: str
            Absolute path on the store machine.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, path: Path):
        """
        Delete a file from the store.

        Parameters
        ----------
        path : Path
            Path to the file in the store to delete.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def store(self, path: Path) -> Path:
        """
        Get an absolute path for a deposit with name path.

        Parameters
        ----------
        path : Path
            Relative filename on the store.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def path_info(self, path: Path, hash_function: str = "xxh3") -> PathInfo:
        """
        Get information about a file or directory at a path.

        Parameters
        ----------
        path : Path
            Path to do this at.
        hash_function: str
            The hashing function chosen for checksuming this data.

        Returns
        -------
        PathInfo
            Filled PathInfo object.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def can_transfer(self, using: CoreTransferManager) -> bool:
        """
        Determines if a transfer can be performed using the specified TransferManager.

        Parameters
        ----------
        using : CoreTransferManager
            The transfer manager to check.

        Returns
        -------
        bool
            True if the transfer can be performed, False otherwise.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def transfer_out(
        self, store_path: Path, destination_path: Path, using: CoreTransferManager
    ) -> bool:
        """
        Transfer a file from the store to a destination.

        Parameters
        ----------
        store_path : Path
            Path to the file in the store to transfer.
        destination_path : Path
            Destination path to transfer the file to.
        using : CoreTransferManager
            The transfer manager to use for the transfer.

        Returns
        -------
        bool
            True if the transfer is successful, False otherwise.
        """
        raise NotImplementedError
