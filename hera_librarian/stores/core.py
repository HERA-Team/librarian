"""
Core store (prototype).
"""

from .pathinfo import PathInfo
from pathlib import Path
from pydantic import BaseModel


class CoreStore(BaseModel):
    """
    Prototype for store management. Should never be used directly
    (other than for type hints!). All derived classes must
    implement all of the functions defined in this prototype.

    All functions should be executed 'on' the store. That may involve
    connecting to remote machines!
    """

    name: str

    @property
    def available(self) -> bool:
        """
        Is the store available?
        """
        raise NotImplementedError

    @property
    def free_space(self) -> int:
        """
        How much free space is available on the store?

        nbytes: int
            Number of bytes available.
        """
        raise NotImplementedError

    def stage(self, file_size: int) -> Path:
        """
        Creates space in the staging area for a file of size file_size.

        Parameters
        ----------
        file_size: int
            Size of the file to be staged in bytes.

        Returns
        -------
        str
            Relative path on the staging store. To get
        """

        raise NotImplementedError

    def unstage(self, path: Path):
        """
        Remove a file from the staging area.

        Parameters
        ----------
        path: str
            Relative path to the staging store.
        """
        raise NotImplementedError

    def commit(self, staging_path: Path, store_path: Path):
        """
        Commit a file from the staging area to the store.

        Parameters
        ----------
        staging_path: str
            Absolute path on the staging machine.
        store_path: str
            Absolute path on the store machine.
        """
        raise NotImplementedError

    def to_dict(self) -> dict:
        """
        Converts the store information to a dictionary. If required,
        the store should be able to re-create itself from that dictionary.
        """
        raise NotImplementedError

    @classmethod
    def from_dict(cls, d: dict) -> "CoreStore":
        """
        Creates a store from a dictionary. The dictionary should be
        the same as the one returned by to_dict.
        """
        raise NotImplementedError

    def path_info(self, path: Path) -> PathInfo:
        """
        Get information about a file or directory at a path.

        Parameters
        ----------
        path : Path
            Path to do this at.

        Returns
        -------
        PathInfo
            Filled PathInfo object.
        """
        raise NotImplementedError
