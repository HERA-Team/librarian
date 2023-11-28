"""
Core store (prototype).
"""

class CoreStore:
    """
    Prototype for store management. Should never be used directly
    (other than for type hints!). All derived classes must
    implement all of the functions defined in this prototype.
    
    All functions should be executed 'on' the store. That may involve
    connecting to remote machines!
    """

    name: str

    def __init__(self, name: str):
        self.name = name
    
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
    
    def stage(self, file_size: int) -> str:
        """
        Creates space in the staging area for a file of size file_size.

        Parameters
        ----------
        file_size: int
            Size of the file to be staged in bytes.

        Returns
        -------
        str
            Absolute path on the staging machine.
        """

        raise NotImplementedError
    
    def unstage(self, path: str):
        """
        Remove a file from the staging area.

        Parameters
        ----------
        path: str
            Absolute path on the staging machine.
        """
        raise NotImplementedError
    
    def commit(self, staging_path: str, store_path: str):
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