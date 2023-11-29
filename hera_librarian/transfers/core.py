"""
Core transfer manager (prototype)
"""

class CoreTransferManager:
    def __init__(self):
        pass

    def transfer(self, local_path: str, remote_path: str):
        """
        Transfer a file from the local machine to the store.

        Parameters
        ----------
        local_path : str
            Path to the local file to upload.
        store_path : str
            Path to store file at on destination host.
        """
        raise NotImplementedError

    @property
    def valid(self) -> bool:
        """
        Whether or not this transfer manager is valid for the
        current system we are running on.
        """
        raise NotImplementedError

    def to_dict(self):
        """
        Convert this transfer manager to a dictionary, for storage in the databsae.
        """
        raise NotImplementedError

    @classmethod
    def from_dict(cls, data: dict):
        """
        Create a transfer manager from a dictionary.
        """
        raise NotImplementedError