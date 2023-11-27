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