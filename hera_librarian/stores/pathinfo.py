"""
Path info object.
"""

from pathlib import Path
from dataclasses import dataclass

@dataclass
class PathInfo:
    """
    Path information for a file. It is the responsibility of the
    various implementations of the store managers to return valid
    PathInfo objects, which then serialize themselves.
    """
    path: Path
    "Path being considered"
    filetype: str
    "File type at the path (e.g. png)"
    md5: str
    "MD5 sum of the file at the path"
    size: int
    "Size in bytes of the file at the path"

    def to_dict(self) -> dict:
        """
        Converts the path information to a dictionary.
        """
        return {
            "path": str(self.path),
            "filetype": self.filetype,
            "md5": self.md5,
            "size": self.size,
        }
    
    def get(self, key: str, default=None):
        """
        Get an attribute of the path information.
        """
        return getattr(self, key, default)