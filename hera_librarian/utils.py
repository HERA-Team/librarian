"""
Useful utilities for files.
"""

import os.path

from pathlib import Path
from checksumdir import dirhash, _filehash, HASH_FUNCS


def get_type_from_path(path):
    """Get the "file type" from a path.

    This is just the last bit of text following the last ".", by definition.

    """
    return path.split('.')[-1]



def get_md5_from_path(path):
    """Compute the MD5 checksum of 'path', which is either a single flat file or a
    directory. The checksum is returned as a hexadecimal string.

    As of Librarian 2.0, we use the checksumdir library for this.
    """

    path = Path(path).resolve()

    if path.is_dir():
        return dirhash(path, "md5")
    else:
        # Just a single file. That's fine!
        return _filehash(path, HASH_FUNCS["md5"])


def get_size_from_path(path):
    """Get the number of bytes occupied within `path`.

    If `path` is a directory, we just add up the sizes of all of the files it
    contains.

    """
    from pathlib import Path

    path = Path(path).resolve()

    if not os.path.isdir(path):
        return os.path.getsize(path)

    size = 0

    for dirname, _, files in os.walk(path):
        for f in files:
            size += os.path.getsize(dirname + '/' + f)

    return size
