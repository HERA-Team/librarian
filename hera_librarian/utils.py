"""
Useful utilities for files.
"""

import hashlib
import os
import os.path
import re
from pathlib import Path

import pkg_resources
import xxhash

# Here we bundle the source code from checksumdir rather than relying on it as
# a dependency. Maintenance seems to have ended for checksumdir, and we want access
# to faster hashing functions

# --- Begin MIT Licensed checksumdir ---


HASH_FUNCS = {
    "md5": hashlib.md5,
    "xxh3": xxhash.xxh3_128,
    "sha1": hashlib.sha1,
    "sha256": hashlib.sha256,
    "sha512": hashlib.sha512,
}


def dirhash(
    dirname,
    hashfunc="md5",
    excluded_files=None,
    ignore_hidden=False,
    followlinks=False,
    excluded_extensions=None,
    include_paths=False,
):
    hash_func = HASH_FUNCS.get(hashfunc)
    if not hash_func:
        raise NotImplementedError("{} not implemented.".format(hashfunc))

    if not excluded_files:
        excluded_files = []

    if not excluded_extensions:
        excluded_extensions = []

    if not os.path.isdir(dirname):
        raise TypeError("{} is not a directory.".format(dirname))

    hashvalues = []
    for root, dirs, files in os.walk(dirname, topdown=True, followlinks=followlinks):
        if ignore_hidden and re.search(r"/\.", root):
            continue

        dirs.sort()
        files.sort()

        for fname in files:
            if ignore_hidden and fname.startswith("."):
                continue

            if fname.split(".")[-1:][0] in excluded_extensions:
                continue

            if fname in excluded_files:
                continue

            hashvalues.append(_filehash(os.path.join(root, fname), hash_func))

            if include_paths:
                hasher = hash_func()
                # get the resulting relative path into array of elements
                path_list = os.path.relpath(os.path.join(root, fname)).split(os.sep)
                # compute the hash on joined list, removes all os specific separators
                hasher.update("".join(path_list).encode("utf-8"))
                hashvalues.append(hasher.hexdigest())

    return _reduce_hash(hashvalues, hash_func)


def _filehash(filepath, hashfunc):
    hasher = hashfunc()
    blocksize = 64 * 1024

    if not os.path.exists(filepath):
        return hasher.hexdigest()

    with open(filepath, "rb") as fp:
        while True:
            data = fp.read(blocksize)
            if not data:
                break
            hasher.update(data)
    return hasher.hexdigest()


def _reduce_hash(hashlist, hashfunc):
    hasher = hashfunc()
    for hashvalue in sorted(hashlist):
        hasher.update(hashvalue.encode("utf-8"))
    return hasher.hexdigest()


# --- end checksumdir ---


def get_type_from_path(path):
    """Get the "file type" from a path.

    This is just the last bit of text following the last ".", by definition.

    """
    return path.split(".")[-1]


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


def get_checksum_from_path(path: str | Path, hash_function: str = "xxh3") -> str:
    """
    Compute the checksum of a file from a path. This allows you to select
    the underlying checksum function, which is by default the very fast
    xxh3. Using this function, you also always have the hashing function
    pre-pended to the hash itself.
    """

    path = Path(path).resolve()

    if path.is_dir():
        return hash_function + ":::" + dirhash(path, hash_function)
    else:
        # Just a single file. That's fine!
        return hash_function + ":::" + _filehash(path, HASH_FUNCS[hash_function])


def get_hash_function_from_hash(hash: str) -> str:
    """
    Searches the hash for the hash function. If none is found, then we return
    the old default (md5).
    """

    for hash_func_name in HASH_FUNCS.keys():
        if hash.startswith(hash_func_name + ":::"):
            return hash_func_name

    return "md5"


def get_base_hash_from_hash(hash: str) -> str:
    """
    Gets the 'base' hash without our hashfunc::: prepended.
    """

    for hash_func_name in HASH_FUNCS.keys():
        if hash.startswith(hash_func_name + ":::"):
            return hash.replace(hash_func_name + ":::", "")

    return hash


def compare_checksums(a: str, b: str) -> bool:
    """
    Compares two checksums to see if they match.

    Raises a ValueError if a, b were checksummed with differing algorithms.
    """

    hf_a = get_hash_function_from_hash(a)
    hf_b = get_hash_function_from_hash(b)

    if hf_a != hf_b:
        raise ValueError(
            f"Checksums {a} and {b} were created with differing has functions!"
        )

    return get_base_hash_from_hash(a) == get_base_hash_from_hash(b)


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
            size += os.path.getsize(dirname + "/" + f)

    return size
