# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Team.
# Licensed under the BSD License.

"""Utilities for interacting with the librarian.

These are various, mainly I/O-related, functions that could be shared between
a variety of scripts. A lot of what we do is related to the fact that some
Librarian "files" are MIRIAD data sets that are actually directories.

"""

__all__ = str('''
get_start_jd_from_path
get_type_from_path
get_obsid_from_path
get_md5_from_path
get_size_from_path
''').split()

import hashlib, locale, os.path, re

import numpy as np
import aipy as a


def get_start_jd_from_path(path):
    """Get the JD from a path, assuming it follows HERA naming conventions.

    This is super fragile!!!! There should be a better way.

    """
    return float(re.findall(r'\d+\.\d+', path)[0])


def get_type_from_path(path):
    """Get the file type from a path, assuming it follows HERA naming conventions.

    This is super fragile!!!! There should be a better way.

    """
    return path.split ('.')[-1]


def get_obsid_from_path(path):
    """Get the obsid from a path.

    We assume that it is a MIRIAD UV dataset.

    """
    uv = a.miriad.UV(path)
    try:
        return uv['obsid']
    except KeyError:
        pass

    # If we're still here, the file is an older one that doesn't have its
    # obsid embedded. In that case, we reconstruct it from the JD in the path.
    # TODO: I don't think we're embedding obsids yet, so this is the code path
    # that's always taken! (PKGW, 2016/04/21).

    from astropy.time import Time
    jd = get_start_jd_from_path(path)
    return int(np.floor(Time(jd, scale='utc', format='jd').gps))


def _md5_of_file(path):
    """Compute and return the MD5 sum of a flat file. The MD5 is returned as a
    hexadecimal string.

    """
    md5 = hashlib.md5()

    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            md5.update(chunk)

    return md5.hexdigest()


def get_md5_from_path(path):
    """Compute the MD5 checksum of 'path', which is either a single flat file or a
    directory. The checksum is returned as a hexadecimal string.

    If 'path' is a flat file, the checksum is the MD5 sum of the file
    contents. If it is a directory, it is the MD5 sum of the MD5 sums and
    names of the files contained in the directory, sorted by their paths as
    starting with './'. In all cases, symbolic links, permissions, ownership
    information, etc. are ignored.

    The definition of the directory checksum is constructed such that the
    computation can be recomputed in a shell script. If you don't have
    pathological file names or locale settings:

      (cd $path && find . ! -type d -exec md5sum {} \; | sort -k 2 | md5sum)

    A more paranoid version:

      (cd $path && find . ! -type d -print0 |LC_ALL=C sort -z |xargs -0 -n1 md5sum |md5sum)

    For each input file, the 'md5sum' program prints the MD5 sum, two spaces,
    and then the file name. This sets the format for the outermost MD5 we do.

    """
    if not os.path.isdir(path):
        return _md5_of_file(path)

    # make sure that path looks like foo/bar, not foo/bar/ or foo/bar/./. .
    # This makes it easier to munge the outputs from os.walk().

    while path.endswith('/.'):
        path = path[:-2]

    if path[-1] == '/':
        path = path[:-1]

    def all_files():
        for dirname, dirs, files in os.walk(path):
            for f in files:
                yield dirname + '/' + f

    md5 = hashlib.md5()
    plen = len(path)

    try:
        # NOTE: this is not threadsafe. This will *probably* never come back
        # to bite us in the ass ...
        prevlocale = locale.getlocale(locale.LC_COLLATE)
        locale.setlocale(locale.LC_COLLATE, 'C')

        for f in sorted(all_files()):
            subhash = _md5_of_file(f)
            md5.update(subhash) # this is the hex digest, like we want
            md5.update('  .') # compat with command-line approach
            md5.update(f[plen:])
            md5.update('\n')
    finally:
        locale.setlocale(locale.LC_COLLATE, prevlocale)

    return md5.hexdigest()


def get_size_from_path(path):
    """Get the number of bytes occupied within `path`.

    If `path` is a directory, we just add up the sizes of all of the files it
    contains.

    """
    if not os.path.isdir(path):
        return os.path.getsize(path)

    size = 0

    for dirname, dirs, files in os.walk(path):
        for f in files:
            size += os.path.getsize(dirname + '/' + f)

    return size
