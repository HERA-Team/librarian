# -*- mode: python; coding: utf-8 -*-
# Copyright 2016-2017 the HERA Team.
# Licensed under the BSD License.

"""Utilities for interacting with the librarian.

These are various, mainly I/O-related, functions that could be shared between
a variety of scripts. A lot of what we do is related to the fact that some
Librarian "files" are MIRIAD data sets that are actually directories.

"""

__all__ = str('''
gather_info_for_path
get_type_from_path
get_obsid_from_path
get_pol_from_path
get_md5_from_path
get_size_from_path
normalize_and_validate_md5
print_info_for_path
format_jd_as_calendar_date
format_jd_as_iso_date_time
format_obsid_as_calendar_date
''').split()

import hashlib
import locale
import os.path
import re

import numpy as np


def get_type_from_path(path):
    """Get the "file type" from a path.

    This is just the last bit of text following the last ".", by definition.

    """
    return path.split('.')[-1]


def get_pol_from_path(path):
    """Get the data polarization from a path, assuming it follows HERA naming
    conventions. Returns None if nothing pol-like is seen.

    This is super fragile!!!! There should be a better way. Also we hardcode
    the XY basis.

    This function was written because at the time, RTP needed to be handed this
    information for it to ingest files from us. Is that still the case?

    """
    matches = re.findall(r'\.([xy][xy])\.', path)
    if not len(matches):
        return None
    return matches[-1]


def get_obsid_from_path(path):
    """Get the obsid from a path, if it is a MIRIAD UV dataset.

    We used to try to guess the obsid from non-UV data sets if something like
    a JD was in the name, but that's too fragile, and as of September 2017 our
    data sets have obsids embedded in them.

    """
    if os.path.isdir(path):
        try:
            import aipy
            uv = aipy.miriad.UV(path)
            return uv['obsid']
        except (RuntimeError, ImportError, IndexError, KeyError):
            pass

    return None


_lc_md5_pattern = re.compile('^[0-9a-f]{32}$')


def normalize_and_validate_md5(text):
    """Convert *text* into a normalized hexadecimal representation of an MD5 sum,
    raising ValueError if it does not look like one.

    The "normalization" consists only of stripping whitespace and lowercasing
    hex letters.

    """
    norm_text = text.strip().lower()
    if not len(_lc_md5_pattern.findall(norm_text)):
        raise ValueError('%r does not look like an MD5 sum' % (text,))
    return norm_text


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
            md5.update(subhash)  # this is the hex digest, like we want
            md5.update('  .')  # compat with command-line approach
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


def gather_info_for_path(path):
    info = {}
    info['type'] = get_type_from_path(path)
    info['md5'] = get_md5_from_path(path)
    info['size'] = get_size_from_path(path)

    obsid = get_obsid_from_path(path)
    if obsid is not None:
        info['obsid'] = obsid

    return info


def print_info_for_path(path):
    """This utility function is meant to be run on a Librarian store. The
    librarian server SSHes into us and runs this function, then parses the
    printed output.

    """
    import json
    import sys
    json.dump(gather_info_for_path(path), sys.stdout)


def format_jd_as_calendar_date(jd, scale='utc', **kwargs):
    """Format a Julian Date value as a calendar date, returning the date as a
    string.

    The return value will look like `2016-07-24`. This, of course, truncates
    all time-of-day information contained in the JD.

    The `scale` keyword argument and remaining keyword arguments are passed to
    the constructor for the AstroPy `Time` class. The defaults should be fine
    in most cases, though, but be careful about the timescale.

    """
    from astropy.time import Time
    t = Time(jd, format='jd', scale=scale, **kwargs)
    return t.iso[:10]


def format_jd_as_iso_date_time(jd, scale='utc', precision=0, **kwargs):
    """Format a Julian Date value as an ISO 8601 date and time, returning a
    string.

    The return value will look like `2016-07-24 11:04:36`. Sub-second
    precision is truncated by default, but can be preserved by using the
    `precision` keword, which specifies the number of sub-second decimal
    places to include in the return value.

    The `scale` keyword argument and remaining keyword arguments are passed to
    the constructor for the AstroPy `Time` class. The defaults should be fine
    in most cases, though, but be careful about the timescale.

    """
    from astropy.time import Time
    t = Time(jd, format='jd', scale=scale, precision=precision, **kwargs)
    return t.iso


def format_obsid_as_calendar_date(obsid):
    """Format an obsid as a UTC calendar date, returning the date as a string.

    This makes only sense because our obsids are actually times, expressed as
    integer GPS seconds. The return value will look like `2016-07-24`. This,
    of course, truncates all time-of-day information contained in the JD. Note
    that we convert from the GPS to the UTC timescale, requiring an accurate
    leap-second table.

    """
    from astropy.time import Time
    t = Time(obsid, format='gps')
    return t.utc.iso[:10]
