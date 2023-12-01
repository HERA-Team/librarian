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


def _convert_book_id_to_obsid(book_id):
    """
    Convert an SO book_id into an obsid deterministically.

    We save the obsid as a unique bigint (signed 8-byte integer) in postgres,
    and want a deterministic way to generate it based on the book_id (which
    should be unique). Some components of the book_id are not numbers, so we use
    enums to convert to numbers. We also "pack" the values as a series of bits
    in different positions, as shown below:

        0111111122222222333333334444444455555555555555555555555555555555

    The different groups of numbers are:
        0: sign bit (unused)
        1: revision information
        2: book type
        3: telescope + optics tube
        4: slot flags
        5: timestamp

    We shift the different components into the corresponding parts and then
    return it to the calling context. The supported list of book types is:
    obs (observation), oper (operation), smurf, hk (housekeeping), stray, misc
    (miscellaneous).

    Not all formats use all components, in which case we use zeros. Some book
    types use a 5-digit ctime (the first 5 digits of the full 10-digit ctime),
    which we convert to a 10-digit timestamp by multiplying by 10**5. Also note
    that for hk data, the "daq_node" information is stored in place of the
    "tel_tube" information.

    Parameters
    ----------
    book_id : str
        The book_id of an SO book.

    Returns
    -------
    obsid : int
        The unique obsid corresponding to that book.

    Raises
    ------
    ValueError
        This is raised for various problems with parsing.
    """
    # build enum for book type
    book_type_enum = {
        "obs": 1,
        "oper": 2,
        "smurf": 3,
        "hk": 4,
        "stray": 5,
        "misc": 6,
    }

    # parse book_id into component parts, separated by underscores
    parsed_book_id = book_id.split("_")
    book_type = parsed_book_id[0].lower()

    # handle book type
    try:
        type_int = book_type_enum[book_type]
    except KeyError:
        raise ValueError(
            f"problem converting book type to int; type {book_type} not found"
        )

    if book_type == "obs":
        # we have timestamp, tel_tube, and slot_flags
        timestamp = int(parsed_book_id[1])
        tel_tube = parsed_book_id[2]
        slot_flags = parsed_book_id[3]
        expected_len = 4

        # extra variables
        daq_node = None
    elif book_type == "oper":
        # we have timestamp, tel_tube, and slot_flags
        timestamp = int(parsed_book_id[1])
        tel_tube = parsed_book_id[2]
        slot_flags = parsed_book_id[3]
        expected_len = 4

        # extra variables
        daq_node = None
    elif book_type == "smurf" or book_type == "stray":
        # we have 5-digit time and tel_tube
        timestamp = int(parsed_book_id[1]) * 10**5
        tel_tube = parsed_book_id[2]
        expected_len = 3

        # extra variables
        slot_flags = None
        daq_node = None
    elif book_type == "hk":
        # we have 5-digit time and daq_node
        timestamp = int(parsed_book_id[1]) * 10**5
        daq_node = parsed_book_id[2]
        expected_len = 3

        # extra variables
        tel_tube = None
        slot_flags = None
    else:  # book_type == "misc":
        # we have timestamp
        timestamp = int(parsed_book_id[1])
        expected_len = 2

        # extra variables
        tel_tube = None
        slot_flags = None
        daq_node = None

    if len(parsed_book_id) == expected_len:
        # no revision info
        revision_info = "r0"
    elif len(parsed_book_id) == expected_len + 1:
        # we have revision info
        revision_info = parsed_book_id[-1]
    else:
        raise ValueError(f"book_id {book_id} seems too long for type {book_type}")

    if tel_tube is not None:
        # handle telescope + tube info
        if tel_tube.lower().startswith("satp"):
            # This assumes the string is of the form "satpN", where N is the SAT
            # number. Note: we only accommodate up to 16 SATs.
            tt_int = int(tel_tube[4:])
        elif tel_tube.lower().startswith("lat"):
            # This is a LAT observation. There is a plain "lat", which is
            # distinct from "latc", "lati", or "lato". We group the plain "lat"
            # with "latc", because the only allowed value is "latc0".  We
            # accommodate up to 16 of each type, and add the actual tube number
            # to the final number value.
            if tel_tube.lower() == "lat":
                # plain lat
                tt_int = 32
            else:
                tube_type = tel_tube[3]
                if tube_type == "i":
                    tt_int = 16
                elif tube_type == "c":
                    tt_int = 33  # accounting for plain "lat"
                elif tube_type == "o":
                    tt_int = 48
                else:
                    raise ValueError(f"LAT tube type {tube_type} not recognized")

                tube_number = int(tel_tube[4:])
                tt_int += tube_number
        elif tel_tube.lower().startswith("ocs"):
            # This is an OCS book. It also has the form "ocsN", where N is the
            # OCS agent number. We accommodate up to 8 OCS agents.
            tt_int = 64 + int(tel_tube[3:])
        else:
            raise ValueError(f"could not recognize telescope type {tel_tube}")

        if tt_int < 0 or tt_int > 2**8:
            raise ValueError(
                f"problem converting telescope tube {tel_tube} to number"
            )
    elif daq_node is not None:
        # placeholder for now
        tt_int = 1
    else:
        tt_int = 0

    if slot_flags is not None:
        # convert bit-encoded slot flags into an int
        slot_int = int(slot_flags, 2)  # base-2 encoding
    else:
        slot_int = 0

    # handle revision info
    try:
        rev_int = int(revision_info[1:])
    except ValueError:
        raise ValueError(f"problem converting revision info {revision_info} to number")

    # put it all together
    obsid = timestamp
    obsid += 2**32 * slot_int
    obsid += 2**40 * tt_int
    obsid += 2**48 * type_int
    obsid += 2**56 * rev_int

    if obsid >= 2**63 or obsid < -2**63:
        raise ValueError("obsid f{obsid} out of range")
    return obsid


def get_metadata_from_path(path):
    """
    Get the obsid, timestamp_start, and type from a path.

    Parameters
    ----------
    path : str
        The full path to the file. We assume it is a YAML file with a
        "book_id" key. We then convert this value into a unique "obsid".

    Returns
    -------
    dict or None
        If the book_id, timestamp_start, and type are found, return
        them (plus obsid) inside of a dict. Otherwise, return None.
    """
    try:
        import yaml
        # assumes index card is in the top-level of path
        index_card = os.path.join(path, "M_index.yaml")
        with open(index_card, "r") as stream:
            file_info = yaml.safe_load(stream)

        metadata_dict = {}
        metadata_dict["book_id"] = file_info["book_id"]
        metadata_dict["timestamp_start"] = file_info["start_time"]
        metadata_dict["type"] = file_info["type"]
        metadata_dict["obsid"] = _convert_book_id_to_obsid(file_info["book_id"])
        # get optional bits
        if "stop_time" in file_info:
            metadata_dict["timestamp_end"] = file_info["stop_time"]
        if "observatory" in file_info:
            metadata_dict["observatory"] = file_info["observatory"]
        if "telescope" in file_info:
            metadata_dict["telescope"] = file_info["telescope"]
        if "stream_ids" in file_info:
            metadata_dict["stream_ids"] = file_info["stream_ids"]
        if "subtype" in file_info:
            metadata_dict["subtype"] = file_info["subtype"]
        if "tags" in file_info:
            metadata_dict["tags"] = file_info["tags"]
        if "scanification" in file_info:
            metadata_dict["scanification"] = file_info["scanification"]
        if "hwp_rate_hz" in file_info:
            metadata_dict["hwp_rate_hz"] = file_info["hwp_rate_hz"]
        if "sequencer_ref" in file_info:
            metadata_dict["sequencer_ref"] = file_info["sequencer_ref"]
        return metadata_dict
    except (ImportError, FileNotFoundError, KeyError):
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
    #TODO Fix this
    from pathlib import Path
    from checksumdir import dirhash

    path = Path(path).resolve()

    return dirhash(path, "md5")


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

    for dirname, dirs, files in os.walk(path):
        for f in files:
            size += os.path.getsize(dirname + '/' + f)

    return size


def gather_info_for_path(path):
    info = {}
    info['type'] = get_type_from_path(path)
    info['md5'] = get_md5_from_path(path)
    info['size'] = get_size_from_path(path)

    metadata_dict = get_metadata_from_path(path)
    if metadata_dict is not None:
        info["obsid"] = metadata_dict["obsid"]
        info["timestamp_start"] = metadata_dict["timestamp_start"]
        info["type"] = metadata_dict["type"]

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
