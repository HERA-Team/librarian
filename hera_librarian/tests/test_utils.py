# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD License

"""Test code in hera_librarian/utils.py

"""

import pytest

import json

from hera_librarian import utils

# import test data attributes from __init__.py
from . import ALL_FILES, filetypes, md5sums, obsids, pathsizes


def test_get_type_from_path():
    """Test type checking from path"""
    path = "/some/long/file.name.txt"
    assert utils.get_type_from_path(path) == "txt"

    return


def test_get_pol_from_path():
    """Test polarization extraction from filename"""
    filename = "zen.2458000.12345.xx.uvh5"
    assert utils.get_pol_from_path(filename) == "xx"

    filename = "zen.2458000.12345.uvh5"
    assert utils.get_pol_from_path(filename) is None

    return


@pytest.mark.filterwarnings("ignore:numpy.ufunc size changed")
@ALL_FILES
def test_get_obsid_from_path(datafiles):
    """Test extracting obsid values from datasets"""
    filepaths = sorted(map(str, datafiles.listdir()))
    for obsid, path in zip(obsids, filepaths):
        assert utils.get_obsid_from_path(path) == obsid

    return


def test_normalize_and_validate_md5():
    """Test md5sum normalization"""
    md5sum = "d41d8cd98f00b204e9800998ecf8427e"
    # function does not do anything for text already lowercase
    assert utils.normalize_and_validate_md5(md5sum) == md5sum

    md5sum_padded = f"{md5sum}   "
    assert utils.normalize_and_validate_md5(md5sum_padded) == md5sum

    md5sum_upper = f"{md5sum.upper()}   "
    assert utils.normalize_and_validate_md5(md5sum_upper) == md5sum

    # make sure error is raised when length is incorrect
    with pytest.raises(ValueError, match="does not look like an MD5 sum"):
        utils.normalize_and_validate_md5(md5sum[:-1])

    return


@ALL_FILES
def test_md5_of_file(datafiles):
    """Test generating md5sum of file"""
    filepaths = sorted(map(str, datafiles.listdir()))
    assert utils._md5_of_file(filepaths[1]) == md5sums[1]

    return


@ALL_FILES
def test_get_md5_from_path(datafiles):
    """Test getting the md5sum for both a flat file and directory"""
    filepaths = sorted(map(str, datafiles.listdir()))
    # test normal execution
    for md5sum, path in zip(md5sums, filepaths):
        assert utils.get_md5_from_path(path) == md5sum

    # test adding funny bits to the ends of the directory names
    datafile_miriad = filepaths[0] + "//."
    assert utils.get_md5_from_path(datafile_miriad) == md5sums[0]

    return


@ALL_FILES
def test_get_size_from_path(datafiles):
    """Test computing filesize from path"""
    filepaths = sorted(map(str, datafiles.listdir()))
    for pathsize, path in zip(pathsizes, filepaths):
        assert utils.get_size_from_path(path) == pathsize

    return


@ALL_FILES
def test_gather_info_for_path(datafiles):
    """Test getting all info for a given path"""
    filepaths = sorted(map(str, datafiles.listdir()))
    for filetype, md5, size, obsid, path in zip(filetypes, md5sums, pathsizes, obsids, filepaths):
        info = utils.gather_info_for_path(path)
        assert info["type"] == filetype
        assert info["md5"] == md5
        assert info["size"] == size
        assert info["obsid"] == obsid

    return


@ALL_FILES
def test_print_info_for_path(datafiles, capsys):
    """Test printing file info to stdout"""
    filepaths = sorted(map(str, datafiles.listdir()))
    for filetype, md5, size, obsid, path in zip(filetypes, md5sums, pathsizes, obsids, filepaths):
        utils.print_info_for_path(path)
        out, err = capsys.readouterr()
        # convert from json to dict
        out_dict = json.loads(out)

        # build up correct dict
        correct_info = {"type": filetype, "md5": md5, "size": size, "obsid": obsid}
        assert out_dict == correct_info

    return


def test_format_jd_as_calendar_date():
    """Test converting JD to calendar date"""
    jd = 2456000
    assert utils.format_jd_as_calendar_date(jd) == "2012-03-13"

    return


def test_format_jd_as_iso_date_time():
    """Test converting JD to ISO datetime"""
    jd = 2456000
    assert utils.format_jd_as_iso_date_time(jd) == "2012-03-13 12:00:00"

    return


def test_format_obsid_as_calendar_date():
    """Test converting obsid to calendar date"""
    assert utils.format_obsid_as_calendar_date(obsids[1]) == "2018-11-09"

    return
