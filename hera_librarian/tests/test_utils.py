# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD License

"""Test code in hera_librarian/utils.py

"""

import pytest
import os
import six
import sys
from contextlib import contextmanager
from hera_librarian import utils

# import test data attributes from __init__.py
from . import ALL_FILES, obsids, filetypes, md5sums, pathsizes


# define a context manager for checking stdout
# from https://stackoverflow.com/questions/4219717/how-to-assert-output-with-nosetest-unittest-in-python
@contextmanager
def captured_output():
    new_out, new_err = six.StringIO(), six.StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err


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


@ALL_FILES
def test_get_obsid_from_path(datafiles):
    """Test extracting obsid values from datasets"""
    obsid_uvh5 = 1225829886
    obsid_miriad = 1192201262
    obsids = [obsid_uvh5, obsid_miriad]
    filepaths = list(map(str, datafiles.listdir()))
    # make sure our files are ordered correctly
    if filepaths[0].endswith("uvA"):
        filepaths = filepaths[::-1]
    for obsid, path in zip(obsids, filepaths):
        assert utils.get_obsid_from_path(path) == obsid

    return


def test_normalize_and_validate_md5():
    """Test md5sum normalization"""
    md5sum = "d41d8cd98f00b204e9800998ecf8427e"
    # function does not do anything for text already lowercase
    assert utils.normalize_and_validate_md5(md5sum) == md5sum

    md5sum_padded = md5sum + "   "
    assert utils.normalize_and_validate_md5(md5sum_padded) == md5sum

    md5sum_upper = md5sum.upper() + "   "
    assert utils.normalize_and_validate_md5(md5sum_upper) == md5sum

    # make sure error is raised when length is incorrect
    with pytest.raises(ValueError):
        utils.normalize_and_validate_md5(md5sum[:-1])

    return


@ALL_FILES
def test_md5_of_file(datafiles):
    """Test generating md5sum of file"""
    filepaths = list(map(str, datafiles.listdir()))
    # make sure our files are ordered correctly
    if filepaths[0].endswith("uvA"):
        filepaths = filepaths[::-1]
    assert utils._md5_of_file(filepaths[0]) == md5sums[0]

    return


@ALL_FILES
def test_get_md5_from_path(datafiles):
    """Test getting the md5sum for both a flat file and directory"""
    filepaths = list(map(str, datafiles.listdir()))
    # make sure our files are ordered correctly
    if filepaths[0].endswith("uvA"):
        filepaths = filepaths[::-1]
    # test normal execution
    for md5sum, path in zip(md5sums, filepaths):
        assert utils.get_md5_from_path(path) == md5sum

    # test adding funny bits to the ends of the directory names
    datafile_miriad = filepaths[1] + "//."
    assert utils.get_md5_from_path(datafile_miriad) == md5sums[1]

    return


@ALL_FILES
def test_get_size_from_path(datafiles):
    """Test computing filesize from path"""
    filepaths = list(map(str, datafiles.listdir()))
    # make sure our files are ordered correctly
    if filepaths[0].endswith("uvA"):
        filepaths = filepaths[::-1]
    for pathsize, path in zip(pathsizes, filepaths):
        assert utils.get_size_from_path(path) == pathsize

    return


@ALL_FILES
def test_gather_info_for_path(datafiles):
    """Test getting all info for a given path"""
    filepaths = list(map(str, datafiles.listdir()))
    # make sure our files are ordered correctly
    if filepaths[0].endswith("uvA"):
        filepaths = filepaths[::-1]
    for filetype, md5, size, obsid, path in zip(
        filetypes, md5sums, pathsizes, obsids, filepaths
    ):
        info = utils.gather_info_for_path(path)
        assert info["type"] == filetype
        assert info["md5"] == md5
        assert info["size"] == size
        assert info["obsid"] == obsid

    return


@ALL_FILES
def test_print_info_for_path(datafiles):
    """Test printing file info to stdout"""
    filepaths = list(map(str, datafiles.listdir()))
    # make sure our files are ordered correctly
    if filepaths[0].endswith("uvA"):
        filepaths = filepaths[::-1]
    for filetype, md5, size, obsid, path in zip(
        filetypes, md5sums, pathsizes, obsids, filepaths
    ):
        with captured_output() as (out, err):
            utils.print_info_for_path(path)
        output = out.getvalue()
        correct_string = '{{"obsid": {0:d}, "size": {1:d}, "type": "{2:}", "md5": "{3:}"}}'.format(
            obsid, size, filetype, md5
        )
        assert output == correct_string

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
    assert utils.format_obsid_as_calendar_date(obsids[0]) == "2018-11-09"

    return
