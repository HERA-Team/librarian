# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD License

"""Test code in librarian_server/store.py

"""


import pytest
import urllib.request, urllib.error, urllib.parse

from . import ALL_FILES, filetypes, obsids, md5sums, pathsizes
from librarian_server import webutil
from librarian_server.webutil import AuthFailedError, ServerError


@ALL_FILES
def test_initiate_upload():
    # test uploading a datafile
    pass
