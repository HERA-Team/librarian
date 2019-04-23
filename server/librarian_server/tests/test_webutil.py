# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD License

"""Test code in librarian_server/webutil.py

"""

from __future__ import print_function, division, absolute_import, unicode_literals
import pytest

from librarian_server import webutil
from librarian_server.webutil import ServerError, AuthFailedError


# mark these tests as "librarian_server" tests
pytestmark = pytest.mark.librarian_server


def test_check_authentication():
    # test authenticating with given settings
    assert webutil._check_authentication("I am a human") == "HumanUser"

    # test rasing an error
    with pytest.raises(AuthFailedError):
        webutil._check_authentication("bogus phrase")

    return


def test_coerce():
    # test coercion of different types
    assert webutil._coerce(bool, "bool_var", True) is True
    assert webutil._coerce(int, "int_var", 7) == 7
    assert (
        webutil._coerce(unicode, "unicode_var", "this is unicode") == "this is unicode"
    )
    assert webutil._coerce(float, "float_var", 1.0) == 1.0
    assert webutil._coerce(dict, "dict_var", {"key": "val"}) == {"key": "val"}
    assert webutil._coerce(list, "list_var", [0, 1, 2]) == [0, 1, 2]

    # test raising errors
    with pytest.raises(ServerError):
        webutil._coerce(bool, "bool_var", "foo")
    with pytest.raises(ServerError):
        webutil._coerce(int, "int_var", "foo")
    with pytest.raises(ServerError):
        webutil._coerce(unicode, "unicode_var", 7)
    with pytest.raises(ServerError):
        webutil._coerce(float, "float_var", "foo")
    with pytest.raises(ServerError):
        webutil._coerce(dict, "dict_var", "foo")
    with pytest.raises(ServerError):
        webutil._coerce(list, "list_var", "foo")
    with pytest.raises(ServerError):
        webutil._coerce("foo", "weird_argtype", "bar")

    return
