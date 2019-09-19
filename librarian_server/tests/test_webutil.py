# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD License

"""Test code in librarian_server/webutil.py

"""

from __future__ import print_function, division, absolute_import, unicode_literals
import pytest
import numpy as np
import json
import urllib

from librarian_server import webutil
from librarian_server.webutil import AuthFailedError, ServerError, json_api


def test_check_authentication():
    # test authenticating with given settings
    assert webutil._check_authentication("I am a human") == "HumanUser"

    # test rasing an error
    with pytest.raises(AuthFailedError):
        webutil._check_authentication("bogus phrase")

    return


def test_json_api(db_connection):
    logger, app, db = db_connection

    @app.route("/")
    @json_api
    def index(args, sourcename=None):
        return {"val1": 1, "val2": "my_string"}

    c = app.test_client()
    mydict = {"val1": 1, "val2": "my_string", "authenticator": "I am a bot"}
    json_dict = json.dumps(mydict)
    req_dict = {"request": json_dict}
    req_url = urllib.urlencode(req_dict)
    r = c.get("/?" + req_url)
    assert r.status_code == 200
    outdict = json.loads(r.data)
    assert outdict["success"] == True
    assert outdict["val1"] == 1
    assert outdict["val2"] == "my_string"


def test_json_api_errors(db_connection):
    logger, app, db = db_connection

    @app.route("/test_error_func/")
    @json_api
    def test_error_func(args, sourcename=None):
        return {"val1": 1, "val2": "my_string"}

    @app.route("/invalid_return/")
    @json_api
    def invalid_return(args, sourcename=None):
        return ["list", "not", "dict"]

    @app.route("/ndarray_return/")
    @json_api
    def ndarray_return(args, sourcename=None):
        return {"array": np.asarray([0, 1, 2])}

    # test some server errors
    c = app.test_client()

    # test not having an authenticator
    mydict = {"val1": 1, "val2": "my_string"}
    json_dict = json.dumps(mydict)
    req_dict = {"request": json_dict}
    req_url = urllib.urlencode(req_dict)
    r = c.get("/test_error_func/?" + req_url)
    assert r.status_code == 400
    outdict = json.loads(r.data)
    assert outdict["success"] == False
    assert outdict["message"] == "no authentication provided"

    # test having a bad authenticator
    mydict["authenticator"] = "bad string"
    json_dict = json.dumps(mydict)
    req_dict = {"request": json_dict}
    req_url = urllib.urlencode(req_dict)
    r = c.get("/test_error_func/?" + req_url)
    assert r.status_code == 400
    outdict = json.loads(r.data)
    assert outdict["success"] == False
    assert outdict["message"] == "authentication failed"

    # test having no payload
    req_url = urllib.urlencode({})
    r = c.get("/test_error_func/?" + req_url)
    assert r.status_code == 400
    outdict = json.loads(r.data)
    assert outdict["success"] == False
    assert outdict["message"] == "no request payload provided"

    # make the payload invalid json
    req_url = urllib.urlencode({"request": "bad_format"})
    r = c.get("/test_error_func/?" + req_url)
    assert r.status_code == 400
    outdict = json.loads(r.data)
    assert outdict["success"] == False
    assert outdict["message"] == (
        "couldn't parse request payload: No JSON object could be decoded"
    )

    # use a function that doesn't return a dict
    mydict["authenticator"] = "I am a bot"
    json_dict = json.dumps(mydict)
    req_dict = {"request": json_dict}
    req_url = urllib.urlencode(req_dict)
    r = c.get("/invalid_return/?" + req_url)
    assert r.status_code == 400
    outdict = json.loads(r.data)
    assert outdict["success"] == False
    assert outdict["message"] == "internal error: response is list, not a dictionary"

    # use a function that returns a dict that can't be encoded in json
    r = c.get("/ndarray_return/?" + req_url)
    assert r.status_code == 400
    outdict = json.loads(r.data)
    assert outdict["success"] == False
    assert outdict["message"] == (
        "couldn't format response data: array([0, 1, 2]) is not JSON serializable"
    )

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


def test_required_arg():
    args = {"arg1": 1, "arg2": u"my_string"}
    arg1 = webutil.required_arg(args, int, "arg1")
    assert arg1 == 1
    arg2 = webutil.required_arg(args, unicode, "arg2")
    assert arg2 == u"my_string"

    with pytest.raises(ServerError):
        arg3 = webutil.required_arg(args, int, "arg3")

    return


def test_optional_arg():
    args = {"arg1": 1, "arg2": u"my_string"}
    arg1 = webutil.optional_arg(args, int, "arg1")
    assert arg1 == 1
    arg2 = webutil.optional_arg(args, unicode, "arg2")
    assert arg2 == u"my_string"

    arg3 = webutil.optional_arg(args, int, "arg3", 7)
    assert arg3 == 7

    return
