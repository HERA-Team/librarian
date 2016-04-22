# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"""This module contains various utilities that help with the Flask-y, web
interface end of things.

"""
from __future__ import absolute_import, division, print_function, unicode_literals

__all__ = str('''
AuthFailedError
ServerError
json_api
login_required
login
logout
''').split ()

from flask import Response, flash, redirect, render_template, request, session, url_for
import json, sys

from . import app


# Generic authentication stuff

class AuthFailedError (Exception):
    pass


def _check_authentication (auth):
    """`auth` is the provided authentication string.

    Currently, we operate in a sort of "username-free" mode, where only the
    authenticator is provided. At the moment we just return the name of the
    Source associated with the provided authenticator string, where these are
    defined in the 'sources' section of the server config file.

    If no matching source is found, we raise an exception. This means that if
    you forget to handle this case, the code aborts, which is more secure than
    proceeding along.

    """
    if auth is not None:
        for name, info in app.config['sources'].iteritems ():
            if info['authenticator'] == auth:
                return name

    raise AuthFailedError ()


# The RPC (Remote Procedure Call) interface

class ServerErrorBase (Exception):
    def __init__ (self, status, fmt, args):
        self.status = status

        if len (args):
            self.message = fmt % args
        else:
            self.message = str (fmt)


class ServerError (ServerErrorBase):
    """Raise this when an error is encountered in an API call. An error message
    will be returned, and the HTTP request will return error 400, which is
    usually what you want.

    """
    def __init__ (self, fmt, *args):
        super (ServerError, self).__init__ (400, fmt, args)


def _json_inner (f, **kwargs):
    if len (request.form):
        reqdata = request.form
    else:
        reqdata = request.args

    reqtext = reqdata.get ('request')
    if reqtext is None:
        raise ServerError ('no request payload provided')

    try:
        payload = json.loads (reqtext)
    except Exception as e:
        raise ServerError ('couldn\'t parse request payload: %s', e)

    if not isinstance (payload, dict):
        raise ServerError ('request payload is %s, not dictionary',
                           payload.__class__.__name__)

    auth = payload.pop ('authenticator', None)
    if auth is None:
        raise ServerError ('no authentication provided')

    try:
        sourcename = _check_authentication (auth)
    except AuthFailedError:
        raise ServerError ('authentication failed')

    result = f (payload, sourcename=sourcename, **kwargs)

    if not isinstance (result, dict):
        raise ServerError ('internal error: response is %s, not a dictionary',
                           result.__class__.__name__)

    if 'success' not in result:
        result['success'] = True # optimism!

    return result


from functools import wraps

def json_api (f):
    """This decorator wraps JSON API functions and does two things.

    First, it converts the input from JSON to Python data structures, and does
    the reverse with the return value of the function. The input is passed as
    as a "request" URL query argument or POST data.

    Second, it makes the the function require authentication in order to
    proceed. The authentication is mapped to a Source name, which is passed to
    the inner function as a "sourcename" keyword argument. The authenticator
    is passed as a string "authenticator" in the JSON request payload, the
    outermost level of which must be a dictionary.

    Arguments can be provided as either URL query arguments or POST data; the
    latter is preferred.

    See also login_required() below.

    """
    @wraps (f)
    def decorated_function (**kwargs):
        try:
            result = _json_inner (f, **kwargs)
            status = 200
        except ServerErrorBase as e:
            result = {
                'success': False,
                'message': e.message,
            }
            status = e.status
        except Exception as e:
            app.log_exception (sys.exc_info ())
            result = {
                'success': False,
                'message': 'internal exception: %s (details logged by server)' % e,
            }
            status = 400

        try:
            outtext = json.dumps (result)
        except Exception as e:
            result = {
                'success': False,
                'message': 'couldn\'t format response data: %s' % e
            }
            status = 400
            outtext = json.dumps (result)

        return Response (outtext, mimetype='application/json', status=status)

    return decorated_function


def _coerce (argtype, name, val):
    """For now, we do not silently promote any types. If this becomes a pain we
    can change that.

    """
    if argtype is int:
        if not isinstance (val, (int, long)):
            raise ServerError ('parameter "%s" should be an integer, but got %r', name, val)
        return val

    if argtype is unicode:
        if not isinstance (val, unicode):
            raise ServerError ('parameter "%s" should be text, but got %r', name, val)
        return val

    if argtype is float:
        if not isinstance (val, float):
            raise ServerError ('parameter "%s" should be a float, but got %r', name, val)
        return val

    if argtype is dict:
        if not isinstance (val, dict):
            raise ServerError ('parameter "%s" should be a dictionary, but got %r', name, val)
        return val

    if argtype is list:
        if not isinstance (val, dict):
            raise ServerError ('parameter "%s" should be a list, but got %r', name, val)
        return val

    raise ServerError ('internal bug: unexpected argument type %s', argtype)


def required_arg (args, argtype, name):
    """Helper for type checking in JSON API functions.

    Might regret this, but we accepts ints and silently promote them to
    floats.

    """
    val = args.get (name)
    if val is None:
        raise ServerError ('required parameter "%s" not provided', name)
    return _coerce (argtype, name, val)


def optional_arg (args, argtype, name):
    """Helper for type checking in JSON API functions.

    Might regret this, but we accepts ints and silently promote them to
    floats.

    """
    val = args.get (name)
    if val is None:
        return None
    return _coerce (argtype, name, val)


# Human user session handling

def login_required (f):
    @wraps (f)
    def decorated_function (*args, **kwargs):
        if 'sourcename' not in session:
            return redirect (url_for ('login', next=request.url))
        return f (*args, **kwargs)
    return decorated_function


@app.route ('/login', methods=['GET', 'POST'])
def login ():
    next = request.form.get ('next')
    if next is None:
        next = url_for ('index')

    if request.method == 'GET':
        return render_template ('login.html', next=next)

    # This is a POST request -- user is actually trying to log in.

    try:
        sourcename = _check_authentication (request.form.get ('auth'))
    except AuthFailedError:
        flash ('Login failed.')
        return render_template ('login.html', next=next)

    session['sourcename'] = sourcename
    return redirect (next)


@app.route ('/logout')
def logout ():
    session.pop ('sourcename', None)
    return redirect (url_for ('index'))
