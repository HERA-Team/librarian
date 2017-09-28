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
''').split()

from flask import Response, flash, redirect, render_template, request, session, url_for
import json
import os
import sys

from . import app, logger


# Generic authentication stuff

class AuthFailedError (Exception):
    pass


def _check_authentication(auth):
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
        for name, info in app.config['sources'].iteritems():
            if info['authenticator'] == auth:
                return name

    raise AuthFailedError()


# The RPC (Remote Procedure Call) interface

class ServerErrorBase (Exception):
    def __init__(self, status, fmt, args):
        self.status = status

        if len(args):
            self.message = fmt % args
        else:
            self.message = str(fmt)

    def __str__(self):
        return self.message


class ServerError (ServerErrorBase):
    """Raise this when an error is encountered in an API call. An error message
    will be returned, and the HTTP request will return error 400, which is
    usually what you want.

    """

    def __init__(self, fmt, *args):
        super(ServerError, self).__init__(400, fmt, args)


def _json_inner(f, **kwargs):
    if len(request.form):
        reqdata = request.form
    else:
        reqdata = request.args

    reqtext = reqdata.get('request')
    if reqtext is None:
        raise ServerError('no request payload provided')

    try:
        payload = json.loads(reqtext)
    except Exception as e:
        raise ServerError('couldn\'t parse request payload: %s', e)

    if not isinstance(payload, dict):
        raise ServerError('request payload is %s, not dictionary',
                          payload.__class__.__name__)

    auth = payload.pop('authenticator', None)
    if auth is None:
        raise ServerError('no authentication provided')

    try:
        sourcename = _check_authentication(auth)
    except AuthFailedError:
        raise ServerError('authentication failed')

    result = f(payload, sourcename=sourcename, **kwargs)

    if not isinstance(result, dict):
        raise ServerError('internal error: response is %s, not a dictionary',
                          result.__class__.__name__)

    if 'success' not in result:
        result['success'] = True  # optimism!

    return result


from functools import wraps


def json_api(f):
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
    @wraps(f)
    def decorated_function(**kwargs):
        try:
            result = _json_inner(f, **kwargs)
            status = 200
        except ServerErrorBase as e:
            result = {
                'success': False,
                'message': e.message,
            }
            status = e.status
        except Exception as e:
            app.log_exception(sys.exc_info())

            # I'm not sure what log_exception() does, but it doesn't seem to
            # print a traceback to stderr, which is helpful.
            import traceback
            traceback.print_exc(file=sys.stderr)

            result = {
                'success': False,
                'message': 'internal exception: %s (details logged by server)' % e,
            }
            status = 400

        try:
            outtext = json.dumps(result)
        except Exception as e:
            result = {
                'success': False,
                'message': 'couldn\'t format response data: %s' % e
            }
            status = 400
            outtext = json.dumps(result)

        return Response(outtext, mimetype='application/json', status=status)

    return decorated_function


def _coerce(argtype, name, val):
    """For now, we do not silently promote any types. If this becomes a pain we
    can change that.

    """
    if argtype is bool:
        if not isinstance(val, bool):
            raise ServerError('parameter "%s" should be a boolean, but got %r', name, val)
        return val

    if argtype is int:
        if not isinstance(val, (int, long)):
            raise ServerError('parameter "%s" should be an integer, but got %r', name, val)
        return val

    if argtype is unicode:
        if not isinstance(val, unicode):
            raise ServerError('parameter "%s" should be text, but got %r', name, val)
        return val

    if argtype is float:
        if not isinstance(val, float):
            raise ServerError('parameter "%s" should be a float, but got %r', name, val)
        return val

    if argtype is dict:
        if not isinstance(val, dict):
            raise ServerError('parameter "%s" should be a dictionary, but got %r', name, val)
        return val

    if argtype is list:
        if not isinstance(val, dict):
            raise ServerError('parameter "%s" should be a list, but got %r', name, val)
        return val

    raise ServerError('internal bug: unexpected argument type %s', argtype)


def required_arg(args, argtype, name):
    """Helper for type checking in JSON API functions.

    Might regret this, but we accepts ints and silently promote them to
    floats.

    """
    val = args.get(name)
    if val is None:
        raise ServerError('required parameter "%s" not provided', name)
    return _coerce(argtype, name, val)


def optional_arg(args, argtype, name, default=None):
    """Helper for type checking in JSON API functions.

    Might regret this, but we accepts ints and silently promote them to
    floats.

    """
    val = args.get(name)
    if val is None:
        return default
    return _coerce(argtype, name, val)


# Human user session handling

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'sourcename' not in session:
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function


@app.route('/login', methods=['GET', 'POST'])
def login():
    if len(request.form):
        reqdata = request.form  # POST
    else:
        reqdata = request.args  # GET

    next = reqdata.get('next')
    if next is None:
        next = url_for('index')

    if request.method == 'GET':
        return render_template('login.html', next=next)

    # This is a POST request -- user is actually trying to log in.

    try:
        sourcename = _check_authentication(request.form.get('auth'))
    except AuthFailedError:
        flash('Login failed.')
        return render_template('login.html', next=next)

    session['sourcename'] = sourcename
    return redirect(next)


@app.route('/logout')
def logout():
    session.pop('sourcename', None)
    return redirect(url_for('index'))


# Streaming of data through the tornado asynchronous API

from tornado import gen, iostream, web


class StreamFile (web.RequestHandler):
    uri_prefix = '/stream/'

    @gen.coroutine
    def get(self):
        if not self.request.uri.startswith(self.uri_prefix):
            self.clear()
            self.set_status(500)
            self.finish('internal server error: bad URI prefix')
            return

        file_name = self.request.uri[len(self.uri_prefix):]

        # Find an instance

        from .file import FileInstance
        inst = FileInstance.query.filter(FileInstance.name == file_name).first()
        if inst is None:
            self.clear()
            self.set_status(404)
            self.finish('no file named "%s" available at this Librarian' % file_name)
            return

        # Get an SSH-based process that will stream data to us.

        proc = inst.store_object._stream_path(inst.store_path)

        # And now forward all of the data to the caller. We sniff the first batch
        # to set the right Content-Type.

        first = True

        try:
            stream = iostream.PipeIOStream(os.dup(proc.stdout.fileno()))

            while True:
                try:
                    data = yield stream.read_bytes(4096, partial=True)
                except iostream.StreamClosedError as e:
                    break

                if first:
                    ctype = 'text/plain'
                    if data.startswith(b'\x89PNG\x0d\x0a\x1a\x0a'):
                        ctype = 'image/png'
                    elif len(data) > 260 and data[257:].startswith(b'ustar'):
                        ctype = 'application/tar'
                        # Bonus: we auto-tar directories, so it's helpful to
                        # tweak the filename to reflect that fact. Github
                        # issue #19. We also try to have an ASCII-only name,
                        # although probably other things will break if the
                        # name isn't ASCII anyway.
                        ret_name = file_name
                        if not ret_name.endswith('.tar'):
                            ret_name += '.tar'
                        ret_name = ret_name.encode('ascii', 'replace').replace('?', '_')
                        self.set_header('Content-disposition', 'attachment; filename=' + ret_name)

                    self.set_header('Content-Type', ctype)
                    first = False

                self.write(data)

            stream.close()
            proc.wait()

            if proc.returncode != 0:
                try:
                    msg = proc.stderr.read()
                except Exception as e:
                    msg = '(could not fetch error output)'
                raise Exception('streaming proxy exited with error code %d: %s' %
                                (proc.returncode, msg))
        except Exception as e:
            self.clear()
            self.set_status(503)
            self.write(str(e))
        finally:
            self.finish()
