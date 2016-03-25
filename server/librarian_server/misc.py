# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

from __future__ import absolute_import, division, print_function, unicode_literals

from flask import (Response, escape, flash, redirect, render_template,
                   request, session, url_for)
import datetime, json, os, sys


from . import app, db


# The database. We start with abstract tables recording properties of
# observations, etc., that don't have anything to do with stored data of any
# kind ...

NotNull = lambda kind: db.Column (kind, nullable=False)


class Observation (db.Model):
    __tablename__ = 'observation'

    obsid = db.Column (db.Integer, primary_key=True)
    start_time_jd = NotNull (db.Float)
    start_time_jd = NotNull (db.Float)
    start_lst_hr = NotNull (db.Float)


# Now, the data structures that actually record files having to do with the
# above information. Notably, we require that every file be associated with an
# obsid.

class Store (db.Model):
    __tablename__ = 'store'

    id = db.Column (db.Integer, primary_key=True)
    name = NotNull (db.String (256))
    path_prefix = NotNull (db.String (256))
    ssh_host = NotNull (db.String (256))
    http_prefix = db.Column (db.String (256))
    available = NotNull (db.Boolean)

    def __init__ (self, name, path_prefix, ssh_host):
        self.name = name
        self.path_prefix = path_prefix
        self.ssh_host = ssh_host
        self.available = True


class File (db.Model):
    __tablename__ = 'file'

    name = db.Column (db.String (256), primary_key=True)
    type = NotNull (db.String (32))
    create_time = NotNull (db.DateTime)
    obsid = db.Column (db.Integer, db.ForeignKey (Observation.obsid), nullable=False)
    source = NotNull (db.String (64))
    size = NotNull (db.Integer)
    md5 = NotNull (db.String (32))

    def __init__ (self, name, type, obsid, source, size, md5, create_time=None):
        if create_time is None:
            create_time = datetime.datetime.now ()

        self.name = name
        self.type = type
        self.create_time = create_time
        self.obsid = obsid
        self.source = source
        self.size = size
        self.md5 = md5


class FileInstance (db.Model):
    __tablename__ = 'file_instance'

    store = db.Column (db.Integer, db.ForeignKey (Store.id), primary_key=True)
    parent_dirs = db.Column (db.String (128), primary_key=True)
    name = db.Column (db.String (256), db.ForeignKey (File.name), primary_key=True)




# Framework for the JSON API

class AuthFailedError (Exception):
    pass


def check_authentication (auth):
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


class RPCErrorBase (Exception):
    def __init__ (self, status, fmt, args):
        self.status = status

        if len (args):
            self.message = fmt % args
        else:
            self.message = str (fmt)


class RPCError (RPCErrorBase):
    """Raise this when an error is encountered in an API call. An error message
    will be returned, and the HTTP request will return error 400, which is
    usually what you want.

    """
    def __init__ (self, fmt, *args):
        super (RPCError, self).__init__ (400, fmt, args)


def _json_inner (f, **kwargs):
    if len (request.form):
        reqdata = request.form
    else:
        reqdata = request.args

    reqtext = reqdata.get ('request')
    if reqtext is None:
        raise RPCError ('no request payload provided')

    try:
        payload = json.loads (reqtext)
    except Exception as e:
        raise RPCError ('couldn\'t parse request payload: %s', e)

    if not isinstance (payload, dict):
        raise RPCError ('request payload is %s, not dictionary',
                        payload.__class__.__name__)

    auth = payload.pop ('authenticator', None)
    if auth is None:
        raise RPCError ('no authentication provided')

    try:
        sourcename = check_authentication (auth)
    except AuthFailedError:
        raise RPCError ('authentication failed')

    result = f (payload, sourcename=sourcename, **kwargs)

    if not isinstance (result, dict):
        raise RPCError ('internal error: response is %s, not a dictionary',
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
        except RPCErrorBase as e:
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


# Actual RPC calls!

@app.route ('/api/ping', methods=['GET', 'POST'])
@json_api
def ping (args, sourcename=None):
    return {'message': 'hello'}


@app.route ('/api/recommended_store', methods=['GET', 'POST'])
@json_api
def recommended_store (args, sourcename=None):
    file_size = args.pop ('file_size', None)
    if not isinstance (file_size, int) or not file_size >= 0:
        raise RPCError ('illegal file_size argument')

    raise RPCError ('not yet implemented')


# User session handling

def login_required (f):
    # See also auth_required() above.
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
        sourcename = check_authentication (request.form.get ('auth'))
    except AuthFailedError:
        flash ('Login failed.')
        return render_template ('login.html', next=next)

    session['sourcename'] = sourcename
    return redirect (next)


@app.route ('/logout')
def logout ():
    session.pop ('sourcename', None)
    return redirect (url_for ('index'))


# The human-aimed web interface!

@app.route ('/')
@login_required
def index ():
    q = File.query.order_by (File.create_time.desc ()).limit (50)
    return render_template (
        'filelisting.html',
        title='Recent Files',
        files=q
    )


@app.route ('/create-database')
@login_required
def create_database ():
    db.create_all ()
    db.session.add (File ('demofile', 'fake', 1, 'source', 0, 'md5sumhere'))
    db.session.commit ()
    return redirect (url_for ('index'))
