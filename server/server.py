#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

from __future__ import absolute_import, division, print_function, unicode_literals

from flask import (Flask, Response, escape, flash, redirect, render_template,
                   request, session, url_for)
import datetime, json, sys


# In order to have the config loaded up in time to affect the database setup,
# we need to load the config here at the top of the file :-(

with open ('server-config.json') as f:
    config = json.load (f)

if 'SECRET_KEY' not in config:
    print ('cannot start server: must define the Flask "secret key" as the item '
           '"SECRET_KEY" in "server-config.json"', file=sys.stderr)
    sys.exit (1)

app = Flask ('librarian')
app.config.update (config)


# The database. Folks mildly encourage you to definite it in a separate Python
# module, but it's intended to be internal-only so let's start with it here.

from flask_sqlalchemy import SQLAlchemy
db = SQLAlchemy (app)


class Store (db.Model):
    id = db.Column (db.Integer, primary_key=True)
    name = db.Column (db.String (256), nullable=False)
    path_prefix = db.Column (db.String (256), nullable=False)
    ssh_host = db.Column (db.String (256), nullable=False)
    http_prefix = db.Column (db.String (256))
    available = db.Column (db.Boolean, nullable=False)

    def __init__ (self, name, path_prefix, ssh_host):
        self.name = name
        self.path_prefix = path_prefix
        self.ssh_host = ssh_host
        self.available = True


class File (db.Model):
    name = db.Column (db.String (256), primary_key=True)
    type = db.Column (db.String (32), nullable=False)
    create_time = db.Column (db.DateTime, nullable=False)
    obsid = db.Column (db.Integer, nullable=False)
    source = db.Column (db.String (64), nullable=False)
    size = db.Column (db.Integer, nullable=False)
    md5 = db.Column (db.String (32), nullable=False)

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


# Framework for the JSON API

class AuthFailedError (Exception):
    pass


def check_authentication (auth):
    """`auth` is the provided authentication string.

    Currently, we operate in a sort of "username-free" mode, where only the
    authenticator is provided. We at the moment we just return the name of the
    Source associated with the provided authenticator string.

    If no matching source is found, we raise an exception. This means that if
    you forget to handle this case, the code aborts, which is more secure than
    proceeding along.

    """
    if auth is not None:
        for name, info in app.config['sources'].iteritems ():
            if info['authenticator'] == auth:
                return name

    raise AuthFailedError ()


class APIErrorBase (Exception):
    def __init__ (self, status, fmt, args):
        self.status = status

        if len (args):
            self.message = fmt % args
        else:
            self.message = str (fmt)


class APIError (APIErrorBase):
    """Raise this when an error is encountered in an API call. An error message
    will be returned, and the HTTP request will return error 400, which is
    usually what you want.

    """
    def __init__ (self, fmt, *args):
        super (APIError, self).__init__ (400, fmt, args)


def _json_inner (f, **kwargs):
    if len (request.form):
        reqdata = request.form
    else:
        reqdata = request.args

    reqtext = reqdata.get ('request')
    if reqtext is None:
        raise APIError ('no request payload provided')

    try:
        payload = json.loads (reqtext)
    except Exception as e:
        raise APIError ('couldn\'t parse request payload: %s', e)

    if not isinstance (payload, dict):
        raise APIError ('request payload is %s, not dictionary',
                        payload.__class__.__name__)

    auth = payload.pop ('authenticator', None)
    if auth is None:
        raise APIError ('no authentication provided')

    try:
        sourcename = check_authentication (auth)
    except AuthFailedError:
        raise APIError ('authentication failed')

    result = f (payload, sourcename=sourcename, **kwargs)

    if not isinstance (result, dict):
        raise APIError ('internal error: response is %s, not a dictionary',
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
        except APIErrorBase as e:
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


# Actual API calls!

@app.route ('/api/ping', methods=['GET', 'POST'])
@json_api
def ping (args, sourcename=None):
    return {'message': 'hello'}


@app.route ('/api/recommended_store', methods=['GET', 'POST'])
@json_api
def recommended_store (args, sourcename=None):
    file_size = args.pop ('file_size', None)
    if not isinstance (file_size, int) or not file_size >= 0:
        raise APIError ('illegal file_size argument')

    raise APIError ('not yet implemented')


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


# The meat of the app

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


# Finally, the short-n-sweet command-line driver:

def commandline (argv):
    debug = app.config.get ('flask-debug', False)
    app.run (debug=debug)


if __name__ == '__main__':
    import sys
    commandline (sys.argv)
