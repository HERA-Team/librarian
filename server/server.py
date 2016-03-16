#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

from __future__ import absolute_import, division, print_function, unicode_literals

from flask import Flask, escape, flash, redirect, render_template, request, session, url_for
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
    capacity = db.Column (db.Integer, nullable=False)
    used_librarian = db.Column (db.Integer, nullable=False)
    used_other = db.Column (db.Integer, nullable=False)
    path_prefix = db.Column (db.String (256), nullable=False)
    ssh_prefix = db.Column (db.String (256), nullable=False)
    http_prefix = db.Column (db.String (256))
    available = db.Column (db.Boolean, nullable=False)

    def __init__ (self, name, capacity, path_prefix, ssh_prefix):
        self.name = name
        self.capacity = capacity
        self.path_prefix = path_prefix
        self.ssh_prefix = ssh_prefix


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


# User session handling

from functools import wraps

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
    auth = request.form.get ('auth')
    sourcename = None

    if auth is not None:
        for name, info in app.config['sources'].iteritems ():
            if info['authenticator'] == auth:
                sourcename = name
                break

    if sourcename is not None: # did we match anything?
        session['sourcename'] = sourcename
        return redirect (next)

    flash ('Login failed.')
    return render_template ('login.html', next=next)


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
