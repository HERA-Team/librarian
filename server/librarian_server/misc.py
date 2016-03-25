# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

from __future__ import absolute_import, division, print_function, unicode_literals

from flask import (Response, escape, flash, redirect, render_template,
                   request, session, url_for)
import datetime, json, os, sys


from . import app, db
from .webutil import RPCError, json_api, login_required
from .dbutil import NotNull


# The database. We start with abstract tables recording properties of
# observations, etc., that don't have anything to do with stored data of any
# kind ...

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
