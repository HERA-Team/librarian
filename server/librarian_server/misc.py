# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

from __future__ import absolute_import, division, print_function, unicode_literals

from flask import flash, redirect, render_template, url_for

from . import app, db
from .webutil import RPCError, json_api, login_required


@app.route ('/api/ping', methods=['GET', 'POST'])
@json_api
def ping (args, sourcename=None):
    return {'message': 'hello'}


@app.route ('/')
@login_required
def index ():
    from .file import File

    q = File.query.order_by (File.create_time.desc ()).limit (50)
    return render_template (
        'file-listing.html',
        title='Recent Files',
        files=q
    )


@app.route ('/create-database')
@login_required
def create_database ():
    if not app.config.get ('flask-debug', False):
        flash ('can only initialize database in debug mode!')
    else:
        from .observation import Observation
        from .file import File
        from .store import Store
        db.create_all ()
        db.session.add (Observation (1234, 2455555.5, 2455555.6, 12.))
        db.session.add (File ('demofile', 'fake', 1234, 'source', 0, 'md5sumhere'))
        db.session.add (Store ('pot1', '/pot1data', 'pot1.fake'))
        db.session.add (Store ('pot2', '/pot2data', 'pot2.fake'))
        db.session.commit ()

    return redirect (url_for ('index'))
