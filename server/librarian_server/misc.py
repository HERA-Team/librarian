# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

from __future__ import absolute_import, division, print_function, unicode_literals

__all__ = str ('''
create_records
gather_records
''').split ()

from flask import flash, redirect, render_template, url_for

from . import app, db
from .webutil import ServerError, json_api, login_required, optional_arg, required_arg


def gather_records (file):
    """Gather up the set of database records that another Librarian will need if
    we're to upload *file* to it.

    Right now this function is fairly simple because the relevant code only
    works with one file at a time. You could imagine extending this function
    to take a list of files and gather all of the records at once, which might
    lead to a little bit less redundancy.

    """
    info = {}
    info['files'] = {file.name: file.to_dict ()}

    obs = file.observation
    info['observations'] = {obs.obsid: obs.to_dict ()}

    sess = obs.session
    if sess is not None:
        info['sessions'] = {sess.id: sess.to_dict ()}

    return info


def create_records (info, sourcename):
    """Create database records for various items that should be synchronized among
    librarians. Various actions, such as file upload, cause records to be
    synchronized from one Librarian to another; this function implements the
    record-receiving end.

    """
    from .observation import ObservingSession, Observation
    from .file import File

    for subinfo in info.get ('sessions', {}).itervalues ():
        obj = ObservingSession.from_dict (subinfo)
        db.session.merge (obj)

    for subinfo in info.get ('observations', {}).itervalues ():
        obj = Observation.from_dict (subinfo)
        db.session.merge (obj)

    for subinfo in info.get ('files', {}).itervalues ():
        obj = File.from_dict (sourcename, subinfo)
        db.session.merge (obj)

    db.session.commit ()


# Misc ...

@app.template_filter('strftime')
def _jinja2_filter_datetime (unixtime, fmt=None):
    import time
    return time.strftime ('%c', time.localtime (unixtime))


@app.template_filter('duration')
def _jinja2_filter_duration (seconds, fmt=None):
    if seconds < 90:
        return '%.0f seconds' % seconds
    if seconds < 4000:
        return '%.1f minutes' % (seconds / 60)
    if seconds < 100000:
        return '%.1f hours' % (seconds / 3600)
    return '%.1f days' % (seconds / 86400)


# JSON API

@app.route ('/api/ping', methods=['GET', 'POST'])
@json_api
def ping (args, sourcename=None):
    return {'message': 'hello'}


# Web UI

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
        from .file import File, FileInstance
        from .store import Store
        db.create_all ()
        db.session.add (Observation (1234, 2455555.5, 2455555.6, 12.))
        db.session.add (File ('demofile', 'fake', 1234, 'source', 0, 'md5sumhere'))
        store1 = Store ('pot1', '/pot1data', 'pot1.fake')
        db.session.add (store1)
        db.session.add (Store ('pot2', '/pot2data', 'pot2.fake'))
        db.session.commit ()
        db.session.add (FileInstance (store1, '2455555', 'demofile'))
        db.session.commit ()

    return redirect (url_for ('index'))
