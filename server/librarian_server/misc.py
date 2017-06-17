# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

from __future__ import absolute_import, division, print_function, unicode_literals

__all__ = str('''
create_records
gather_records
''').split()

from flask import flash, redirect, render_template, url_for

from . import app, db
from .webutil import ServerError, json_api, login_required, optional_arg, required_arg


def gather_records(file):
    """Gather up the set of database records that another Librarian will need if
    we're to upload *file* to it.

    Right now this function is fairly simple because the relevant code only
    works with one file at a time. You could imagine extending this function
    to take a list of files and gather all of the records at once, which might
    lead to a little bit less redundancy.

    """
    info = {}
    info['files'] = {file.name: file.to_dict()}

    obs = file.observation
    info['observations'] = {obs.obsid: obs.to_dict()}

    sess = obs.session
    if sess is not None:
        info['sessions'] = {sess.id: sess.to_dict()}

    return info


def create_records(info, sourcename):
    """Create database records for various items that should be synchronized among
    librarians. Various actions, such as file upload, cause records to be
    synchronized from one Librarian to another; this function implements the
    record-receiving end.

    """
    from .observation import ObservingSession, Observation
    from .file import File

    for subinfo in info.get('sessions', {}).itervalues():
        obj = ObservingSession.from_dict(subinfo)
        db.session.merge(obj)

    for subinfo in info.get('observations', {}).itervalues():
        obj = Observation.from_dict(subinfo)
        db.session.merge(obj)

    for subinfo in info.get('files', {}).itervalues():
        obj = File.from_dict(sourcename, subinfo)
        db.session.merge(obj)

    db.session.commit()


# Misc ...

@app.template_filter('strftime')
def _jinja2_filter_datetime(unixtime, fmt=None):
    import time
    return time.strftime('%c', time.localtime(unixtime))


@app.template_filter('duration')
def _jinja2_filter_duration(seconds, fmt=None):
    if seconds < 90:
        return '%.0f seconds' % seconds
    if seconds < 4000:
        return '%.1f minutes' % (seconds / 60)
    if seconds < 100000:
        return '%.1f hours' % (seconds / 3600)
    return '%.1f days' % (seconds / 86400)


@app.context_processor
def inject_current_time_info():
    import datetime, dateutil.tz, pytz

    utc = datetime.datetime.now(tz=pytz.utc)
    sa_tz = pytz.timezone('Africa/Johannesburg')
    sa = utc.astimezone(sa_tz)
    local_tz = dateutil.tz.tzlocal()
    local = utc.astimezone(local_tz)

    cti = utc.strftime('%Y-%m-%d %H:%M') + ' (UTC) • ' + sa.strftime('%H:%M (%Z)')

    if local.tzname() not in ('UTC', sa.tzname()):
        cti += ' • ' + local.strftime('%H:%M (%Z)')

    return {'current_time_info': cti}


# JSON API

@app.route('/api/ping', methods=['GET', 'POST'])
@json_api
def ping(args, sourcename=None):
    return {'message': 'hello'}


# Web UI

@app.route('/')
@login_required
def index():
    from .observation import ObservingSession
    from .file import File

    rs = ObservingSession.query.order_by(ObservingSession.start_time_jd.desc()).limit(7)
    rf = File.query.order_by(File.create_time.desc()).limit(50)
    return render_template(
        'main-page.html',
        title='Librarian Homepage',
        recent_files=rf,
        recent_sessions=rs,
    )
