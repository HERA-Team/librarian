# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

from __future__ import absolute_import, division, print_function, unicode_literals

__all__ = str('''
create_records
gather_records
''').split()

from flask import flash, redirect, render_template, url_for
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound

from . import app, db
from .dbutil import SQLAlchemyError
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

    if file.obsid is not None:
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

    from .mc_integration import is_file_record_invalid, note_file_created

    for subinfo in info.get('files', {}).itervalues():
        obj = File.from_dict(sourcename, subinfo)

        # Things get slightly more complicated here because if we're linked in
        # to HERA M&C, we need to check if files are valid, and report when
        # new File records are created. I don't think `merge()` gives us any
        # reasonable path to do that, so we need a hand-rolled UPSERT to
        # figure out what happened. Fortunately the primary key of the File
        # table is simple, and file records are immutable, so we don't need to
        # get too tricky. Cf.
        # https://stackoverflow.com/questions/2546207/does-sqlalchemy-have-an-equivalent-of-djangos-get-or-create
        #
        # Note, however, that only the Karoo Librarian has M&C integration,
        # and that's the one Librarian that it is unlikely that anyone is ever
        # going to upload a file *to*, which is how this code path gets
        # activated. But let's be thorough.

        if is_file_record_invalid(obj):
            raise ServerError('new file %s (obsid %s) rejected by M&C; see M&C error logs for the reason',
                              obj.name, obj.obsid)

        try:
            db.session.query(File).filter_by(name=obj.name).one()
        except NoResultFound:
            try:
                db.session.add(obj)
                db.session.flush()
            except IntegrityError:
                db.session.rollback()
            else:
                note_file_created(obj)

    try:
        db.session.commit()
    except SQLAlchemyError:
        import sys
        db.session.rollback()
        app.log_exception(sys.exc_info())
        raise ServerError('failed to commit records to database; see logs for details')


# Verrry miscellaneous.

def ensure_dirs_gw(path, _parent_mode=False):
    """Ensure that path is a directory by creating it and parents if
    necessary.  Also ensure that it is group-writeable and
    -executable, and setgid. All created parent directories get their
    mode bits set comparably.

    This is a fairly specialized function used in support of the NRAO
    Lustre staging feature.

    Implementation copied from os.makedirs() with some tweaks.

    """
    import os.path
    import stat

    head, tail = os.path.split(path)  # /a/b/c => /a/b, c
    if not len(tail):  # if we got something like "a/b/" => ("a/b", "")
        head, tail = os.path.split(head)
    if len(head) and head != '/':
        ensure_dirs_gw(head, _parent_mode=True)

    try_chmod = not _parent_mode  # deepest directory must be g+wxs

    try:
        # Note: the `mode` passed to mkdir is altered by the umask, which may
        # remove the group-write bit we want, so we can't rely on it to set
        # permissions correctly.
        os.mkdir(path)
        try_chmod = True  # we created it, so definitely chmod
    except OSError as e:
        if e.errno == 17:
            pass  # already exists; no problem, and maybe no chmod
        elif e.errno == 13:  # EACCES
            raise Exception('unable to create directory \"%s\"; you probably '
                            'need to make its parent group-writeable with:\n\n'
                            'chmod g+wx \'%s\'' % (path, os.path.dirname(path)))
        else:
            raise

    if try_chmod:
        st = os.stat(path)
        mode = stat.S_IMODE(st.st_mode)
        new_mode = mode | (stat.S_IWUSR | stat.S_IWGRP | stat.S_IXUSR | stat.S_IXGRP | stat.S_ISGID)

        if new_mode != mode:  # avoid failure if perms are OK but we don't own the dir
            try:
                os.chmod(path, new_mode)
            except OSError as e:
                if e.errno == 1:  # EPERM
                    raise Exception('unable to make \"%s\" group-writeable; '
                                    'please do so yourself with:\n\nchmod g+wx \'%s\''
                                    % (path, path))
                raise


def copyfiletree(src, dst):
    """Something like shutil.copytree that just copies data, not mode
    bits, and that will accept either a file or a directory as input.

    *dst* may not be the name of containing directory. It must be the
    name where the data in *src* are intended to land.

    As a special hack, we make the new files and directories
    group-writeable, since this function is used at NRAO when staging
    data to Lustre and otherwise users can't actually modify the
    files/dirs created for them, which is super annoying.

    """
    import os.path
    from shutil import copyfile
    import stat

    try:
        items = os.listdir(src)
    except OSError as e:
        if e.errno == 20:  # not a directory?
            copyfile(src, dst)
            st = os.stat(dst)  # NOTE! not src; we explicitly do not preserve perms
            mode = stat.S_IMODE(st.st_mode)
            mode |= (stat.S_IWUSR | stat.S_IWGRP)
            os.chmod(dst, mode)
            return
        raise

    os.mkdir(dst)
    st = os.stat(dst)  # NOTE! not src; we explicitly do not preserve perms
    mode = stat.S_IMODE(st.st_mode)
    mode |= (stat.S_IWUSR | stat.S_IWGRP | stat.S_IXUSR | stat.S_IXGRP | stat.S_ISGID)
    os.chmod(dst, mode)

    for item in items:
        copyfiletree(
            os.path.join(src, item),
            os.path.join(dst, item)
        )


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
def inject_globals():
    import datetime
    import dateutil.tz
    import pytz

    utc = datetime.datetime.now(tz=pytz.utc)
    sa_tz = pytz.timezone('Africa/Johannesburg')
    sa = utc.astimezone(sa_tz)
    local_tz = dateutil.tz.tzlocal()
    local = utc.astimezone(local_tz)

    cti = utc.strftime('%Y-%m-%d %H:%M') + ' (UTC) • ' + sa.strftime('%H:%M (%Z)')

    if local.tzname() not in ('UTC', sa.tzname()):
        cti += ' • ' + local.strftime('%H:%M (%Z)')

    vi = 'Librarian %s (%s)' % (app.config['_version_string'], app.config['_git_hash'])

    lds_info = app.config.get('local_disk_staging')
    if lds_info is not None:
        staging_available = True
        staging_dest_displayed = lds_info['displayed_dest']
        staging_dest_path = lds_info['dest_prefix']
        staging_username_placeholder = lds_info['username_placeholder']
    else:
        staging_available = False
        staging_dest_displayed = None
        staging_dest_path = None
        staging_username_placeholder = None

    return {
        'current_time_info': cti,
        'version_info': vi,
        'staging_available': staging_available,
        'staging_dest_displayed': staging_dest_displayed,
        'staging_dest_path': staging_dest_path,
        'staging_username_placeholder': staging_username_placeholder,
    }


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
