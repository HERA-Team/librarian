# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"Files."

from __future__ import absolute_import, division, print_function, unicode_literals

__all__ = str('''
File
''').split ()

import datetime, re
from flask import flash, redirect, render_template, url_for

from . import app, db
from .dbutil import NotNull
from .webutil import RPCError, json_api, login_required, optional_arg, required_arg
from .observation import Observation
from .store import Store


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

        if '/' in name:
            raise ValueError ('illegal file name "%s": names may not contain "/"' % name)

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

    def __init__ (self, store_obj, parent_dirs, name):
        if '/' in name:
            raise ValueError ('illegal file name "%s": names may not contain "/"' % name)

        self.store = store_obj.id
        self.parent_dirs = parent_dirs
        self.name = name

    @property
    def store_name (self):
        from .store import Store
        return Store.query.get (self.store).name

    @property
    def file (self):
        return File.query.get (self.name)


# RPC endpoints

_md5_re = re.compile (r'^[0-9a-f]{32}$')

@app.route ('/api/create_or_update_file', methods=['GET', 'POST'])
@json_api
def create_or_update_file (args, sourcename=None):
    name = required_arg (args, unicode, 'name')
    type = required_arg (args, unicode, 'type')
    create_time = optional_arg (args, int, 'create_time_unix') # XXX: MJD? something else?
    obsid = required_arg (args, int, 'obsid')
    size = required_arg (args, int, 'size')
    md5 = required_arg (args, unicode, 'md5')

    if len (name) < 1 or len (name) > 256:
        raise RPCError ('ill-formed/overlong file name %r', name)
    if '/' in name:
        raise RPCError ('File names must not contain "/" characters; got %r', name)
    if len (type) < 1 or len (type) > 32:
        raise RPCError ('ill-formed file type %r for %r', type, name)
    if size < 0:
        raise RPCError ('File sizes must be nonnegative; got %r for %r', size, name)

    md5 = md5.lower ()
    if len (md5) != 32 or _md5_re.match (md5) is None:
        raise RPCError ('Ill-formatted MD5 sum %r for file %r', md5, name)

    if create_time is not None:
        import datetime
        create_time = datetime.datetime.fromtimestamp (create_time)

    file = File (name, type, obsid, sourcename, size, md5, create_time)
    db.session.merge (file)
    db.session.commit ()
    return {}


# Web user interface

@app.route ('/files/<string:name>')
@login_required
def specific_file (name):
    file = File.query.get (name)
    if file is None:
        flash ('No such file "%s" known' % name)
        return redirect (url_for ('index'))

    instances = list (FileInstance.query.filter (FileInstance.name == name))

    return render_template (
        'file-individual.html',
        title='%s File %s' % (file.type, file.name),
        file=file,
        instances=instances,
    )
