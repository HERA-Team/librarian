# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"Stores."

from __future__ import absolute_import, division, print_function, unicode_literals

__all__ = str('''
Store
''').split ()

from . import app, db
from .dbutil import NotNull
from .webutil import RPCError, json_api, login_required


class Store (db.Model):
    """A Store is a computer with a disk where we can store data. Several of the
    things we keep track of regarding stores are essentially configuration
    items; but we also keep track of the available space and the machine's
    availability, which is state that is better tracked in the database.

    """
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


# RPC API

@app.route ('/api/recommended_store', methods=['GET', 'POST'])
@json_api
def recommended_store (args, sourcename=None):
    file_size = args.pop ('file_size', None)
    if not isinstance (file_size, int) or not file_size >= 0:
        raise RPCError ('illegal file_size argument')

    raise RPCError ('not yet implemented')


# TODO: Web user interface?
