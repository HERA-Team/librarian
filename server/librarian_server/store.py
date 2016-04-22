# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"Stores."

from __future__ import absolute_import, division, print_function, unicode_literals

__all__ = str('''
Store
''').split ()

from flask import render_template

from . import app, db
from .dbutil import NotNull
from .webutil import ServerError, json_api, login_required, optional_arg, required_arg


class Store (db.Model):
    """A Store is a computer with a disk where we can store data. Several of the
    things we keep track of regarding stores are essentially configuration
    items; but we also keep track of the machine's availability, which is
    state that is better tracked in the database.

    """
    __tablename__ = 'store'

    id = db.Column (db.Integer, primary_key=True)
    name = NotNull (db.String (256))
    ssh_host = NotNull (db.String (256))
    path_prefix = NotNull (db.String (256))
    http_prefix = db.Column (db.String (256))
    available = NotNull (db.Boolean)

    def __init__ (self, name, path_prefix, ssh_host):
        self.name = name
        self.path_prefix = path_prefix
        self.ssh_host = ssh_host
        self.available = True


    @classmethod
    def get_by_name (cls, name):
        """Look up a store by name, or raise an RPCError on failure."""

        stores = list (cls.query.filter (cls.name == name))
        if not len (stores):
            raise RPCError ('No such store %r', name)
        if len (stores) > 1:
            raise RPCError ('Internal error: multiple stores with name %r', name)
        return stores[0]


    def _ssh_slurp (self, command):
        """SSH to the store host, run a command, and return its standard output. Raise
        an RPCError with standard error output if anything goes wrong.

        You MUST be careful about quoting! `command` is passed as an argument
        to 'bash -c', so it goes through one layer of parsing by the shell on
        the remote host. For instance, filenames containing '>' or ';' or '('
        or ' ' will cause problems unless you quote them appropriately. We do
        *not* launch our SSH process through a shell, so only one layer of
        shell quoting is required -- you'd need two if you were just typing
        the command in a terminal manually. BUT THEN, you're probably writing
        your command string as a Python string, so you probably need another
        layer of Python string literal quoting on top of that!

        """
        import subprocess

        argv = ['ssh', self.ssh_host, command]
        proc = subprocess.Popen (argv, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate ()

        if proc.returncode != 0:
            raise RPCError ('command "%s" failed: exit code %d; stdout:\n\n%s\n\nstderr:\n\n%s',
                            ' '.join (argv), proc.returncode, stdout, stderr)

        return stdout


    def get_info_for_path (self, storepath):
        """`storepath` is a path relative to our `path_prefix`. We assume that we are
        not running on the store host, but can transparently SSH to it.

        """
        import json
        text = self._ssh_slurp ("python -c \'import hera_librarian.utils as u; u.print_info_for_path(\"%s/%s\")\'"
                                % (self.path_prefix, storepath))
        return json.loads(text)


    _cached_space_info = None
    _space_info_timestamp = None

    def get_space_info (self):
        """Get information about how much space is available in the store. We have a
        simpleminded cache since it's nice just to be able to call the
        function, but SSHing into the store every time is going to be a bit
        silly.

        """
        import time
        now = time.time ()

        # 30 second lifetime:
        if self._cached_space_info is not None and now - self._space_info_timestamp < 30:
            return self._cached_space_info

        output = self._ssh_slurp ('df -B1 %s' % self.path_prefix)
        bits = output.splitlines ()[-1].split ()
        info = {}
        info['used'] = int(bits[2]) # measured in bytes
        info['available'] = int(bits[3]) # measured in bytes
        info['total'] = info['used'] + info['available']

        self._cached_space_info = info
        self._space_info_timestamp = now

        return info

    @property
    def capacity (self):
        """Returns the total capacity of the store, in bytes.

        Accessing this property may trigger an SSH into the store host!

        """
        return self.get_space_info ()['total']

    @property
    def usage_percentage (self):
        """Returns the amount of the storage capacity that is currently used as a
        percentage.

        Accessing this property may trigger an SSH into the store host!

        """
        info = self.get_space_info ()
        return 100. * info['used'] / (info['total'])


# RPC API

@app.route ('/api/recommended_store', methods=['GET', 'POST'])
@json_api
def recommended_store (args, sourcename=None):
    file_size = required_arg (args, int, 'file_size')
    if file_size < 0:
        raise ServerError ('"file_size" must be nonnegative')

    # We are simpleminded and just choose the store with the most available
    # space.

    most_avail = -1
    most_avail_store = None

    for store in Store.query.filter (Store.available):
        avail = store.get_space_info ()['available']
        if avail > most_avail:
            most_avail = avail
            most_avail_store = store

    if most_avail < file_size or most_avail_store is None:
        raise ServerError ('unable to find a store able to hold %d bytes', file_size)

    info = {}
    info['name'] = store.name
    info['ssh_host'] = store.ssh_host
    info['path_prefix'] = store.path_prefix
    info['available'] = most_avail # might be helpful?
    return info


# Web user interface

@app.route ('/stores')
@login_required
def stores ():
    q = Store.query.order_by (Store.name.asc ())
    return render_template (
        'store-listing.html',
        title='Stores',
        stores=q
    )


@app.route ('/stores/<string:name>')
@login_required
def specific_store (name):
    try:
        store = Store.get_by_name (name)
    except ServerError as e:
        flash (str (e))
        return redirect (url_for ('stores'))

    from .file import FileInstance
    instances = list (FileInstance.query
                      .filter (FileInstance.store == store.id)
                      .order_by (FileInstance.parent_dirs.asc (),
                                 FileInstance.name.asc ()))

    return render_template (
        'store-individual.html',
        title='Store %s' % (store.name),
        store=store,
        instances=instances,
    )
