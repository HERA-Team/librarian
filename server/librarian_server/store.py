# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"""Stores.

So this gets a bit complicated. The `hera_librarian package`, which is used by
both the server and clients, includes a Store class, since Librarian clients
access stores directly by SSH'ing into them. However, here in the server, we
also have database records for every store. I *think* it will not make things
too complicated and crazy to do the multiple inheritance thing we do below, so
that we get the functionality of the `hera_librarian.store.Store` class while
also making our `ServerStore` objects use the SQLAlchemy ORM. If this turns
out to be a dumb idea, we should have the ORM-Store class just be a thin
wrapper that can easily be turned into a `hera_librarian.store.Store`
instance.

"""

from __future__ import absolute_import, division, print_function, unicode_literals

__all__ = str('''
Store
''').split ()

import os.path

from flask import render_template

from hera_librarian.store import Store as BaseStore

from . import app, db
from .dbutil import NotNull
from .webutil import ServerError, json_api, login_required, optional_arg, required_arg


class Store (db.Model, BaseStore):
    """A Store is a computer with a disk where we can store data. Several of the
    things we keep track of regarding stores are essentially configuration
    items; but we also keep track of the machine's availability, which is
    state that is better tracked in the database.

    """
    __tablename__ = 'store'

    id = db.Column (db.BigInteger, primary_key=True)
    name = NotNull (db.String (256), unique=True)
    ssh_host = NotNull (db.String (256))
    path_prefix = NotNull (db.String (256))
    http_prefix = db.Column (db.String (256))
    available = NotNull (db.Boolean)
    instances = db.relationship ('FileInstance', back_populates='store_object')

    def __init__ (self, name, path_prefix, ssh_host):
        db.Model.__init__ (self)
        BaseStore.__init__ (self, name, path_prefix, ssh_host)
        self.available = True


    @classmethod
    def get_by_name (cls, name):
        """Look up a store by name, or raise an ServerError on failure."""

        stores = list (cls.query.filter (cls.name == name))
        if not len (stores):
            raise ServerError ('No such store %r', name)
        if len (stores) > 1:
            raise ServerError ('Internal error: multiple stores with name %r', name)
        return stores[0]


    def convert_to_base_object (self):
        """Asynchronous store operations are run on worker threads, which means that
        they're not allowed to access the database. But we'd like to be able
        to pass Store references around and reuse the functionality
        implemented in the `hera_librarian.store.Store` class. So we have this
        helper function that converts this fancy, database-enabled object into
        a simpler one that can be passed to other threads and so on.

        """
        return BaseStore (self.name, self.path_prefix, self.ssh_host)


# RPC API

@app.route ('/api/initiate_upload', methods=['GET', 'POST'])
@json_api
def initiate_upload (args, sourcename=None):
    """Called when Librarian client wants to upload a file instance to one of our
    Stores. We verify that there's room, make a staging directory, and ingest
    the database records that we'll need to make sense of the file.

    """
    upload_size = required_arg (args, int, 'upload_size')
    if upload_size < 0:
        raise ServerError ('"upload_size" must be nonnegative')

    # First, figure out where the upload will go. We are simpleminded and just
    # choose the store with the most available space.

    most_avail = -1
    most_avail_store = None

    for store in Store.query.filter (Store.available):
        avail = store.get_space_info ()['available']
        if avail > most_avail:
            most_avail = avail
            most_avail_store = store

    if most_avail < upload_size or most_avail_store is None:
        raise ServerError ('unable to find a store able to hold %d bytes', upload_size)

    info = {}
    info['name'] = store.name
    info['ssh_host'] = store.ssh_host
    info['path_prefix'] = store.path_prefix
    info['available'] = most_avail # might be helpful?

    # Now, create a staging directory where the uploader can put their files.
    # This avoids multiple uploads stepping on each others' toes.

    info['staging_dir'] = store._create_tempdir ('staging')

    # Finally, the caller will also want to inform us about new database
    # records pertaining to the files that are about to be uploaded. Ingest
    # that information.

    from .misc import create_records
    create_records (args, sourcename)

    return info


@app.route ('/api/complete_upload', methods=['GET', 'POST'])
@json_api
def complete_upload (args, sourcename=None):
    """Called after a Librarian client has finished uploading a file instance to
    one of our Stores. We verify that the upload was successful and move the
    file into its final destination.

    """
    store_name = required_arg (args, unicode, 'store_name')
    staging_dir = required_arg (args, unicode, 'staging_dir')
    dest_store_path = required_arg (args, unicode, 'dest_store_path')
    meta_mode = required_arg (args, unicode, 'meta_mode')

    store = Store.get_by_name (store_name) # ServerError if failure
    file_name = os.path.basename (dest_store_path)
    staged_path = os.path.join (staging_dir, file_name)

    from .file import File, FileInstance

    # Do we already have the intended instance? If so ... just delete the
    # staged instance and return success, because the intended effect of this
    # RPC call has already been achieved.

    parent_dirs = os.path.dirname (dest_store_path)
    instance = FileInstance.query.get ((store.id, parent_dirs, file_name))
    if instance is not None:
        store._delete (staging_dir)
        return {}

    # Every file has associated metadata. Either we've already been given the
    # right info, or we need to infer it from the file instance -- the latter
    # technique only working for certain kinds of files that we know how to
    # deal with.

    if meta_mode == 'direct':
        # In this case, the `initiate_upload` call should have created all of
        # the database records that we need to make sense of this file. In
        # particular, we should have a File record ready to go.

        file = File.query.get (file_name)

        if file is None:
            # If this happens, it doesn't seem particularly helpful for debugging
            # to leave the staged file lying around.
            store._delete (staging_dir)
            raise ServerError ('cannot complete upload to %s:%s: proper metadata were '
                               'not uploaded in initiate_upload call',
                               store_name, dest_store_path)

        # Validate the staged file, abusing our argument-parsing helpers to make
        # sure we got everything from the info call. Note that we leave the file
        # around if we fail, in case that's helpful for debugging.

        try:
            info = store.get_info_for_path (staged_path)
        except Exception as e:
            raise ServerError ('cannot complete upload to %s:%s: %s', store_name, dest_store_path, e)

        observed_size = required_arg (info, int, 'size')
        observed_md5 = required_arg (info, unicode, 'md5')

        if observed_size != file.size:
            raise ServerError ('cannot complete upload to %s:%s: expected size %d; observed %d',
                               store_name, dest_store_path, file.size, observed_size)

        if observed_md5 != file.md5:
            raise ServerError ('cannot complete upload to %s:%s: expected MD5 %s; observed %s',
                               store_name, dest_store_path, file.md5, observed_md5)
    elif meta_mode == 'infer':
        # In this case, we must infer the metadata from the file instance itself.
        # This mode should be avoided, since we're unable to verify that the file
        # upload succeeded.

        file = File.get_inferring_info (store, staged_path, sourcename)
    else:
        raise ServerError ('unrecognized "meta_mode" value %r', meta_mode)

    # Staged file is OK and we're not redundant. Move it to its new home.

    store._move (staged_path, dest_store_path)

    # Update the database. NOTE: there is an inevitable race between the move
    # and the database modification. Would it be safer to switch the ordering?

    inst = FileInstance (store, parent_dirs, file_name)
    db.session.add (inst)
    db.session.add (file.make_instance_creation_event (inst, store))
    db.session.commit ()

    # Finally, kill the staging directory. We save this for last just in case
    # it fails, so that the key operations (the move and the DB update) are
    # already locked in.

    store._delete (staging_dir)
    return {}


@app.route ('/api/register_instances', methods=['GET', 'POST'])
@json_api
def register_instances (args, sourcename=None):
    """In principle, this RPC call is similar to what `initiate_upload` and
    `complete_upload` do. However, this function should be called when files
    have magically appeared on a store rather than being "uploaded" from some
    external source. There is no consistency checking and no staging, and we
    always attempt to infer the files' key properties.

    If you are SCP'ing a file to a store, you should be using the
    `complete_upload` call, likely via the
    `hera_librarian.LibrarianClient.upload_file` routine, rather than this
    function.

    Because this API call is most sensibly initiated from a store, the caller
    already goes to the work of gathering the basic file info (MD5, size,
    etc.) that we're going to need in our inference step. See
    `scripts/add_obs_librarian.py` for the implementation.

    """
    store_name = required_arg (args, unicode, 'store_name')
    file_info = required_arg (args, dict, 'file_info')

    from .file import File, FileInstance

    store = Store.get_by_name (store_name) # ServerError if failure
    slashed_prefix = store.path_prefix + '/'

    # Sort the files to get the creation times to line up.

    for full_path in sorted (file_info.iterkeys ()):
        if not full_path.startswith (slashed_prefix):
            raise ServerError ('file path %r should start with "%s"',
                               full_path, slashed_prefix)

        # Do we already know about this instance? If so, just ignore it.

        store_path = full_path[len (slashed_prefix):]
        parent_dirs = os.path.dirname (store_path)
        name = os.path.basename (store_path)

        instance = FileInstance.query.get ((store.id, parent_dirs, name))
        if instance is not None:
            continue

        # OK, we have to create some stuff.

        file = File.get_inferring_info (store, store_path, sourcename,
                                        info=file_info[full_path])
        inst = FileInstance (store, parent_dirs, name)
        db.session.add (inst)
        db.session.add (file.make_instance_creation_event (inst, store))

    db.session.commit ()
    return {}


def _upload_background_worker (store, connection_name, rec_info, store_path, remote_store_path):
    store.upload_file_to_other_librarian (connection_name, rec_info,
                                          store_path, remote_store_path)


def _upload_wrapup (func_args, func_kwargs, retval, exc):
    import logging
    store, conn_name, rec_info, store_path, remote_store_path = func_args

    # In principle, we might want different integer error codes if there are
    # specific failure modes that we want to be able to analyze without
    # parsing the error messages. At the time being, we just use "1" to mean
    # that some exception happened. An "error" code of 0 always means success.

    if exc is None:
        logging.info ('upload of %s:%s => %s:%s succeeded',
                      store.name, store_path, conn_name, remote_store_path)
        error_code = 0
        error_message = 'success'
    else:
        logging.warn ('upload of %s:%s => %s:%s FAILED: %s',
                      store.name, store_path, conn_name, remote_store_path, exc)
        error_code = 1
        error_message = str (exc)

    from .file import File
    file = File.query.get (os.path.basename (store_path))
    db.session.add (file.make_copy_finished_event (conn_name, remote_store_path,
                                                   error_code, error_message))
    db.session.commit ()


@app.route ('/api/launch_file_copy', methods=['GET', 'POST'])
@json_api
def launch_file_copy (args, sourcename=None):
    """Launch a copy of a file to a remote store.

    Note that we only take the file name as an input -- we use our DB to see
    if there are any instances of it available locally.

    """
    file_name = required_arg (args, unicode, 'file_name')
    connection_name = required_arg (args, unicode, 'connection_name')
    remote_store_path = optional_arg (args, unicode, 'remote_store_path')

    # Find a local instance of the file

    from .file import FileInstance
    inst = FileInstance.query.filter (FileInstance.name == file_name).first ()
    if inst is None:
        raise ServerError ('cannot upload %s: no local file instances with that name', file_name)

    basestore = inst.store_object.convert_to_base_object ()
    file = inst.file

    # Gather up information describing the database records that the other
    # Librarian will need.

    from .misc import gather_records
    rec_info = gather_records (file)

    # And launch away

    from . import launch_background_task
    launch_background_task (_upload_background_worker, _upload_wrapup,
                            basestore, connection_name, rec_info, inst.store_path, remote_store_path)
    db.session.add (file.make_copy_launched_event (connection_name, remote_store_path))
    db.session.commit ()
    return {}


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
