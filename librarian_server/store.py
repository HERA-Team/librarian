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



__all__ = str('''
Store
UploaderTask
OffloaderTask
''').split()

import os.path

from flask import flash, redirect, render_template, url_for

from hera_librarian.base_store import BaseStore

from . import app, db, logger
from .webutil import ServerError, json_api, login_required, optional_arg, required_arg

from .orm.storemetadata import StoreMetadata
from hera_librarian.stores import CoreStore

class Store:
    pass


# RPC API

@app.route('/api/register_instances', methods=['GET', 'POST'])
@json_api
def register_instances(args, sourcename=None):
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
    store_name = required_arg(args, str, 'store_name')
    file_info = required_arg(args, dict, 'file_info')

    from .file import File, FileInstance

    store = Store.get_by_name(store_name)  # ServerError if failure
    slashed_prefix = store.path_prefix + '/'

    # Sort the files to get the creation times to line up.

    for full_path in sorted(file_info.keys()):
        if not full_path.startswith(slashed_prefix):
            raise ServerError('file path %r should start with "%s"',
                              full_path, slashed_prefix)

        # Do we already know about this instance? If so, just ignore it.

        store_path = full_path[len(slashed_prefix):]
        parent_dirs = os.path.dirname(store_path)
        name = os.path.basename(store_path)

        instance = FileInstance.query.get((store.id, parent_dirs, name))
        if instance is not None:
            continue

        # OK, we have to create some stuff.

        file = File.get_inferring_info(store, store_path, sourcename,
                                       info=file_info[full_path])
        inst = FileInstance(store, parent_dirs, name)
        db.session.add(inst)
        db.session.add(file.make_instance_creation_event(inst, store))

    try:
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        app.log_exception(sys.exc_info())
        raise ServerError('failed to commit new records to database; see logs for details')

    # Finally, trigger a look at our standing orders.

    from .search import queue_standing_order_copies
    queue_standing_order_copies()

    return {}


# File uploads and copies -- maybe this should be separated into its own file?

from . import bgtasks


class UploaderTask(bgtasks.BackgroundTask):
    """Object that manages the task of copying a file to another Librarian.

    If `known_staging_store` and `known_staging_subdir` are not None, the copy
    will be launched assuming that files have already been staged at a known
    location at the final destination. This is useful if files have been
    copied from one Librarian site to another outside of the Librarian
    framework.

    Parameters
    ----------
    store : BaseStore object
        A BaseStore object corresponding to the originating store.
    conn_name : str
        The name of the connection to use, as defined in ~/.hl_client.cfg.
    rec_info : dict
        A dictionary containing database information for the file to be
        transferred.
    store_path : str
        The full path to the file in the local store.
    remote_store_path : str, optional
        The path to place the file in the destination store. This may be None,
        in which case we will request the same "store path" as the FileInstance
        in this Librarian.
    standing_order_name : str, optional
        The standing order corresponding to this upload task.
    known_staging_store : str, optional
        The store corresponding to the already-uploaded file. Must be specified
        if `known_staging_subdir` is specified.
    known_staging_subdir : str, optional
        The target directory corresponding to the already-uploaded file. Must by
        specified if `known_staging_store` is specified.
    use_globus : bool, optional
        Specify whether to try to use globus to transfer files.
    client_id : str, optional
        The globus client ID to use for the transfer.
    transfer_token : str, optional
        The globus transfer token to use for the transfer.
    source_endpoint_id : str, optional
        The globus endpoint ID of the source store. May be omitted, in which
        case we assume it is a "personal" (as opposed to public) client.
    """
    t_start = None
    t_finish = None

    def __init__(
        self,
        store,
        conn_name,
        rec_info,
        store_path,
        remote_store_path,
        standing_order_name=None,
        known_staging_store=None,
        known_staging_subdir=None,
        use_globus=False,
        client_id=None,
        transfer_token=None,
        source_endpoint_id=None,
    ):
        self.store = store
        self.conn_name = conn_name
        self.rec_info = rec_info
        self.store_path = store_path
        self.remote_store_path = remote_store_path
        self.standing_order_name = standing_order_name
        self.known_staging_store = known_staging_store
        self.known_staging_subdir = known_staging_subdir
        self.use_globus = use_globus
        self.client_id = client_id
        self.transfer_token = transfer_token
        self.source_endpoint_id = source_endpoint_id

        self.desc = 'upload %s:%s to %s:%s' % (store.name, store_path,
                                               conn_name, remote_store_path or '<any>')

        if standing_order_name is not None:
            self.desc += ' (standing order "%s")' % standing_order_name

    def thread_function(self):
        import time
        self.t_start = time.time()
        self.store.upload_file_to_other_librarian(
            self.conn_name,
            self.rec_info,
            self.store_path,
            self.remote_store_path,
            known_staging_store=self.known_staging_store,
            known_staging_subdir=self.known_staging_subdir,
            use_globus=self.use_globus,
            client_id=self.client_id,
            transfer_token=self.transfer_token,
            source_endpoint_id=self.source_endpoint_id,
        )
        self.t_finish = time.time()

    def wrapup_function(self, retval, exc):
        # In principle, we might want different integer error codes if there are
        # specific failure modes that we want to be able to analyze without
        # parsing the error messages. At the time being, we just use "1" to mean
        # that some exception happened. An "error" code of 0 always means success.

        if exc is None:
            logger.info('upload of %s:%s => %s:%s succeeded',
                        self.store.name, self.store_path, self.conn_name,
                        self.remote_store_path or self.store_path)
            error_code = 0
            error_message = 'success'
        else:
            logger.warn('upload of %s:%s => %s:%s FAILED: %s',
                        self.store.name, self.store_path, self.conn_name,
                        self.remote_store_path or self.store_path, exc)
            error_code = 1
            error_message = str(exc)

        from .file import File
        file = File.query.get(os.path.basename(self.store_path))

        if error_code != 0:
            dt = rate = None
        else:
            dt = self.t_finish - self.t_start  # seconds
            dt_eff = max(dt, 0.5)  # avoid div-by-zero just in case
            rate = file.size / (dt_eff * 1024.)  # kilobytes/sec (AKA kB/s)


        db.session.add(file.make_copy_finished_event(self.conn_name, self.remote_store_path,
                                                     error_code, error_message, duration=dt,
                                                     average_rate=rate))

        if self.standing_order_name is not None and error_code == 0:
            # XXX keep this name synched with that in search.py:StandingOrder
            type = 'standing_order_succeeded:' + self.standing_order_name
            db.session.add(file.make_generic_event(type))

        if error_code == 0:
            logger.info('transfer of %s:%s: duration %.1f s, average rate %.1f kB/s',
                        self.store.name, self.store_path, dt, rate)

        try:
            db.session.commit()
        except SQLAlchemyError:
            db.session.rollback()
            app.log_exception(sys.exc_info())
            raise ServerError('failed to commit completion events to database')


def launch_copy_by_file_name(
    file_name,
    connection_name,
    remote_store_path=None,
    standing_order_name=None,
    no_instance='raise',
    known_staging_store=None,
    known_staging_subdir=None,
):
    """Launch a copy of a file to a remote Librarian.

    A ServerError will be raised if no instance of the file is available.

    The copy will be registered as a "background task" that the server will
    execute in a separate thread. If the server crashes, information about the
    background task will be lost.

    If `remote_store_path` is None, we request that the instance be located in
    whatever "store path" was used by the instance we locate.

    If `no_instance` is "raise", an exception is raised if no instance of the
    file is available on this location. If it is "return", we return True.
    Other values are not allowed.

    If `known_staging_store` and `known_staging_subdir` are not None, the copy
    will be launched assuming that files have already been staged at a known
    location at the final destination. This is useful if files have been
    copied from one Librarian site to another outside of the Librarian
    framework.

    """
    # Find a local instance of the file

    from .file import FileInstance
    inst = FileInstance.query.filter(FileInstance.name == file_name).first()
    if inst is None:
        if no_instance == 'raise':
            raise ServerError('cannot upload %s: no local file instances with that name', file_name)
        elif no_instance == 'return':
            return True
        else:
            raise ValueError('unknown value for no_instance: %r' % (no_instance, ))

    file = inst.file

    # Gather up information describing the database records that the other
    # Librarian will need.

    from .misc import gather_records
    rec_info = gather_records(file)

    # Figure out if we should try to use globus or not
    if app.config.get("use_globus", False):
        source_endpoint_id = app.config.get("globus_endpoint_id", None)
        try:
            client_id = app.config["globus_client_id"]
            transfer_token = app.config["globus_transfer_token"]
            use_globus = True
        except KeyError:
            client_id = None
            transfer_token = None
            use_globus = False
    else:
        use_globus = False
        client_id = None
        transfer_token = None
        source_endpoint_id = None

    # Launch the background task. We need to convert the Store to a base object since
    # the background task can't access the database.
    basestore = inst.store_object.convert_to_base_object()
    bgtasks.submit_background_task(
        UploaderTask(
            basestore,
            connection_name,
            rec_info,
            inst.store_path,
            remote_store_path,
            standing_order_name,
            known_staging_store=known_staging_store,
            known_staging_subdir=known_staging_subdir,
            use_globus=use_globus,
            client_id=client_id,
            transfer_token=transfer_token,
            source_endpoint_id=source_endpoint_id,
        )
    )

    # Remember that we launched this copy.
    db.session.add(file.make_copy_launched_event(connection_name, remote_store_path))

    try:
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        app.log_exception(sys.exc_info())
        raise ServerError('failed to commit copy-launch event to database')


@app.route('/api/launch_file_copy', methods=['GET', 'POST'])
@json_api
def launch_file_copy(args, sourcename=None):
    """Launch a copy of a file to a remote store.

    """
    file_name = required_arg(args, str, 'file_name')
    connection_name = required_arg(args, str, 'connection_name')
    remote_store_path = optional_arg(args, str, 'remote_store_path')
    known_staging_store = optional_arg(args, str, 'known_staging_store')
    known_staging_subdir = optional_arg(args, str, 'known_staging_subdir')

    if (known_staging_store is None) ^ (known_staging_subdir is None):
        raise ServerError('if known_staging_store is provided, known_staging_subdir must be '
                          'too, and vice versa')

    launch_copy_by_file_name(file_name, connection_name, remote_store_path,
                             known_staging_store=known_staging_store,
                             known_staging_subdir=known_staging_subdir)
    return {}


@app.route('/api/gather_file_record', methods=['GET', 'POST'])
@json_api
def gather_file_record(args, sourcename=None):
    """Get the record info for a file.

    """
    from .file import File
    from .misc import gather_records

    file_name = required_arg(args, str, "file_name")
    file = File.query.get(file_name)
    if file is None:
        raise ServerError('no file with that name found')

    rec_info = gather_records(file)

    return rec_info


@app.route('/api/create_file_record', methods=['GET', 'POST'])
@json_api
def create_file_record(args, sourcename=None):
    """Create file records.

    """
    from .misc import create_records

    create_records(args, sourcename)

    return {}


# Offloading files. This functionality was developed for a time when we had to
# use the RTP "still" machines as temporary emergency Librarian stores. After
# the emergency was over, we wanted to transfer their files back to the main
# storage "pot" machine and deactivate the temporary stores.

class InstanceOffloadInfo (object):
    def __init__(self, file_instance):
        self.parent_dirs = file_instance.parent_dirs
        self.name = file_instance.name
        self.success = False


class OffloaderTask (bgtasks.BackgroundTask):
    """Object that manages the task of offloading file instances from one store to
    another, staying on this Librarian.

    """

    def __init__(self, source_store, dest_store, staging_dir, instance_info):
        self.source_store = source_store
        self.dest_store = dest_store
        self.staging_dir = staging_dir
        self.instance_info = instance_info
        self.desc = 'offload ~%d instances from %s to %s' \
                    % (len(instance_info), source_store.name, dest_store.name)

    def thread_function(self):
        # I think it's better to just let the thread crash if anything goes
        # wrong, rather than catching exceptions for each file. The offload
        # operation is one that should be reliable; if something surprising
        # happens, the cautious course of action is to stop trying to futz
        # with things.

        for i, info in enumerate(self.instance_info):
            # It's conceivable that we could be attempting to move two
            # instances of the same file. In that case, their basenames would
            # clash in our staging directory. Therefore we mix in the index of
            # the instance_info item to uniquify things.

            sourcepath = os.path.join(info.parent_dirs, info.name)
            stagepath = os.path.join(self.staging_dir, str(i) + '_' + info.name)
            self.source_store.upload_file_to_local_store(sourcepath, self.dest_store, stagepath)
            info.success = True

    def wrapup_function(self, retval, exc):
        from .file import DeletionPolicy, FileInstance

        # Yay, we can access the database again! We need it to delete all of
        # the instances that we *successfully* copied. We also need to turn
        # the stores back into a DB-ified objects to do what we need to do.

        source_store = Store.get_by_name(self.source_store.name)
        dest_store = Store.get_by_name(self.dest_store.name)

        if exc is None:
            logger.info('instance offload %s => %s succeeded',
                        source_store.name, dest_store.name)
        else:
            # If the thread crashed, our state information should still be
            # reasonable, and we might as well complete any offloads that may
            # have actually copied successfully. So we pretty much ignore the
            # fact that an exception occurred.
            logger.warn('instance offload %s => %s FAILED: %s',
                        source_store.name, dest_store.name, exc)

        # For all successful copies, we need to un-stage the file in the usual
        # way. If that worked, we mark the original instance as being
        # deleteable. The command-line client give the user a query that will
        # safely remove thee redundant instances using the standard deletion
        # mechanism.
        #
        # Here we *are* paranoid about exceptions.

        pmode = app.config.get('permissions_mode', 'readonly')
        need_chmod = (pmode == 'readonly')

        for i, info in enumerate(self.instance_info):
            desc_name = '%s:%s/%s' % (source_store.name, info.parent_dirs, info.name)

            if not info.success:
                logger.warn('offload thread did not succeed on instance %s', desc_name)
                continue

            try:
                source_inst = FileInstance.query.get((source_store.id, info.parent_dirs, info.name))
            except Exception as e:
                logger.warn('offloader wrapup: no instance %s; already deleted?', desc_name)
                continue

            stagepath = os.path.join(self.staging_dir, str(i) + '_' + source_inst.name)

            try:
                dest_store.process_staged_file(stagepath, source_inst.store_path,
                                               'direct', source_inst.deletion_policy)
            except Exception as e:
                logger.warn('offloader failed to complete upload of %s',
                            source_inst.descriptive_name())
                continue

            # If we're still here, the copy succeeded and the destination
            # store has a shiny new instance. Mark the source instance as
            # deleteable.

            logger.info('offloader: marking "%s" for deletion', source_inst.descriptive_name())
            source_inst.deletion_policy = DeletionPolicy.ALLOWED
            db.session.add(source_inst.file.make_generic_event('instance_deletion_policy_changed',
                                                               store_name=source_inst.store_object.name,
                                                               parent_dirs=source_inst.parent_dirs,
                                                               new_policy=DeletionPolicy.ALLOWED,
                                                               context='offload'))

        try:
            db.session.commit()
        except SQLAlchemyError:
            db.session.rollback()
            app.log_exception(sys.exc_info())
            logger.error('offloader: failed to commit db changes; continuing')

        # Finally, we can blow away the staging directory.

        logger.info('offloader: processing complete; clearing staging directory "%s"', self.staging_dir)
        dest_store._delete(self.staging_dir)


OFFLOAD_BATCH_SIZE = 200


@app.route('/api/initiate_offload', methods=['GET', 'POST'])
@json_api
def initiate_offload(args, sourcename=None):
    """Launch a task to offload file instances from one store to another.

    This launches a background task that copies file instances from a source
    store to a destination store, then marks the source instances for
    deletion. If the source store is out of instances, it is marked as
    unavailable. Repeated calls, combined with appropriate deletion commands,
    will therefore eventually drain the source store of all its contents so
    that it can be shut down.

    To keep each task reasonably-sized, there is a limit to the number of
    files that may be offloaded in each call to this API. Just keep calling it
    until the source store is emptied. The actual number of instances
    transferred in each batch is unpredictable because instances may be added
    to or removed from the store while the offload operation is running.

    Note that this API just launches the background task and returns quickly,
    so it can't provide the caller with any information about whether the
    offload operation is successful. You need to look at the Librarian logs or
    task monitoring UI to check that.

    This API is motivated by a time when we needed to create some temporary
    stores to provide emergency backstop disk space. Once the emergency was
    over, we wanted to shut down these temporary stores.

    Due to this origin, this API is quite limited: for instance, you cannot
    choose *which* file instances to offload in each call.

    """
    source_store_name = required_arg(args, str, 'source_store_name')
    dest_store_name = required_arg(args, str, 'dest_store_name')

    from sqlalchemy import func
    from sqlalchemy.orm import aliased
    from .file import FileInstance

    source_store = Store.get_by_name(source_store_name)  # ServerError if failure
    dest_store = Store.get_by_name(dest_store_name)

    # Gather information about instances in the source store that we'll try to
    # transfer. Background tasks can't access the database, so we need to
    # pre-collect this information. We want instances this store that do not
    # correspond to files that have instances on other stores, which results in
    # some moderately messy SQL.

    inst_alias = aliased(FileInstance)

    n_other_stores = (db.session.query(func.count())
                      .filter(inst_alias.name == FileInstance.name)
                      .filter(inst_alias.store != source_store.id)
                      .as_scalar())

    q = (FileInstance.query
         .filter(FileInstance.store == source_store.id)
         .filter(n_other_stores == 0)
         .limit(OFFLOAD_BATCH_SIZE))

    info = [InstanceOffloadInfo(i) for i in q]

    # If no such instances exist, mark the store as unavailable, essentially
    # clearing it for deletion, and return.

    if not len(info):
        source_store.available = False

        try:
            db.session.commit()
        except SQLAlchemyError:
            db.session.rollback()
            app.log_exception(sys.exc_info())
            raise ServerError('offload: failed to mark store as unavailable')

        return {'outcome': 'store-shut-down'}

    # Otherwise, we're going to launch an offloader task. Create a staging
    # directory and fire off the task.

    staging_dir = dest_store._create_tempdir('offloader')
    base_source = source_store.convert_to_base_object()  # again: can't access DB
    base_dest = dest_store.convert_to_base_object()

    bgtasks.submit_background_task(OffloaderTask(
        base_source, base_dest, staging_dir, info))

    return {'outcome': 'task-launched', 'instance-count': len(info)}


@app.route('/stores/<string:name>/make-available', methods=['POST'])
@login_required
def make_store_available(name):
    try:
        store = Store.get_by_name(name)
    except ServerError as e:
        flash(str(e))
        return redirect(url_for('stores'))

    store.available = True

    try:
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        app.log_exception(sys.exc_info())
        flash('Failed to update database?! See server logs for details.')
        return redirect(url_for('stores'))

    flash('Marked store "%s" as available' % store.name)
    return redirect(url_for('stores') + '/' + store.name)


@app.route('/stores/<string:name>/make-unavailable', methods=['POST'])
@login_required
def make_store_unavailable(name):
    try:
        store = Store.get_by_name(name)
    except ServerError as e:
        flash(str(e))
        return redirect(url_for('stores'))

    store.available = False

    try:
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        app.log_exception(sys.exc_info())
        flash('Failed to update database?! See server logs for details.')
        return redirect(url_for('stores'))

    flash('Marked store "%s" as unavailable' % store.name)
    return redirect(url_for('stores') + '/' + store.name)


# Web user interface

@app.route('/stores')
@login_required
def stores():
    q = Store.query.order_by(Store.name.asc())
    return render_template(
        'store-listing.html',
        title='Stores',
        stores=q
    )


@app.route('/stores/<string:name>')
@login_required
def specific_store(name):
    from sqlalchemy import func

    try:
        store = Store.get_by_name(name)
    except ServerError as e:
        flash(str(e))
        return redirect(url_for('stores'))

    from .file import FileInstance
    num_instances = (db.session.query(func.count())
                     .filter(FileInstance.store == store.id)
                     .scalar())

    if store.available:
        toggle_action = 'make-unavailable'
        toggle_description = 'Make unavailable'
    else:
        toggle_action = 'make-available'
        toggle_description = 'Make available'

    return render_template(
        'store-individual.html',
        title='Store %s' % (store.name),
        store=store,
        num_instances=num_instances,
        toggle_action=toggle_action,
        toggle_description=toggle_description,
    )
