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


__all__ = str(
    """
Store
UploaderTask
OffloaderTask
"""
).split()

import os.path
import sys
from sqlalchemy.exc import SQLAlchemyError

from hera_librarian.base_store import BaseStore

from . import app, bgtasks, db, logger
from .dbutil import NotNull
from .webutil import ServerError, required_arg


class Store(db.Model, BaseStore):
    """A Store is a computer with a disk where we can store data. Several of the
    things we keep track of regarding stores are essentially configuration
    items; but we also keep track of the machine's availability, which is
    state that is better tracked in the database.

    """

    __tablename__ = "store"

    id = db.Column(db.BigInteger, primary_key=True)
    name = NotNull(db.String(256), unique=True)
    ssh_host = NotNull(db.String(256))
    path_prefix = NotNull(db.String(256))
    http_prefix = db.Column(db.String(256))  # NOTE: this is totally unused
    available = NotNull(db.Boolean)
    instances = db.relationship("FileInstance", back_populates="store_object")

    def __init__(self, name, path_prefix, ssh_host):
        db.Model.__init__(self)
        BaseStore.__init__(self, name, path_prefix, ssh_host)
        self.available = True

    @classmethod
    def get_by_name(cls, name):
        """Look up a store by name, or raise an ServerError on failure."""

        stores = list(cls.query.filter(cls.name == name))
        if not len(stores):
            raise ServerError("No such store %r", name)
        if len(stores) > 1:
            raise ServerError("Internal error: multiple stores with name %r", name)
        return stores[0]

    def convert_to_base_object(self):
        """Asynchronous store operations are run on worker threads, which means that
        they're not allowed to access the database. But we'd like to be able to
        pass Store references around and reuse the functionality implemented in
        the `hera_librarian.base_store.BaseStore` class. So we have this helper
        function that converts this fancy, database-enabled object into a
        simpler one that can be passed to other threads and so on.

        """
        return BaseStore(self.name, self.path_prefix, self.ssh_host)

    def to_dict(self):
        """This function is currently only used for the /api/probe_stores command,
        so it's a bit limited. That could be changed.

        """
        return {"name": self.name, "path_prefix": self.path_prefix, "ssh_host": self.ssh_host}

    def process_staged_file(
        self,
        staged_path,
        dest_store_path,
        meta_mode,
        deletion_policy,
        source_name=None,
        null_obsid=False,
    ):
        """Called after a file has been placed in a staging directory on a store. We
        validate the upload and, if it's OK, put the file into its final
        destination and create the relevant database entries.

        """
        parent_dirs = os.path.dirname(dest_store_path)
        file_name = os.path.basename(dest_store_path)

        from .file import File, FileInstance

        if null_obsid and meta_mode != "infer":
            raise ServerError('internal error: null_obsid only valid when meta_mode is "infer"')

        # Do we already have the intended instance? If so ... just delete the
        # staged instance and return success, because the intended effect has
        # already been achieved.

        instance = FileInstance.query.get((self.id, parent_dirs, file_name))
        if instance is not None:
            self._delete(staged_path)
            return

        # Every file has associated metadata. Either we've already been given the
        # right info, or we need to infer it from the file instance -- the latter
        # technique only working for certain kinds of files that we know how to
        # deal with.

        if meta_mode == "direct":
            # In this case, something like the `initiate_upload` call should
            # have created all of the database records that we need to make
            # sense of this file. In particular, we should have a File record
            # ready to go.

            file = File.query.get(file_name)

            if file is None:
                # If this happens, it doesn't seem particularly helpful for debugging
                # to leave the staged file lying around.
                self._delete(staged_path)
                raise ServerError(
                    "cannot complete upload to %s:%s: proper metadata were "
                    "not uploaded in initiate_upload call",
                    self.name,
                    dest_store_path,
                )

            # Validate the staged file, abusing our argument-parsing helpers to make
            # sure we got everything from the info call. Note that we leave the file
            # around if we fail, in case that's helpful for debugging.

            try:
                info = self.get_info_for_path(staged_path)
            except Exception as e:
                raise ServerError(
                    "cannot complete upload to %s:%s: %s", self.name, dest_store_path, e
                )

            observed_size = required_arg(info, int, "size")
            observed_md5 = required_arg(info, str, "md5")

            if observed_size != file.size:
                raise ServerError(
                    "cannot complete upload to %s:%s: expected size %d; observed %d",
                    self.name,
                    dest_store_path,
                    file.size,
                    observed_size,
                )

            if observed_md5 != file.md5:
                raise ServerError(
                    "cannot complete upload to %s:%s: expected MD5 %s; observed %s",
                    self.name,
                    dest_store_path,
                    file.md5,
                    observed_md5,
                )
        elif meta_mode == "infer":
            # In this case, we must infer the metadata from the file instance
            # itself. This mode should be avoided, since we're unable to
            # verify that the file upload succeeded, but sometimes it's
            # necessary.

            if source_name is None:
                raise ServerError(
                    "internal bug on upload of %s:%s: must specify source_name "
                    "if inferring file properties",
                    self.name,
                    dest_store_path,
                )

            file = File.get_inferring_info(self, staged_path, source_name, null_obsid=null_obsid)
        else:
            raise ServerError('unrecognized "meta_mode" value %r', meta_mode)

        # Staged file is OK and we're not redundant. Move it to its new home. We
        # refuse to clobber an existing file; if one exists, there must be
        # something in the store's filesystem of which the Librarian is unaware,
        # which is a big red flag. If that happens, call that an error.
        #
        # We also change the file permissions if requested. I originally tried to
        # do this *before* the mv to avoid a race, but it turns out that if you're
        # non-root, you can't mv a directory that you don't have write permissions
        # on. (That is always true if you don't have write access on the
        # *containing* directory, but here I mean the directory itself.) To make
        # things as un-racy as possible, though, we include the chmod in the same
        # SSH invocation as the 'mv'.

        pmode = app.config.get("permissions_mode", "readonly")
        modespec = None

        if pmode == "readonly":
            modespec = "ugoa-w"
        elif pmode == "unchanged":
            pass
        else:
            logger.warn('unrecognized value %r for configuration option "permissions_mode"', pmode)

        try:
            self._move(staged_path, dest_store_path, chmod_spec=modespec)
        except Exception as e:
            raise ServerError(
                "cannot move upload to its destination (is there already "
                "a file there, unknown to this Librarian?): %s" % e
            )

        # Update the database. NOTE: there is an inevitable race between the move
        # and the database modification. Would it be safer to switch the ordering?

        inst = FileInstance(self, parent_dirs, file_name, deletion_policy=deletion_policy)
        db.session.add(inst)
        db.session.add(file.make_instance_creation_event(inst, self))

        try:
            db.session.commit()
        except SQLAlchemyError:
            db.session.rollback()
            app.log_exception(sys.exc_info())
            raise ServerError(
                "failed to commit new instance information to database; DB/FS consistency broken!"
            )

        return inst



# File uploads and copies -- maybe this should be separated into its own file?


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

        self.desc = "upload {}:{} to {}:{}".format(
            store.name, store_path, conn_name, remote_store_path or "<any>"
        )

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
        from .file import File

        if exc is None:
            logger.info(
                "upload of %s:%s => %s:%s succeeded",
                self.store.name,
                self.store_path,
                self.conn_name,
                self.remote_store_path or self.store_path,
            )
            error_code = 0
            error_message = "success"
        else:
            logger.warn(
                "upload of %s:%s => %s:%s FAILED: %s",
                self.store.name,
                self.store_path,
                self.conn_name,
                self.remote_store_path or self.store_path,
                exc,
            )
            error_code = 1
            error_message = str(exc)

        with app.app_context():
            file = File.query.get(os.path.basename(self.store_path))

            if error_code != 0:
                dt = rate = None
            else:
                dt = self.t_finish - self.t_start  # seconds
                dt_eff = max(dt, 0.5)  # avoid div-by-zero just in case
                rate = file.size / (dt_eff * 1024.0)  # kilobytes/sec (AKA kB/s)

                from . import mc_integration

                mc_integration.note_file_upload_succeeded(self.conn_name, file.size)

            db.session.add(
                file.make_copy_finished_event(
                    self.conn_name,
                    self.remote_store_path,
                    error_code,
                    error_message,
                    duration=dt,
                    average_rate=rate,
                )
            )

            if self.standing_order_name is not None and error_code == 0:
                # XXX keep this name synched with that in search.py:StandingOrder
                _type = "standing_order_succeeded:" + self.standing_order_name
                db.session.add(file.make_generic_event(_type))

            if error_code == 0:
                logger.info(
                    "transfer of %s:%s: duration %.1f s, average rate %.1f kB/s",
                    self.store.name,
                    self.store_path,
                    dt,
                    rate,
                )

            try:
                db.session.commit()
            except SQLAlchemyError:
                db.session.rollback()
                app.log_exception(sys.exc_info())
                raise ServerError("failed to commit completion events to database")


def launch_copy_by_file_name(
    file_name,
    connection_name,
    remote_store_path=None,
    standing_order_name=None,
    no_instance="raise",
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
        if no_instance == "raise":
            raise ServerError("cannot upload %s: no local file instances with that name", file_name)
        elif no_instance == "return":
            return True
        else:
            raise ValueError(f"unknown value for no_instance: {no_instance!r}")

    file = inst.file

    # Gather up information describing the database records that the other
    # Librarian will need.

    from .misc import gather_records

    rec_info = gather_records(file)

    # Figure out if we should try to use globus or not
    if app.config["use_globus"]:
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
        raise ServerError("failed to commit copy-launch event to database")


# Offloading files. This functionality was developed for a time when we had to
# use the RTP "still" machines as temporary emergency Librarian stores. After
# the emergency was over, we wanted to transfer their files back to the main
# storage "pot" machine and deactivate the temporary stores.


class InstanceOffloadInfo:
    def __init__(self, file_instance):
        self.parent_dirs = file_instance.parent_dirs
        self.name = file_instance.name
        self.success = False


class OffloaderTask(bgtasks.BackgroundTask):
    """Object that manages the task of offloading file instances from one store to
    another, staying on this Librarian.

    """

    def __init__(self, source_store, dest_store, staging_dir, instance_info):
        self.source_store = source_store
        self.dest_store = dest_store
        self.staging_dir = staging_dir
        self.instance_info = instance_info
        self.desc = "offload ~%d instances from %s to %s" % (
            len(instance_info),
            source_store.name,
            dest_store.name,
        )

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
            stagepath = os.path.join(self.staging_dir, str(i) + "_" + info.name)
            self.source_store.upload_file_to_local_store(sourcepath, self.dest_store, stagepath)
            info.success = True

    def wrapup_function(self, retval, exc):
        from .file import DeletionPolicy, FileInstance

        with app.app_context():
            # Yay, we can access the database again! We need it to delete all of
            # the instances that we *successfully* copied. We also need to turn
            # the stores back into a DB-ified objects to do what we need to do.

            source_store = Store.get_by_name(self.source_store.name)
            dest_store = Store.get_by_name(self.dest_store.name)

            if exc is None:
                logger.info("instance offload %s => %s succeeded",
                            source_store.name, dest_store.name)
            else:
                # If the thread crashed, our state information should still be
                # reasonable, and we might as well complete any offloads that may
                # have actually copied successfully. So we pretty much ignore the
                # fact that an exception occurred.
                logger.warn(
                    "instance offload %s => %s FAILED: %s", source_store.name, dest_store.name, exc
                )

            # For all successful copies, we need to un-stage the file in the usual
            # way. If that worked, we mark the original instance as being
            # deleteable. The command-line client give the user a query that will
            # safely remove thee redundant instances using the standard deletion
            # mechanism.
            #
            # Here we *are* paranoid about exceptions.
            for i, info in enumerate(self.instance_info):
                desc_name = f"{source_store.name}:{info.parent_dirs}/{info.name}"

                if not info.success:
                    logger.warn("offload thread did not succeed on instance %s", desc_name)
                    continue

                try:
                    source_inst = FileInstance.query.get(
                        (source_store.id, info.parent_dirs, info.name))
                except Exception:
                    logger.warn("offloader wrapup: no instance %s; already deleted?", desc_name)
                    continue

                stagepath = os.path.join(self.staging_dir, f"{str(i)}_{source_inst.name}")

                try:
                    dest_store.process_staged_file(
                        stagepath, source_inst.store_path, "direct", source_inst.deletion_policy
                    )
                except Exception:
                    logger.warn(
                        "offloader failed to complete upload of %s", source_inst.descriptive_name()
                    )
                    continue

                # If we're still here, the copy succeeded and the destination
                # store has a shiny new instance. Mark the source instance as
                # deleteable.

                logger.info('offloader: marking "%s" for deletion', source_inst.descriptive_name())
                source_inst.deletion_policy = DeletionPolicy.ALLOWED
                db.session.add(
                    source_inst.file.make_generic_event(
                        "instance_deletion_policy_changed",
                        store_name=source_inst.store_object.name,
                        parent_dirs=source_inst.parent_dirs,
                        new_policy=DeletionPolicy.ALLOWED,
                        context="offload",
                    )
                )

            try:
                db.session.commit()
            except SQLAlchemyError:
                db.session.rollback()
                app.log_exception(sys.exc_info())
                logger.error("offloader: failed to commit db changes; continuing")

        # Finally, we can blow away the staging directory.

        logger.info(
            'offloader: processing complete; clearing staging directory "%s"', self.staging_dir
        )
        dest_store._delete(self.staging_dir)
