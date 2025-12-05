# Copyright 2025 the HERA Collaboration
# Licensed under the BSD License.

"""Store related endpoints."""

import os.path
import sys
from flask import flash, redirect, render_template, url_for
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func
from sqlalchemy.orm import aliased

from . import app, bgtasks, db
from .file import FileInstance
from .store import InstanceOffloadInfo, launch_copy_by_file_name, OffloaderTask, Store
from .webutil import ServerError, json_api, login_required, optional_arg, required_arg

OFFLOAD_BATCH_SIZE = 200


@app.route("/api/probe_stores", methods=["GET", "POST"])
@json_api
def probe_stores(args, sourcename=None):
    """Get information about the stores attached to this Librarian.

    The purpose of this API is to help administrators verify the configuration
    of one or more Librarian instances. It helps make it possible for a store
    host attached to Librarian A to check whether it is capable of connecting
    to the store hosts attached to Librarian B.

    As such, right now this command returns only the minimal amount of
    information needed to implement this behavior. It could give more detailed
    information.

    """
    store_list = []

    for store in Store.query.filter(Store.available):
        store_list.append(store.to_dict())

    return {"stores": store_list}


@app.route("/api/initiate_upload", methods=["GET", "POST"])
@json_api
def initiate_upload(args, sourcename=None):
    """Called when Librarian client wants to upload a file instance to one of our
    Stores. We verify that there's room, make a staging directory, and ingest
    the database records that we'll need to make sense of the file.

    """
    upload_size = required_arg(args, int, "upload_size")
    if upload_size < 0:
        raise ServerError('"upload_size" must be nonnegative')

    known_staging_store = optional_arg(args, str, "known_staging_store")
    known_staging_subdir = optional_arg(args, str, "known_staging_subdir")

    if (known_staging_store is None) ^ (known_staging_subdir is None):
        raise ServerError(
            'if "known_staging_store" is specified, so must '
            '"known_staging_subdir", and vice versa'
        )

    # First, figure out where the upload will go. If the destination isn't
    # pre-specified, we are simpleminded and just choose the store that is
    # marked as available that has the most available space.

    if known_staging_store is not None:
        dest_store = Store.get_by_name(known_staging_store)
        space_avail = dest_store.get_space_info()["available"]
    else:
        space_avail = -1
        dest_store = None

        for store in Store.query.filter(Store.available):
            avail = store.get_space_info()["available"]
            if avail > space_avail:
                space_avail = avail
                dest_store = store

        del store  # paranoia; had a bug where we used this below!

    if space_avail < upload_size or dest_store is None:
        raise ServerError("unable to find a store able to hold %d bytes", upload_size)

    info = {}
    info["name"] = dest_store.name
    info["ssh_host"] = dest_store.ssh_host
    info["path_prefix"] = dest_store.path_prefix
    info["available"] = space_avail  # might be helpful?

    # Now, create a staging directory where the uploader can put their files,
    # if necessary. This avoids multiple uploads stepping on each others'
    # toes.

    if known_staging_store is not None:
        info["staging_dir"] = known_staging_subdir
    else:
        info["staging_dir"] = dest_store._create_tempdir("staging")

    # Finally, the caller will also want to inform us about new database
    # records pertaining to the files that are about to be uploaded. Ingest
    # that information.

    from .misc import create_records

    create_records(args, sourcename)

    return info


@app.route("/api/complete_upload", methods=["GET", "POST"])
@json_api
def complete_upload(args, sourcename=None):
    """Called after a Librarian client has finished uploading a file instance to
    one of our Stores. We verify that the upload was successful and move the
    file into its final destination.

    """
    store_name = required_arg(args, str, "store_name")
    staging_dir = required_arg(args, str, "staging_dir")
    dest_store_path = required_arg(args, str, "dest_store_path")
    meta_mode = required_arg(args, str, "meta_mode")
    deletion_policy = optional_arg(args, str, "deletion_policy", "disallowed")
    staging_was_known = optional_arg(args, bool, "staging_was_known", False)
    null_obsid = optional_arg(args, bool, "null_obsid", False)
    store = Store.get_by_name(store_name)  # ServerError if failure
    file_name = os.path.basename(dest_store_path)
    staged_path = os.path.join(staging_dir, file_name)

    from .file import DeletionPolicy

    # Turn the specified deletion policy into one of our integer codes.
    # If the text is unrecognized, we go with DISALLOWED. That seems
    # better than erroring out, since if we've gotten here then the
    # client has already successfully uploaded the data -- we don't
    # want that to go to waste. And DISALLOWED is pretty clearly the
    # "safe" option.

    deletion_policy = DeletionPolicy.parse_safe(deletion_policy)

    store.process_staged_file(
        staged_path,
        dest_store_path,
        meta_mode,
        deletion_policy,
        source_name=sourcename,
        null_obsid=null_obsid,
    )

    # If we're still here, we're good and can kill the staging directory,
    # unless it was one that was handed to us externally, in which case we
    # assume that we should not touch it.

    if not staging_was_known:
        store._delete(staging_dir)

    # Finally, trigger a look at our standing orders.

    from .search import queue_standing_order_copies

    queue_standing_order_copies()

    return {}


@app.route("/api/register_instances", methods=["GET", "POST"])
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
    store_name = required_arg(args, str, "store_name")
    file_info = required_arg(args, dict, "file_info")
    null_obsid = optional_arg(args, bool, "null_obsid", False)

    from .file import File, FileInstance

    store = Store.get_by_name(store_name)  # ServerError if failure
    slashed_prefix = store.path_prefix + "/"

    # Sort the files to get the creation times to line up.

    for full_path in sorted(file_info.keys()):
        if not full_path.startswith(slashed_prefix):
            raise ServerError('file path %r should start with "%s"', full_path, slashed_prefix)

        # Do we already know about this instance? If so, just ignore it.

        store_path = full_path[len(slashed_prefix):]
        parent_dirs = os.path.dirname(store_path)
        name = os.path.basename(store_path)

        instance = FileInstance.query.get((store.id, parent_dirs, name))
        if instance is not None:
            continue

        # OK, we have to create some stuff.

        file = File.get_inferring_info(
            store, store_path, sourcename, info=file_info[full_path], null_obsid=null_obsid
        )
        inst = FileInstance(store, parent_dirs, name)
        db.session.add(inst)
        db.session.add(file.make_instance_creation_event(inst, store))

    try:
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        app.log_exception(sys.exc_info())
        raise ServerError("failed to commit new records to database; see logs for details")

    # Finally, trigger a look at our standing orders.

    from .search import queue_standing_order_copies

    queue_standing_order_copies()

    return {}


@app.route("/api/launch_file_copy", methods=["GET", "POST"])
@json_api
def launch_file_copy(args, sourcename=None):
    """Launch a copy of a file to a remote store."""
    file_name = required_arg(args, str, "file_name")
    connection_name = required_arg(args, str, "connection_name")
    remote_store_path = optional_arg(args, str, "remote_store_path")
    known_staging_store = optional_arg(args, str, "known_staging_store")
    known_staging_subdir = optional_arg(args, str, "known_staging_subdir")

    if (known_staging_store is None) ^ (known_staging_subdir is None):
        raise ServerError(
            "if known_staging_store is provided, known_staging_subdir must be "
            "too, and vice versa"
        )

    launch_copy_by_file_name(
        file_name,
        connection_name,
        remote_store_path,
        known_staging_store=known_staging_store,
        known_staging_subdir=known_staging_subdir,
    )
    return {}

@app.route("/api/initiate_offload", methods=["GET", "POST"])
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
    source_store_name = required_arg(args, str, "source_store_name")
    dest_store_name = required_arg(args, str, "dest_store_name")

    source_store = Store.get_by_name(source_store_name)  # ServerError if failure
    dest_store = Store.get_by_name(dest_store_name)

    # Gather information about instances in the source store that we'll try to
    # transfer. Background tasks can't access the database, so we need to
    # pre-collect this information. We want instances this store that do not
    # correspond to files that have instances on other stores, which results in
    # some moderately messy SQL.

    inst_alias = aliased(FileInstance)

    n_other_stores = (
        db.session.query(func.count())
        .filter(inst_alias.name == FileInstance.name)
        .filter(inst_alias.store != source_store.id)
        .as_scalar()
    )

    q = (
        FileInstance.query.filter(FileInstance.store == source_store.id)
        .filter(n_other_stores == 0)
        .limit(OFFLOAD_BATCH_SIZE)
    )

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
            raise ServerError("offload: failed to mark store as unavailable")

        return {"outcome": "store-shut-down"}

    # Otherwise, we're going to launch an offloader task. Create a staging
    # directory and fire off the task.

    staging_dir = dest_store._create_tempdir("offloader")
    base_source = source_store.convert_to_base_object()  # again: can't access DB
    base_dest = dest_store.convert_to_base_object()

    bgtasks.submit_background_task(OffloaderTask(base_source, base_dest, staging_dir, info))

    return {"outcome": "task-launched", "instance-count": len(info)}


@app.route("/stores/<string:name>/make-available", methods=["POST"])
@login_required
def make_store_available(name):
    try:
        store = Store.get_by_name(name)
    except ServerError as e:
        flash(str(e))
        return redirect(url_for("stores"))

    store.available = True

    try:
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        app.log_exception(sys.exc_info())
        flash("Failed to update database?! See server logs for details.")
        return redirect(url_for("stores"))

    flash('Marked store "%s" as available' % store.name)
    return redirect(url_for("stores") + "/" + store.name)


@app.route("/stores/<string:name>/make-unavailable", methods=["POST"])
@login_required
def make_store_unavailable(name):
    try:
        store = Store.get_by_name(name)
    except ServerError as e:
        flash(str(e))
        return redirect(url_for("stores"))

    store.available = False

    try:
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        app.log_exception(sys.exc_info())
        flash("Failed to update database?! See server logs for details.")
        return redirect(url_for("stores"))

    flash('Marked store "%s" as unavailable' % store.name)
    return redirect(url_for("stores") + "/" + store.name)


# Web user interface


@app.route("/stores")
@login_required
def stores():
    q = Store.query.order_by(Store.name.asc())
    return render_template("store-listing.html", title="Stores", stores=q)


@app.route("/stores/<string:name>")
@login_required
def specific_store(name):
    try:
        store = Store.get_by_name(name)
    except ServerError as e:
        flash(str(e))
        return redirect(url_for("stores"))

    from .file import FileInstance

    num_instances = db.session.query(func.count()).filter(FileInstance.store == store.id).scalar()

    if store.available:
        toggle_action = "make-unavailable"
        toggle_description = "Make unavailable"
    else:
        toggle_action = "make-available"
        toggle_description = "Make available"

    return render_template(
        "store-individual.html",
        title="Store %s" % (store.name),
        store=store,
        num_instances=num_instances,
        toggle_action=toggle_action,
        toggle_description=toggle_description,
    )
