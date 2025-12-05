# Copyright 2025 the HERA Collaboration
# Licensed under the BSD License.

"Files related RPC endpoints."

import sys
from flask import flash, redirect, render_template, url_for
from sqlalchemy.exc import SQLAlchemyError

from . import app, db
from .file import DeletionPolicy, File, FileInstance
from .webutil import ServerError, json_api, login_required, optional_arg, required_arg


@app.route("/api/create_file_event", methods=["GET", "POST"])
@json_api
def create_file_event(args, sourcename=None):
    """Create a FileEvent record for a File.

    We enforce basically no structure on the event data.

    """
    file_name = required_arg(args, str, "file_name")
    tp = required_arg(args, str, "type")
    payload = required_arg(args, dict, "payload")

    file = File.query.get(file_name)
    if file is None:
        raise ServerError('no known file "%s"', file_name)

    event = file.make_generic_event(tp, **payload)
    db.session.add(event)

    try:
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        app.log_exception(sys.exc_info())
        raise ServerError("failed to add event to database -- see server logs for details")

    return {}


@app.route("/api/locate_file_instance", methods=["GET", "POST"])
@json_api
def locate_file_instance(args, sourcename=None):
    """Tell the caller where to find an instance of the named file."""
    file_name = required_arg(args, str, "file_name")

    file = File.query.get(file_name)
    if file is None:
        raise ServerError('no known file "%s"', file_name)

    for inst in file.instances:
        return {
            "full_path_on_store": inst.full_path_on_store(),
            "store_name": inst.store_name,
            "store_path": inst.store_path,
            "store_ssh_host": inst.store_object.ssh_host,
        }

    raise ServerError('no instances of file "%s" on this librarian', file_name)


@app.route("/api/set_one_file_deletion_policy", methods=["GET", "POST"])
@json_api
def set_one_file_deletion_policy(args, sourcename=None):
    """Set the deletion policy of one instance of a file.

    The "one instance" restriction is just a bit of a sanity-check to throw up
    barriers against deleting all instances of a file if more than one
    instance actually exists.

    If the optional 'restrict_to_store' argument is supplied, only instances
    on the specified store will be modified. This is useful when clearing out
    a store for deactivation (see also the "offload" functionality). Note that
    the "one instance" limit still applies.

    """
    file_name = required_arg(args, str, "file_name")
    deletion_policy = required_arg(args, str, "deletion_policy")
    restrict_to_store = optional_arg(args, str, "restrict_to_store")
    if restrict_to_store is not None:
        from .store import Store

        restrict_to_store = Store.get_by_name(restrict_to_store)  # ServerError if lookup fails

    file = File.query.get(file_name)
    if file is None:
        raise ServerError('no known file "%s"', file_name)

    deletion_policy = DeletionPolicy.parse_safe(deletion_policy)

    for inst in file.instances:
        # We could do this filter in SQL but it's easier to just do it this way;
        # you can't call filter() on `file.instances`.
        if restrict_to_store is not None and inst.store != restrict_to_store.id:
            continue

        inst.deletion_policy = deletion_policy
        break  # just one!
    else:
        raise ServerError('no instances of file "%s" on this librarian', file_name)

    try:
        db.session.add(
            file.make_generic_event(
                "instance_deletion_policy_changed",
                store_name=inst.store_object.name,
                parent_dirs=inst.parent_dirs,
                new_policy=deletion_policy,
            )
        )
    except:
        app.log_exception(sys.exc_info())
        raise ServerError("failed to add "+file_name+" to db.session.add. in app.context")

    try:
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        app.log_exception(sys.exc_info())
        raise ServerError("failed to commit changes to the database")
    except Exception as e:
        app.log_exception(sys.exc_info())
        raise ServerError("Some error that is not SQLAlchemyError has occurred while trying to mark"+file_name+" for deletion") from e

    return {}


@app.route("/api/delete_file_instances", methods=["GET", "POST"])
@json_api
def delete_file_instances(args, sourcename=None):
    """DANGER ZONE! Delete instances of the named file on all stores!

    See File.delete_instances for a description of the safety interlocks.

    """
    file_name = required_arg(args, str, "file_name")
    mode = optional_arg(args, str, "mode", "standard")
    restrict_to_store = optional_arg(args, str, "restrict_to_store")
    if restrict_to_store is not None:
        from .store import Store

        restrict_to_store = Store.get_by_name(restrict_to_store)  # ServerError if lookup fails

    file = File.query.get(file_name)
    if file is None:
        raise ServerError('no known file "%s"', file_name)

    return file.delete_instances(mode=mode, restrict_to_store=restrict_to_store)


@app.route("/api/delete_file_instances_matching_query", methods=["GET", "POST"])
@json_api
def delete_file_instances_matching_query(args, sourcename=None):
    """DANGER ZONE! Delete instances of lots of files on the store!

    See File.delete_instances for a description of the safety interlocks.

    """
    query = required_arg(args, str, "query")
    mode = optional_arg(args, str, "mode", "standard")
    restrict_to_store = optional_arg(args, str, "restrict_to_store")
    if restrict_to_store is not None:
        from .store import Store

        restrict_to_store = Store.get_by_name(restrict_to_store)  # ServerError if lookup fails

    from .search import compile_search

    query = compile_search(query, query_type="files")
    stats = {}

    for file in query:
        stats[file.name] = file.delete_instances(mode=mode, restrict_to_store=restrict_to_store)

    return {"stats": stats}


# Web user interface


@app.route("/files/<string:name>")
@login_required
def specific_file(name):
    file = File.query.get(name)
    if file is None:
        flash('No such file "%s" known' % name)
        return redirect(url_for("index"))

    instances = list(FileInstance.query.filter(FileInstance.name == name))
    events = sorted(file.events, key=lambda e: e.time, reverse=True)

    return render_template(
        "file-individual.html",
        title=f"{file.type} File {file.name}",
        file=file,
        instances=instances,
        events=events,
    )
