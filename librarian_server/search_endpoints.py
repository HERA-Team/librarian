# Copyright 2016-2017 the HERA Collaboration
# Licensed under the BSD License.

"""Web user interface endpoints for searches."""

import os.path
import sys
from flask import Response, flash, redirect, render_template, request, url_for
from sqlalchemy.exc import SQLAlchemyError

from . import app, db
from .search import compile_search, StandingOrder, launch_stage_operation, queue_standing_order_copies
from .webutil import ServerError, json_api, login_required, optional_arg, required_arg


@app.route("/standing-orders")
@login_required
def standing_orders():
    q = StandingOrder.query.order_by(StandingOrder.name.asc())

    return render_template("standing-order-listing.html", title="Standing Orders", storders=q)


@app.route("/standing-orders/<string:name>")
@login_required
def specific_standing_order(name):
    storder = StandingOrder.query.filter(StandingOrder.name == name).first()
    if storder is None:
        flash('No such standing order "%s"' % name)
        return redirect(url_for("standing_orders"))

    try:
        cur_files = list(storder.get_files_to_copy())
    except Exception as e:
        app.log_exception(sys.exc_info())
        flash("Cannot run this order’s search: %s" % e)
        cur_files = []

    return render_template(
        "standing-order-individual.html",
        title="Standing Order %s" % (storder.name),
        storder=storder,
        cur_files=cur_files,
    )


default_search = """{
  "name-matches": "any-file-named-like-%-this",
  "not-older-than": 14 # days
}"""


@app.route("/standing-orders/<string:ignored_name>/create", methods=["POST"])
@login_required
def create_standing_order(ignored_name):
    """Note that we ignore the order name and instead takes its value from the
    POST data; this is basically an implementation/consistency thing.

    """
    name = required_arg(request.form, str, "name")

    try:
        if not len(name):
            raise Exception("order name may not be empty")

        storder = StandingOrder(name, default_search, "undefined-connection")
        storder._validate()
        db.session.add(storder)

        try:
            db.session.commit()
        except SQLAlchemyError:
            db.session.rollback()
            app.log_exception(sys.exc_info())
            raise Exception("failed to commit information to database; see logs for details")
    except Exception as e:
        flash(f'Cannot create "{name}": {e}')
        return redirect(url_for("standing_orders"))

    return redirect(url_for("standing_orders") + "/" + name)


@app.route("/standing-orders/<string:name>/update", methods=["POST"])
@login_required
def update_standing_order(name):
    storder = StandingOrder.query.filter(StandingOrder.name == name).first()
    if storder is None:
        flash('No such standing order "%s"' % name)
        return redirect(url_for("standing_orders"))

    new_name = required_arg(request.form, str, "name")
    new_conn = required_arg(request.form, str, "conn")
    new_search = required_arg(request.form, str, "search")

    try:
        storder.name = new_name
        storder.conn_name = new_conn
        storder.search = new_search
        storder._validate()
        db.session.merge(storder)

        try:
            db.session.commit()
        except SQLAlchemyError:
            db.session.rollback()
            app.log_exception(sys.exc_info())
            raise Exception("failed to commit update to database; see logs for details")
    except Exception as e:
        flash(f'Cannot update "{name}": {e}')
        return redirect(url_for("standing_orders"))

    # There might be new things to look at!
    queue_standing_order_copies()

    flash('Updated standing order "%s"' % new_name)
    return redirect(url_for("standing_orders"))


@app.route("/standing-orders/<string:name>/delete", methods=["POST"])
@login_required
def delete_standing_order(name):
    storder = StandingOrder.query.filter(StandingOrder.name == name).first()
    if storder is None:
        flash('No such standing order "%s"' % name)
        return redirect(url_for("standing_orders"))

    db.session.delete(storder)

    try:
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        app.log_exception(sys.exc_info())
        raise ServerError("failed to commit deletion to database; see logs for details")

    flash('Deleted standing order "%s"' % name)
    return redirect(url_for("standing_orders"))


# Web interface to searches outside of the standing order system

sample_file_search = '{ "name-matches": "%12345%.uv" }'


@app.route("/search-files", methods=["GET", "POST"])
@login_required
def search_files():
    return render_template(
        "search-files.html", title="Search Files", sample_search=sample_file_search
    )


sample_obs_search = '{ "duration-less-than": 0.003 }'


@app.route("/search-obs", methods=["GET", "POST"])
@login_required
def search_obs():
    return render_template(
        "search-obs.html", title="Search Observations", sample_search=sample_obs_search
    )


sample_session_search = '{ "session-id-is-exactly": 1171209640 }'


@app.route("/search-sessions", methods=["GET", "POST"])
@login_required
def search_sessions():
    return render_template(
        "search-sessions.html",
        title="Search Observing Sessions",
        sample_search=sample_session_search,
    )


# These formats are defined in templates/search-*.html:
file_name_format = "Raw text with file names"
full_path_format = "Raw text with full instance paths"
human_file_format = "List of files"
human_obs_format = "List of observations"
human_session_format = "List of sessions"
stage_the_files_human_format = "stage-the-files-human"


@app.route("/search", methods=["GET", "POST"])
@login_required
def execute_search_ui():
    """The user-facing version of the search feature.

    Note that we perform no verification of the `stage_user` parameter!
    (Besides checking that it corresponds to a real system user.) This is
    incredibly lame but I'm not keen to build a real login system here. This
    means that we let users perform "file giveaways". I believe that this can
    be a security threat, but because the files that are given away are ones
    that come out of the Librarian, I think the most nefarious thing that can
    happen is denial-of-service by filling up someone else's quota. The chown
    script deployed at NRAO has safety checks in place to prevent giveaways to
    user accounts that are not HERA-using humans.

    """
    if len(request.form):
        reqdata = request.form
    else:
        reqdata = request.args

    query_type = required_arg(reqdata, str, "type")
    search_text = required_arg(reqdata, str, "search")
    output_format = optional_arg(reqdata, str, "output_format", human_file_format)
    stage_user = optional_arg(reqdata, str, "stage_user", "")
    stage_dest_suffix = optional_arg(reqdata, str, "stage_dest_suffix", "")
    for_humans = True

    if output_format == full_path_format:
        for_humans = False
        query_type = "names"
    elif output_format == file_name_format:
        for_humans = False
    elif output_format == human_file_format:
        for_humans = True
    elif output_format == human_obs_format:
        for_humans = True
    elif output_format == human_session_format:
        for_humans = True
    elif output_format == stage_the_files_human_format:
        for_humans = True
        query_type = "instances-stores"
        if request.method == "GET":
            return Response("Staging requires a POST operation", status=400)
        if not len(stage_user):
            return Response("Stage-files command did not specify the username", status=400)
    else:
        return Response(f"Illegal search output type {output_format!r}", status=400)

    status = 200

    if for_humans:
        mimetype = "text/html"
    else:
        mimetype = "text/plain"

    try:
        search = compile_search(search_text, query_type=query_type)

        if output_format == full_path_format:
            from .file import FileInstance

            instances = FileInstance.query.filter(FileInstance.name.in_(search))
            text = "\n".join(i.full_path_on_store() for i in instances)
        elif output_format == file_name_format:
            text = "\n".join(f.name for f in search)
        elif output_format == human_file_format:
            files = list(search)

            text = render_template(
                "search-results-file.html",
                title="Search Results: %d Files" % len(files),
                search_text=search_text,
                files=files,
                error_message=None,
            )
        elif output_format == human_obs_format:
            obs = list(search)
            text = render_template(
                "search-results-obs.html",
                title="Search Results: %d Observations" % len(obs),
                search_text=search_text,
                obs=obs,
                error_message=None,
            )
        elif output_format == human_session_format:
            sess = list(search)
            text = render_template(
                "search-results-session.html",
                title="Search Results: %d Sessions" % len(sess),
                search_text=search_text,
                sess=sess,
                error_message=None,
            )
        elif output_format == stage_the_files_human_format:
            # This will DTRT if stage_dest_suffix is empty:
            dest_prefix = app.config["local_disk_staging"]["dest_prefix"]
            stage_dest = os.path.join(dest_prefix, stage_user, stage_dest_suffix)

            try:
                final_dest, n_instances, n_bytes = launch_stage_operation(
                    stage_user, search, stage_dest
                )
                error_message = None
            except Exception as e:
                app.log_exception(sys.exc_info())
                final_dest = "(ignored)"
                n_instances = n_bytes = 0
                error_message = str(e)

            text = render_template(
                "stage-launch-report.html",
                title="Staging Results",
                search_text=search_text,
                final_dest=final_dest,
                n_instances=n_instances,
                n_bytes=n_bytes,
                error_message=error_message,
            )
        else:
            raise ServerError("internal logic failure mishandled output format")
    except Exception as e:
        app.log_exception(sys.exc_info())
        status = 400

        if for_humans:
            text = render_template(
                "search-results-file.html",
                title="Search Results: Error",
                search_text=search_text,
                files=[],
                error_message=str(e),
            )
        else:
            text = "Search resulted in error: %s" % e

    return Response(text, status=status, mimetype=mimetype)


stage_the_files_json_format = "stage-the-files-json"
session_listing_json_format = "session-listing-json"
file_listing_json_format = "file-listing-json"
instance_listing_json_format = "instance-listing-json"
obs_listing_json_format = "obs-listing-json"


@app.route("/api/search", methods=["GET", "POST"])
@json_api
def execute_search_api(args, sourcename=None):
    """JSON API version of the search facility.

    Note that we perform no verification of the `stage_user` parameter!
    (Besides checking that it corresponds to a real system user.) This is
    incredibly lame but I'm not keen to build a real login system here.

    """
    search_text = required_arg(args, str, "search")
    output_format = required_arg(args, str, "output_format")
    stage_user = optional_arg(args, str, "stage_user", "")
    stage_dest = optional_arg(args, str, "stage_dest", "")

    if output_format == stage_the_files_json_format:
        query_type = "instances-stores"

        if request.method == "GET":
            raise ServerError("staging requires a POST operation")
        if not len(stage_dest):
            raise ServerError("stage-files search did not specify destination directory")
        if "local_disk_staging" not in app.config:
            raise ServerError("this Librarian does not support local-disk staging")
    elif output_format == session_listing_json_format:
        query_type = "sessions"
    elif output_format == file_listing_json_format:
        query_type = "files"
    elif output_format == instance_listing_json_format:
        query_type = "instances"
    elif output_format == obs_listing_json_format:
        query_type = "obs"
    else:
        raise ServerError("illegal search output type %r", output_format)

    search = compile_search(search_text, query_type=query_type)

    if output_format == stage_the_files_json_format:
        final_dest, n_instances, n_bytes = launch_stage_operation(stage_user, search, stage_dest)
        return dict(destination=final_dest, n_instances=n_instances, n_bytes=n_bytes)
    elif output_format == session_listing_json_format:
        return dict(results=[sess.to_dict() for sess in search])
    elif output_format == file_listing_json_format:
        return dict(results=[files.to_dict() for files in search])
    elif output_format == instance_listing_json_format:
        return dict(results=[instance.to_dict() for instance in search])
    elif output_format == obs_listing_json_format:
        return dict(results=[obs.to_dict() for obs in search])
    else:
        raise ServerError("internal logic failure mishandled output format")
