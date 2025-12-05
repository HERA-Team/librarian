# Copyright 2025 the HERA Collaboration
# Licensed under the BSD License.


"""Miscellaneous endpoints."""

__all__ = str(
    """
create_records
gather_records
"""
).split()

from flask import render_template

from . import app
from .webutil import json_api, login_required


@app.template_filter("strftime")
def _jinja2_filter_datetime(unixtime, fmt=None):
    import time

    return time.strftime("%c", time.localtime(unixtime))


@app.template_filter("duration")
def _jinja2_filter_duration(seconds, fmt=None):
    if seconds < 90:
        return "%.0f seconds" % seconds
    if seconds < 4000:
        return "%.1f minutes" % (seconds / 60)
    if seconds < 100000:
        return "%.1f hours" % (seconds / 3600)
    return "%.1f days" % (seconds / 86400)


@app.context_processor
def inject_globals():
    import datetime
    import dateutil.tz
    import pytz

    utc = datetime.datetime.now(tz=pytz.utc)
    sa_tz = pytz.timezone("Africa/Johannesburg")
    sa = utc.astimezone(sa_tz)
    local_tz = dateutil.tz.tzlocal()
    local = utc.astimezone(local_tz)

    cti = utc.strftime("%Y-%m-%d %H:%M") + " (UTC) • " + sa.strftime("%H:%M (%Z)")

    if local.tzname() not in ("UTC", sa.tzname()):
        cti += " • " + local.strftime("%H:%M (%Z)")

    vi = "Librarian {} ({})".format(app.config["_version_string"], app.config["_git_hash"])

    lds_info = app.config.get("local_disk_staging")
    if lds_info is not None:
        staging_available = True
        staging_dest_displayed = lds_info["displayed_dest"]
        staging_dest_path = lds_info["dest_prefix"]
        staging_username_placeholder = lds_info["username_placeholder"]
    else:
        staging_available = False
        staging_dest_displayed = None
        staging_dest_path = None
        staging_username_placeholder = None

    return {
        "current_time_info": cti,
        "version_info": vi,
        "staging_available": staging_available,
        "staging_dest_displayed": staging_dest_displayed,
        "staging_dest_path": staging_dest_path,
        "staging_username_placeholder": staging_username_placeholder,
    }


# JSON API


@app.route("/api/ping", methods=["GET", "POST"])
@json_api
def ping(args, sourcename=None):
    return {"message": "hello"}


# Web UI


@app.route("/")
@login_required
def index():
    from .file import File
    from .observation import ObservingSession

    rs = ObservingSession.query.order_by(ObservingSession.start_time_jd.desc()).limit(7)
    rf = File.query.order_by(File.create_time.desc()).limit(50)
    return render_template(
        "main-page.html", title="Librarian Homepage", recent_files=rf, recent_sessions=rs
    )


@app.route("/connectivity-check")
@login_required
def connectivity_check():
    from .store import Store

    results = {}

    for store in Store.query.filter(Store.available):
        results[store.name] = store.check_stores_connections()

    return render_template("connectivity-check.html", title="Connectivity Check", results=results)
