# Copyright 2025 the HERA Collaboration
# Licensed under the BSD License.

"""Background task web user interface endpoints."""


from flask import render_template

from . import app
from .webutil import the_task_manager
from .webutil import login_required


# Web user interface


@app.route("/tasks")
@login_required
def tasks():
    the_task_manager._maybe_purge_tasks()

    active = [
        t for t in the_task_manager.tasks if t.start_time is not None and t.finish_time is None
    ]
    pending = [t for t in the_task_manager.tasks if t.start_time is None]
    finished = [t for t in the_task_manager.tasks if t.finish_time is not None]

    return render_template(
        "task-listing.html", title="Tasks", active=active, pending=pending, finished=finished
    )
