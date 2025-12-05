# Copyright 2016-2017 the HERA Collaboration
# Licensed under the BSD License.

"""Searches of the database

This code will likely need a lot of expansion, but we'll start simple.

"""


__all__ = str(
    """
compile_search
StandingOrder
queue_standing_order_copies
register_standing_order_checkin
"""
).split()

import datetime
import json
import logging
import os.path
import sys
import time

from . import app, bgtasks, db, is_primary_server, logger
from .dbutil import NotNull
from .webutil import ServerError

# The search parser. We save searches in a (hopefully) simple JSON format. The
# format is documented in `docs/Searching.md`. KEEP THE DOCS UPDATED!


class _AttributeTypes:
    string = "s"
    int = "i"
    float = "f"


AttributeTypes = _AttributeTypes()


class GenericSearchCompiler:
    """A simple singleton class that helps with compiling searches. The only state
    that we manage is the list of search clauses, which can be extended
    dynamically to support different types of attributes that searchable
    things possess.

    """

    def __init__(self):
        self.clauses = {
            "and": self._do_and,
            "or": self._do_or,
            "none-of": self._do_none_of,
            "always-true": self._do_always_true,
            "always-false": self._do_always_false,
        }

    def compile(self, search):
        """Compile a search that is specified as a JSON-like data structure.

        The `search` must be a dict, which is interpreted as a set of clauses
        that are ANDed logically.

        """
        if isinstance(search, dict):
            return self._compile_clause("and", search)

        raise ServerError(
            "can't parse search: data must " "be in dict format; got %s", search.__class__.__name__
        )

    def _compile_clause(self, name, payload):
        impl = self.clauses.get(name)
        if impl is None:
            raise ServerError("can't parse search: unrecognized clause %r" % name)
        return impl(name, payload)

    # Framework for doing searches on general attributes of database items.

    def _add_attributes(self, cls, attr_info):
        from functools import partial

        for attr_name, attr_type, attr_getter in attr_info:
            clause_name = attr_name.replace("_", "-")

            if attr_getter is None:
                attr_getter = partial(getattr, cls, attr_name)

            if attr_type == AttributeTypes.string:
                self.clauses[clause_name + "-is-exactly"] = partial(
                    self._do_str_is_exactly, attr_getter
                )
                self.clauses[clause_name + "-is-not"] = partial(self._do_str_is_not, attr_getter)
                self.clauses[clause_name + "-matches"] = partial(self._do_str_matches, attr_getter)
            elif attr_type == AttributeTypes.int:
                self.clauses[clause_name + "-is-exactly"] = partial(
                    self._do_int_is_exactly, attr_getter
                )
                self.clauses[clause_name + "-is-not"] = partial(self._do_int_is_not, attr_getter)
                self.clauses[clause_name + "-greater-than"] = partial(
                    self._do_num_greater_than, attr_getter
                )
                self.clauses[clause_name + "-less-than"] = partial(
                    self._do_num_less_than, attr_getter
                )
                self.clauses[clause_name + "-in-range"] = partial(
                    self._do_num_in_range, attr_getter
                )
                self.clauses[clause_name + "-not-in-range"] = partial(
                    self._do_num_not_in_range, attr_getter
                )
            elif attr_type == AttributeTypes.float:
                self.clauses[clause_name + "-greater-than"] = partial(
                    self._do_num_greater_than, attr_getter
                )
                self.clauses[clause_name + "-less-than"] = partial(
                    self._do_num_less_than, attr_getter
                )
                self.clauses[clause_name + "-in-range"] = partial(
                    self._do_num_in_range, attr_getter
                )
                self.clauses[clause_name + "-not-in-range"] = partial(
                    self._do_num_not_in_range, attr_getter
                )

    def _do_str_matches(self, attr_getter, clause_name, payload):
        if not isinstance(payload, str):
            raise ServerError(
                'can\'t parse "%s" clause: contents must be text, ' "but got %s",
                clause_name,
                payload.__class__.__name__,
            )
        return attr_getter().like(payload)

    def _do_str_is_exactly(self, attr_getter, clause_name, payload):
        if not isinstance(payload, str):
            raise ServerError(
                'can\'t parse "%s" clause: contents must be text, ' "but got %s",
                clause_name,
                payload.__class__.__name__,
            )
        return attr_getter() == payload

    def _do_str_is_not(self, attr_getter, clause_name, payload):
        if not isinstance(payload, str):
            raise ServerError(
                'can\'t parse "%s" clause: contents must be text, ' "but got %s",
                clause_name,
                payload.__class__.__name__,
            )
        return attr_getter() != payload

    def _do_int_is_exactly(self, attr_getter, clause_name, payload):
        if not isinstance(payload, int):
            raise ServerError(
                'can\'t parse "%s" clause: contents must be an integer, ' "but got %s",
                clause_name,
                payload.__class__.__name__,
            )
        return attr_getter() == payload

    def _do_int_is_not(self, attr_getter, clause_name, payload):
        if not isinstance(payload, int):
            raise ServerError(
                'can\'t parse "%s" clause: contents must be an integer, ' "but got %s",
                clause_name,
                payload.__class__.__name__,
            )
        return attr_getter() != payload

    def _do_num_greater_than(self, attr_getter, clause_name, payload):
        if not isinstance(payload, (int, float)):
            raise ServerError(
                'can\'t parse "%s" clause: contents must be numeric, ' "but got %s",
                clause_name,
                payload.__class__.__name__,
            )
        return attr_getter() > payload

    def _do_num_less_than(self, attr_getter, clause_name, payload):
        if not isinstance(payload, (int, float)):
            raise ServerError(
                'can\'t parse "%s" clause: contents must be numeric, ' "but got %s",
                clause_name,
                payload.__class__.__name__,
            )
        return attr_getter() < payload

    def _do_num_in_range(self, attr_getter, clause_name, payload):
        if (
            not isinstance(payload, list)
            or len(payload) != 2
            or not isinstance(payload[0], (int, float))
            or not isinstance(payload[1], (int, float))
        ):
            raise ServerError(
                'can\'t parse "%s" clause: contents must be a list of two numbers, ' "but got %s",
                clause_name,
                payload.__class__.__name__,
            )

        v1, v2 = payload
        if v1 > v2:
            v1, v2 = v2, v1

        from sqlalchemy import and_

        value = attr_getter()
        return and_(value >= v1, value <= v2)

    def _do_num_not_in_range(self, attr_getter, clause_name, payload):
        if (
            not isinstance(payload, list)
            or len(payload) != 2
            or not isinstance(payload[0], (int, float))
            or not isinstance(payload[1], (int, float))
        ):
            raise ServerError(
                'can\'t parse "%s" clause: contents must be a list of two numbers, ' "but got %s",
                clause_name,
                payload.__class__.__name__,
            )

        v1, v2 = payload
        if v1 > v2:
            v1, v2 = v2, v1

        from sqlalchemy import or_

        value = attr_getter()
        return or_(value < v1, value > v2)

    # Custom, generic clauses.

    def _do_and(self, clause_name, payload):
        if not isinstance(payload, dict) or not len(payload):
            raise ServerError(
                'can\'t parse "%s" clause: contents must be a dict, ' "but got %s",
                clause_name,
                payload.__class__.__name__,
            )
        from sqlalchemy import and_

        return and_(*[self._compile_clause(*t) for t in payload.items()])

    def _do_or(self, clause_name, payload):
        if not isinstance(payload, dict) or not len(payload):
            raise ServerError(
                'can\'t parse "%s" clause: contents must be a dict, ' "but got %s",
                clause_name,
                payload.__class__.__name__,
            )
        from sqlalchemy import or_

        return or_(*[self._compile_clause(*t) for t in payload.items()])

    def _do_none_of(self, clause_name, payload):
        if not isinstance(payload, dict) or not len(payload):
            raise ServerError(
                'can\'t parse "%s" clause: contents must be a dict, ' "but got %s",
                clause_name,
                payload.__class__.__name__,
            )
        from sqlalchemy import not_, or_

        return not_(or_(*[self._compile_clause(*t) for t in payload.items()]))

    def _do_always_true(self, clause_name, payload):
        """We just ignore the payload."""
        from sqlalchemy import literal

        return literal(True)

    def _do_always_false(self, clause_name, payload):
        """We just ignore the payload."""
        from sqlalchemy import literal

        return literal(False)


# Searches for observing sessions


def _session_get_id():
    from .observation import ObservingSession

    return ObservingSession.id


def _session_get_duration():
    """There is a "duration" property on the ObservingSession class, but it
    computes its result in Python code using math, which means that it doesn't
    work within an SQL query. Empirically, we get a silent failure to match
    any files if we try to search that way.

    """
    from .observation import ObservingSession

    return ObservingSession.stop_time_jd - ObservingSession.start_time_jd


def _session_get_num_obs():
    from sqlalchemy import func

    from .observation import Observation, ObservingSession

    return (
        db.session.query(func.count(Observation.obsid))
        .filter(Observation.session_id == ObservingSession.id)
        .as_scalar()
    )


def _session_get_num_files():
    from sqlalchemy import func

    from .file import File
    from .observation import Observation, ObservingSession

    return (
        db.session.query(func.count(File.name))
        .filter(File.obsid == Observation.obsid)
        .filter(Observation.session_id == ObservingSession.id)
        .as_scalar()
    )


def _session_get_age():
    from astropy.time import Time

    from .observation import ObservingSession

    return Time.now().jd - ObservingSession.stop_time_jd


simple_session_attrs = [
    ("session_id", AttributeTypes.int, _session_get_id),
    ("start_time_jd", AttributeTypes.float, None),
    ("stop_time_jd", AttributeTypes.float, None),
    ("duration", AttributeTypes.float, _session_get_duration),
    ("num_obs", AttributeTypes.int, _session_get_num_obs),
    ("num_files", AttributeTypes.int, _session_get_num_files),
    ("age", AttributeTypes.float, _session_get_age),
]


class ObservingSessionSearchCompiler(GenericSearchCompiler):
    def __init__(self):
        from .observation import ObservingSession

        super().__init__()
        self._add_attributes(ObservingSession, simple_session_attrs)

        self.clauses["no-file-has-event"] = self._do_no_file_has_event

    def _do_no_file_has_event(self, clause_name, payload):
        if not isinstance(payload, str):
            raise ServerError(
                'can\'t parse "%s" clause: contents must be text, ' "but got %s",
                clause_name,
                payload.__class__.__name__,
            )

        from sqlalchemy import func

        from .file import File, FileEvent
        from .observation import Observation, ObservingSession

        # This feels awfully gross, but it works.

        return (
            db.session.query(func.count(File.name))
            .filter(File.obsid == Observation.obsid)
            .filter(Observation.session_id == ObservingSession.id)
            .join(FileEvent)
            .filter(FileEvent.type == payload, File.name == FileEvent.name)
            .as_scalar()
            == 0
        )


the_session_search_compiler = ObservingSessionSearchCompiler()


# Searches for observations


def _obs_get_duration():
    """There is a "duration" property on the Observation class, but it computes
    its result in Python code using math, which means that it doesn't work
    within an SQL query. Empirically, we get a silent failure to match any
    files if we try to search that way.

    """
    from .observation import Observation

    return Observation.stop_time_jd - Observation.start_time_jd


def _obs_get_num_files():
    from sqlalchemy import func

    from .file import File
    from .observation import Observation

    return (
        db.session.query(func.count(File.name)).filter(File.obsid == Observation.obsid).as_scalar()
    )


def _obs_get_total_size():
    from sqlalchemy import func

    from .file import File
    from .observation import Observation

    return db.session.query(func.sum(File.size)).filter(File.obsid == Observation.obsid).as_scalar()


simple_obs_attrs = [
    ("obsid", AttributeTypes.int, None),
    ("start_time_jd", AttributeTypes.float, None),
    ("stop_time_jd", AttributeTypes.float, None),
    ("start_lst_hr", AttributeTypes.float, None),
    ("session_id", AttributeTypes.int, None),
    ("duration", AttributeTypes.float, _obs_get_duration),
    ("num_files", AttributeTypes.int, _obs_get_num_files),
    ("total_size", AttributeTypes.int, _obs_get_total_size),
]


class ObservationSearchCompiler(GenericSearchCompiler):
    def __init__(self):
        from .observation import Observation

        super().__init__()
        self._add_attributes(Observation, simple_obs_attrs)


the_obs_search_compiler = ObservationSearchCompiler()


# Searches for files


def _file_get_num_instances():
    from sqlalchemy import func

    from .file import File, FileInstance

    return db.session.query(func.count()).filter(FileInstance.name == File.name).as_scalar()


simple_file_attrs = [
    ("name", AttributeTypes.string, None),
    ("type", AttributeTypes.string, None),
    ("source", AttributeTypes.string, None),
    ("size", AttributeTypes.int, None),
    ("obsid", AttributeTypes.int, None),
    ("num-instances", AttributeTypes.int, _file_get_num_instances),
]


class FileSearchCompiler(GenericSearchCompiler):
    def __init__(self):
        from .file import File

        super().__init__()
        self._add_attributes(File, simple_file_attrs)
        self.clauses["obs-matches"] = self._do_obs_matches

        self.clauses["name-like"] = self.clauses["name-matches"]  # compat alias
        self.clauses["source-is"] = self.clauses["source-is-exactly"]  # compat alias

        self.clauses["obsid-is-null"] = self._do_obsid_is_null

        # These are technically properties of Observations, not Files, but
        # users aren't going to want to jump through extra hoops to query for
        # them, so we proxy the query clauses.
        for pfx in ("start-time-jd", "stop-time-jd", "start-lst-hr", "session-id"):
            for cname in the_obs_search_compiler.clauses.keys():
                if cname.startswith(pfx):
                    self.clauses[cname] = self._do_obs_sub_query

        # I named these in a very ... weird way.
        self.clauses["not-older-than"] = self._do_not_older_than
        self.clauses["not-newer-than"] = self._do_not_newer_than

    def _do_obsid_is_null(self, clause_name, payload):
        """We just ignore the payload."""
        from .file import File

        return File.obsid is None

    def _do_not_older_than(self, clause_name, payload):
        if not isinstance(payload, (int, float)):
            raise ServerError(
                'can\'t parse "%s" clause: contents must be ' "numeric, but got %s",
                clause_name,
                payload.__class__.__name__,
            )

        from .file import File

        cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=payload)
        return File.create_time > cutoff

    def _do_not_newer_than(self, clause_name, payload):
        if not isinstance(payload, (int, float)):
            raise ServerError(
                'can\'t parse "%s" clause: contents must be ' "numeric, but got %s",
                clause_name,
                payload.__class__.__name__,
            )

        from .file import File

        cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=payload)
        return File.create_time < cutoff

    def _do_obs_matches(self, clause_name, payload):
        from .file import File
        from .observation import Observation

        matched_obsids = db.session.query(Observation.obsid).filter(
            the_obs_search_compiler.compile(payload)
        )
        return File.obsid.in_(matched_obsids)

    def _do_obs_sub_query(self, clause_name, payload):
        from .file import File
        from .observation import Observation

        matched_obsids = db.session.query(Observation.obsid).filter(
            the_obs_search_compiler._compile_clause(clause_name, payload)
        )
        return File.obsid.in_(matched_obsids)


the_file_search_compiler = FileSearchCompiler()


def compile_search(search_string, query_type="files"):
    """This function returns a query on the File table that will return the File
    items matching the search.

    """
    from .file import File, FileInstance
    from .observation import Observation, ObservingSession
    from .store import Store

    # As a convenience, we strip out #-delimited comments from the input text.
    # The default JSON parser doesn't accept them, but they're nice for users.

    def filter_comments():
        for line in search_string.splitlines():
            yield line.split("#", 1)[0]

    search_string = "\n".join(filter_comments())

    # Parse JSON.

    try:
        search = json.loads(search_string)
    except Exception as e:
        app.log_exception(sys.exc_info())
        raise ServerError("can't parse search as JSON: %s", e)

    # Offload to the helper classes.

    if query_type == "files":
        return File.query.filter(the_file_search_compiler.compile(search))
    elif query_type == "names":
        return db.session.query(File.name).filter(the_file_search_compiler.compile(search))
    elif query_type == "obs":
        return Observation.query.filter(the_obs_search_compiler.compile(search))
    elif query_type == "sessions":
        return ObservingSession.query.filter(the_session_search_compiler.compile(search))
    elif query_type == "instances-stores":
        # The following syntax gives us a LEFT OUTER JOIN which is what we want to
        # get (at most) one instance for each File of interest.
        return (
            db.session.query(FileInstance, File, Store)
            .join(Store)
            .join(File, isouter=True)
            .filter(the_file_search_compiler.compile(search))
        )
    elif query_type == "instances":
        return (
            db.session.query(FileInstance)
            .join(File, isouter=True)
            .filter(the_file_search_compiler.compile(search))
        )
    else:
        raise ServerError("unhandled query_type %r", query_type)


# "Standing orders" to copy files from one Librarian to another.

stord_logger = logging.getLogger("librarian.standingorders")


class StandingOrder(db.Model):
    """A StandingOrder describes a rule for copying data from this Librarian to
    another. We save a search and a destination. When new files match that
    search, we automatically start copying them to the destination. We create
    a FileEvent with a name based on the name of the StandingOrder to mark
    when a file has successfully been copied.

    It is assumed that the relevant search has some time limit applied so that
    only files created in the last (e.g.) 7 days match.

    """

    __tablename__ = "standing_order"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = NotNull(db.String(64), unique=True)
    search = NotNull(db.Text)
    conn_name = NotNull(db.String(64))

    def __init__(self, name, search, conn_name):
        self.name = name
        self.search = search
        self.conn_name = conn_name
        self._validate()

    def _validate(self):
        """Check that this object's fields follow our invariants."""
        compile_search(self.search)  # will raise a ServerError if there's a problem.

    @property
    def event_type(self):
        return "standing_order_succeeded:" + self.name

    def get_files_to_copy(self):
        """Generate a list of files that ought to be copied, according to the
        specifications of this StandingOrder.

        """
        from .file import File, FileEvent

        # The core query is something freeform specified by the user.

        query = compile_search(self.search)

        # We then layer on a check that the files don't have the specified
        # marker event.

        already_done = db.session.query(File.name).filter(
            FileEvent.name == File.name, FileEvent.type == self.event_type
        )
        query = query.filter(~File.name.in_(already_done))

        # Finally we filter out files that already have copy tasks associated
        # with this standing order, exceping those tasks that encountered an
        # error.

        from .bgtasks import the_task_manager
        from .store import UploaderTask

        already_launched = {
            os.path.basename(t.store_path)
            for t in the_task_manager.tasks
            if (
                isinstance(t, UploaderTask)
                and self.name == t.standing_order_name
                and t.exception is None
            )
        }

        for file in query:
            if file.name not in already_launched:
                yield file

    def maybe_launch_copies(self):
        """Launch any file copy operations that need to happen according to this
        StandingOrder's specification.

        """
        from .store import launch_copy_by_file_name

        stord_logger.debug("evaluating standing order %s", self.name)

        for file in self.get_files_to_copy():
            stord_logger.debug("got a hit: %s", file.name)
            if launch_copy_by_file_name(
                file.name, self.conn_name, standing_order_name=self.name, no_instance="return"
            ):
                stord_logger.warn(
                    "standing order %s should copy file %s to %s, but no instances "
                    "of it are available",
                    self.name,
                    file.name,
                    self.conn_name,
                )


# A simple little manager for running standing orders. We have a timeout to
# not evaluate them that often ... in the current setup, evaluating certain
# orders can be quite hard on the database.

MIN_STANDING_ORDER_INTERVAL = 1200  # seconds
DEFAULT_STANDING_ORDER_DELAY = 90  # seconds


def _launch_copy_timeout():
    stord_logger.debug("timeout invoked")

    if the_standing_order_manager.maybe_launch_copies():
        # The checks actually ran.
        the_standing_order_manager.launch_queued = False
    else:
        # We didn't run the checks because we did so recently. If a new file
        # was uploaded we want to make sure that it's eventually checked, so
        # re-queue ourselves to run again.
        from tornado.ioloop import IOLoop

        stord_logger.debug("re-scheduling timeout")
        IOLoop.current().call_later(DEFAULT_STANDING_ORDER_DELAY, _launch_copy_timeout)


class StandingOrderManager:
    """A simple, singleton class for managing our standing orders.

    Other folks should primarily access the manager via the
    `queue_standing_order_copies` function. That function *queues* a command
    to examine our standing orders and launch any needed copy commands, with a
    default delay of 90 seconds. The delay is in place since uploads of files
    to the Librarian are likely to occur in batches, but it's easiest to just
    command the manager to "do its thing" whenever a file is uploaded. The
    delay makes it so that when we actually look for files to copy, there's
    probably a bunch of them ready to go, not just the very first one that was
    uploaded.

    """

    last_check = 0
    launch_queued = False

    def maybe_launch_copies(self):
        """Returns True unless nothing happened because we've run a search recently."""
        now = time.time()

        if now - self.last_check < MIN_STANDING_ORDER_INTERVAL:
            return False  # Don't evaluate too often

        # Check if there are any restrictions on what we do with standing
        # orders. TODO: it's been requested that we also add time constraints
        # on the uploads (Github issue #23).

        mode = app.config.get("standing_order_mode", "normal")

        if mode == "disabled":
            stord_logger.debug("not checking standing orders: explicitly disabled")
            return True
        elif mode == "nighttime":
            # Hack: qmaster is now on UTC = SAST - 2, so our definition of
            # "night" is a bit different than you might expect. Our intent is
            # 8pm-8am (actual) local time.
            hour = time.localtime(now).tm_hour
            if hour >= 6 and hour < 18:
                stord_logger.debug(
                    'not checking standing orders: "nighttime" ' "mode and hour = %d", hour
                )
                return True
        elif mode != "normal":
            stord_logger.warn('unrecognized standing_order_mode %r; treating as "normal"', mode)
            mode = "normal"

        stord_logger.debug("running searches")
        self.last_check = now
        with app.app_context():
            for storder in StandingOrder.query.all():
                storder.maybe_launch_copies()

        return True

    def queue_launch_copy(self):
        """Queue a main-thread callback to check whether we need to launch any copies
        associated with our standing orders.

        """
        stord_logger.debug("called queue_launch_copy")
        if self.launch_queued:
            return

        self.launch_queued = True
        from tornado.ioloop import IOLoop

        stord_logger.debug("timeout actually scheduled")
        IOLoop.current().call_later(DEFAULT_STANDING_ORDER_DELAY, _launch_copy_timeout)


the_standing_order_manager = StandingOrderManager()


def queue_standing_order_copies():
    # Only the primary server processes standing orders.
    if not is_primary_server():
        stord_logger.debug("not checking standing orders -- not primary server process")
        return

    stord_logger.debug("queueing check of standing orders")
    the_standing_order_manager.queue_launch_copy()


def register_standing_order_checkin():
    """Create a Tornado PeriodicCallback that will periodically check the
    standing orders to see if there's anything to do.

    Since we know all events related to files, in theory this shouldn't be
    needed, but in practice this can't hurt.

    The timeout for the callback is measured in milliseconds, so we queue an
    evaluation every 10 minutes.

    """
    from tornado import ioloop

    cb = ioloop.PeriodicCallback(queue_standing_order_copies, 60 * 10 * 1000)
    cb.start()
    return cb


# The local-disk staging system for the NRAO Librarian. In a sense this code
# isn't super relevant to searches, but the search system is how it gets
# launched, and it's not obvious to me that there's a better place to put it.


class StagerTask(bgtasks.BackgroundTask):
    """Object that manages the task of staging files from one disk to
    another on the machine that the Librarian server is running on.

    This functionality is extremely specialized to the NRAO Librarian, which
    runs on a machine called `herastore01` that is equipped with both large
    local RAID arrays, where the HERA data are stored, and a mount of a Lustre
    network filesystem, where users do their data processing. This "staging"
    functionality allows users to have the server copy data over to Lustre
    as quick as possible.

    """

    def __init__(self, dest, stage_info, bytes, user, chown_command):
        """Arguments:

        dest (str)
          The destination directory, which should exist.
        stage_info
          Iterable of `(store_prefix, parent_dirs, name)`.
        bytes (integer)
          Number of bytes to be staged.
        user (str)
          The name of the user that the files will be chowned to.
        chown_command (list of str)
          Beginning of the command line that will be used to change
          file ownership after staging is complete.

        """
        self.dest = dest
        self.stage_info = stage_info
        self.user = user
        self.chown_command = chown_command
        self.desc = "stage %d bytes to %s" % (bytes, dest)

        import os.path
        import time

        self.t_start = time.time()

        # In principle, we could execute multiple stage operations to the same
        # destination directory at the same time, but the files that we use to
        # report progress don't have unique names, so it wouldn't be possible
        # to understand whether individual operations succeeded or failed. We
        # therefore only allow one stage at once, using the STAGING-IN-PROGRESS
        # file as a lock.
        #
        # Relatedly, if a stage has already been executed to this directory,
        # any lingering STAGING-SUCCEEDED/STAGING-ERRORS files will get
        # chowned when this operation completes. The chown happens happens
        # *before* we write the new result files, so the when we try to do so
        # we get an EPERM. Prevent this by blowing away preexisting result
        # files.

        from errno import EEXIST, ENOENT

        try:
            flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
            fd = os.open(os.path.join(dest, "STAGING-IN-PROGRESS"), flags, 0o666)
        except OSError as e:
            if e.errno == EEXIST:
                raise Exception(
                    'a staging operation into directory "%s" is already in progress' % dest
                )
            raise
        else:
            with os.fdopen(fd, "wt") as f:
                print(self.t_start, file=f)

        for base in ["STAGING-SUCCEEDED", "STAGING-ERRORS"]:
            try:
                os.unlink(os.path.join(dest, base))
            except OSError as e:
                if e.errno != ENOENT:
                    raise

        self.failures = []

    def thread_function(self):
        import os
        import subprocess

        from .misc import copyfiletree, ensure_dirs_gw

        for store_prefix, parent_dirs, name in self.stage_info:
            source = os.path.join(store_prefix, parent_dirs, name)
            dest_pfx = os.path.join(self.dest, parent_dirs)
            dest = os.path.join(self.dest, parent_dirs, name)

            try:
                ensure_dirs_gw(dest_pfx)
            except Exception as e:
                self.failures.append((dest_pfx, str(e)))

            try:
                copyfiletree(source, dest)
            except Exception as e:
                self.failures.append((dest, str(e)))

        if len(self.failures):
            raise Exception("failures while attempting to create and copy files")

        # Now change ownership of the files.

        argv = self.chown_command + ["-u", self.user, "-R", "-d", self.dest]  # <= recursive

        subprocess.check_output(
            argv,
            stdin=open(os.devnull, "rb"),
            stderr=subprocess.STDOUT,
            shell=False,
            close_fds=True,
        )

    def wrapup_function(self, retval, exc):
        import time

        self.t_stop = time.time()

        if exc is not None or len(self.failures):
            with open(os.path.join(self.dest, "STAGING-ERRORS"), "w") as f:
                if exc is not None:
                    print("Unhandled exception:", exc, file=f)

                for destpath, e in self.failures:
                    print(f"For {destpath}: {e}", file=f)

            outcome_desc = "FAILED"
            log_func = logger.warn
        else:
            with open(os.path.join(self.dest, "STAGING-SUCCEEDED"), "w") as f:
                print(self.t_stop, file=f)

            outcome_desc = "finished"
            log_func = logger.info

        try:
            os.unlink(os.path.join(self.dest, "STAGING-IN-PROGRESS"))
        except Exception:
            # NOTE: app.log_exception() does not work here since we're not in a
            # request-handling context.
            logger.exception("couldn't remove staging-in-progress indicator for %r", self.dest)

        log_func(
            "local-disk staging into %s %s: duration %.1fs",
            self.dest,
            outcome_desc,
            self.t_stop - self.t_start,
        )


def launch_stage_operation(user, search, stage_dest):
    """Shared code to prep and launch a local-disk staging operation.

    user
      The user that will own the files in the end. This function validates
      that specified username is in fact a valid one on the system, but
      does not (and cannot) verify that the invoker is who they say they
      are.
    search
      A SQLAlchemy search for File objects that the user wants to stage.
    stage_dest
      The user-specified destination for the staging operation.
    Returns
      A tuple `(final_dest_dir, n_instances, n_bytes)`.

    """
    import os.path
    import pwd

    from .misc import ensure_dirs_gw
    from .store import Store

    lds_info = app.config["local_disk_staging"]

    # Valid username?

    try:
        pwd.getpwnam(user)
    except KeyError:
        raise Exception('staging user name "%s" was not recognized by the system' % user)

    # Validate and make the destination directory; let exception handling deal
    # with it if there's a problem.
    dest = os.path.realpath(stage_dest)
    if not dest.startswith(lds_info["dest_prefix"]):
        raise Exception(
            'staging destination must resolve to a subdirectory of "%s"; '
            'input "%s" resolved to "%s" instead' % (lds_info["dest_prefix"], stage_dest, dest)
        )
    ensure_dirs_gw(dest)

    info = list(search.filter(Store.ssh_host == lds_info["ssh_host"], Store.available))

    n_bytes = 0

    for _inst, file, _store in info:
        n_bytes += file.size

    # Quasi-hack: don't try to stage multiple instances of the same
    # file, since that will break if the "file" is a directory.
    stage_info = []
    seen_names = set()

    for inst, _file, store in info:
        if inst.name not in seen_names:
            seen_names.add(inst.name)
            stage_info.append((store.path_prefix, inst.parent_dirs, inst.name))

    bgtasks.submit_background_task(
        StagerTask(dest, stage_info, n_bytes, user, lds_info["chown_command"])
    )

    return dest, len(info), n_bytes
