# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"""The way that Flask is designed, we have to read our configuration and
initialize many things on module import, which is a bit lame. There are
probably ways to work around that but things work well enough as is.

"""


from importlib.metadata import version, PackageNotFoundError
from packaging.version import parse

import contextlib
import logging
import sys


with contextlib.suppress(PackageNotFoundError):
    # version information is saved under hera_librarian package
    __version__ = version("hera_librarian")

_log_level_names = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


def _initialize():
    import json
    import os.path
    from flask import Flask
    from flask_sqlalchemy import SQLAlchemy

    if "LIBRARIAN_CONFIG_PATH" not in os.environ:
        raise ValueError(
            "The `LIBRARIAN_CONFIG_PATH` environment variable must be set "
            "before starting the librarian server. Run `export "
            "LIBRARIAN_CONFIG_PATH=/path/to/config.json` and try again."
        )
    config_path = os.environ["LIBRARIAN_CONFIG_PATH"]
    try:
        with open(config_path) as f:
            config = json.load(f)
    except FileNotFoundError:
        raise ValueError(f"Librarian configuration file {config_path} not found.")

    if "SECRET_KEY" not in config:
        print(
            'cannot start server: must define the Flask "secret key" as the item '
            '"SECRET_KEY" in "server-config.json"',
            file=sys.stderr,
        )
        sys.exit(1)

    # TODO: configurable logging parameters will likely be helpful. We use UTC
    # for timestamps using standard ISO-8601 formatting. The Python docs claim
    # that 8601 is the default format but this does not appear to be true.
    loglevel_cfg = config.get("log_level", "info")
    loglevel = _log_level_names.get(loglevel_cfg)
    warn_loglevel = loglevel is None
    if warn_loglevel:
        loglevel = logging.INFO

    logging.basicConfig(
        level=loglevel,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    import time

    logging.getLogger("").handlers[0].formatter.converter = time.gmtime
    logger = logging.getLogger("librarian")

    if warn_loglevel:
        logger.warn('unrecognized value %r for "log_level" config item', loglevel_cfg)

    tf = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
    app = Flask("librarian", template_folder=tf)
    app.config.update(config)
    db = SQLAlchemy(app)
    return logger, app, db


logger, app, db = _initialize()


def is_primary_server():
    """Ugh, need to figure out new model to deal with all of this."""
    if app.config.get("server", "flask") != "tornado":
        return True

    import tornado.process

    return tornado.process.task_id() == 0


# We have to manually import the modules that implement services. It's not
# crazy to worry about circular dependency issues, but everything will be all
# right.
from . import bgtasks, file, misc, observation, search, store, webutil  # noqa: E402


def get_version_info():
    """
    Extract version info from version tag.

    We're using setuptools_scm, so the git information is in the version tag.
    The one exception is when we're running from a tagged release. In that case,
    we get the git hash of the corresponding release from GitHub directly.

    Parameters
    ----------
    None

    Returns
    -------
    tag : str
        The semantic version of the installed librarian server.
    git_hash : str
        The git hash of the installed librarian server.
    """
    parsed_version = parse(__version__)
    tag = parsed_version.base_version
    local = parsed_version.local

    if local is None:
        # we're running from a "clean" (tagged/released) repo
        # get the git info from GitHub directly
        from subprocess import CalledProcessError, check_output

        gitcmd = ["git", "ls-remote", "https://github.com/HERA-Team/librarian.git", f"v{tag}"]

        try:
            output = check_output(gitcmd).decode("utf-8")
            git_hash = output.split()[0]
        except CalledProcessError:
            git_hash = "???"
    else:
        # check if version has "dirty" tag
        split_local = local.split(".")
        if len(split_local) > 1:
            logger.warn("running from a codebase with uncommited changes")

        # get git info from the tag--the hash has a leading "g" we ignore
        git_hash = split_local[0][1:]

    return tag, git_hash


def commandline(argv):
    version_string, git_hash = get_version_info()
    logger.info("starting up Librarian %s (%s)", version_string, git_hash)
    app.config["_version_string"] = version_string
    app.config["_git_hash"] = git_hash

    server = app.config.get("server", "flask")
    host = app.config.get("host", None)
    port = app.config.get("port", 21106)
    debug = app.config.get("flask_debug", False)
    n_server_processes = app.config.get("n_server_processes", 1)

    if host is None:
        print(
            'note: no "host" set in config; server will not be remotely accessible', file=sys.stderr
        )

    with app.app_context():
        maybe_add_stores()

    if n_server_processes > 1 and server != "tornado":
        print("error: can only use multiple processes with Tornado server", file=sys.stderr)
        sys.exit(1)

    if server == "tornado":
        # Need to set up HTTP server and fork subprocesses before doing
        # anything with the IOLoop.
        from tornado import web
        from tornado.httpserver import HTTPServer
        from tornado.ioloop import IOLoop
        from tornado.wsgi import WSGIContainer

        from .webutil import StreamFile

        flask_app = WSGIContainer(app)
        tornado_app = web.Application(
            [(r"/stream/.*", StreamFile), (r".*", web.FallbackHandler, {"fallback": flask_app})]
        )

        http_server = HTTPServer(tornado_app)
        http_server.bind(port, address=host)
        http_server.start(n_server_processes)
        with app.app_context():
            db.engine.dispose()  # force new connection after potentially forking

    do_mandc = app.config.get("report_to_mandc", False)
    if do_mandc:
        from . import mc_integration

        mc_integration.register_callbacks(version_string, git_hash)

    if server == "tornado":
        # Set up periodic report on background task status; also reminds us
        # that the server is alive.
        bgtasks.register_background_task_reporter()

        if is_primary_server():
            # Primary server is also in charge of checking out whether there's
            # anything to do with our standing orders.
            IOLoop.current().add_callback(search.queue_standing_order_copies)
            search.register_standing_order_checkin()

        # Hack the logger to indicate which server we are.
        import tornado.process

        taskid = tornado.process.task_id()
        if taskid is not None:
            fmtr = logging.getLogger("").handlers[0].formatter
            fmtr._fmt = fmtr._fmt.replace(": ", " #%d: " % taskid)

    if server == "flask":
        print('note: using "flask" server, so background operations will not work', file=sys.stderr)
        app.run(host=host, port=port, debug=debug)
    elif server == "tornado":
        from tornado.ioloop import IOLoop

        IOLoop.current().start()
    else:
        print("error: unknown server type %r" % server, file=sys.stderr)
        sys.exit(1)

    use_globus = app.config.get("use_globus", False)
    if use_globus:
        have_all_info = True
        # make sure we have the other information that we need
        if "globus_client_id" not in app.config.keys():
            print(
                "error: globus_client_id must be in the config file to use " "globus.",
                file=sys.stderr,
            )
            have_all_info = False
        if "globus_transfer_token" not in app.config.keys():
            print(
                "error: globus_transfer_token must be in the config file to use " "globus.",
                file=sys.stderr,
            )
            have_all_info = False
        if not have_all_info:
            app.config["use_globus"] = False
    else:
        # add the key just in case it wasn't there
        app.config["use_globus"] = False

    bgtasks.maybe_wait_for_threads_to_finish()


def maybe_add_stores():
    """Add any stores specified in the configuration file that we didn't already
    know about.

    """
    from sqlalchemy.exc import SQLAlchemyError
    from .store import Store

    for name, cfg in app.config.get("add-stores", {}).items():
        prev = Store.query.filter(Store.name == name).first()
        if prev is None:
            _store = Store(name, cfg["path_prefix"], cfg["ssh_host"])
            _store.http_prefix = cfg.get("http_prefix")
            _store.available = cfg.get("available", True)
            db.session.add(_store)

    try:
        db.session.commit()
    except SQLAlchemyError:
        db.rollback()
        raise  # this only happens on startup, so just refuse to start
