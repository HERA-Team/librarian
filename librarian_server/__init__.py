# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"""The way that Flask is designed, we have to read our configuration and
initialize many things on module import, which is a bit lame. There are
probably ways to work around that but things work well enough as is.

"""


import logging
import sys
import time
import signal
from pkg_resources import get_distribution, DistributionNotFound, parse_version


try:
    # version information is saved under hera_librarian package
    __version__ = get_distribution("hera_librarian").version
except DistributionNotFound:
    # package is not installed
    pass


_log_level_names = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
}


def _initialize():
    import json
    import os.path
    from flask import Flask
    from flask_sqlalchemy import SQLAlchemy

    # we need to add this environment variable to let Flask use OAuth2 over http
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

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

    if 'SECRET_KEY' not in config:
        print('cannot start server: must define the Flask "secret key" as the item '
              '"SECRET_KEY" in "server-config.json"', file=sys.stderr)
        sys.exit(1)

    # TODO: configurable logging parameters will likely be helpful. We use UTC
    # for timestamps using standard ISO-8601 formatting. The Python docs claim
    # that 8601 is the default format but this does not appear to be true.
    loglevel_cfg = config.get('log_level', 'info')
    loglevel = _log_level_names.get(loglevel_cfg)
    warn_loglevel = (loglevel is None)
    if warn_loglevel:
        loglevel = logging.INFO

    logging.basicConfig(
        level=loglevel,
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%SZ'
    )
    import time
    logging.getLogger('').handlers[0].formatter.converter = time.gmtime
    logger = logging.getLogger('librarian')

    if warn_loglevel:
        logger.warn('unrecognized value %r for "log_level" config item', loglevel_cfg)

    tf = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
    app = Flask('librarian', template_folder=tf)
    app.config.update(config)
    db = SQLAlchemy(app)
    return logger, app, db


logger, app, db = _initialize()


def is_primary_server():
    """Ugh, need to figure out new model to deal with all of this.

    """
    if app.config.get('server', 'flask') != 'tornado':
        return True

    import tornado.process
    return tornado.process.task_id() == 0


# We have to manually import the modules that implement services. It's not
# crazy to worry about circular dependency issues, but everything will be all
# right.

from . import webutil
from . import observation
from . import store
from . import file
from . import bgtasks
from . import search
from . import misc


# Finally ...

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
    parsed_version = parse_version(__version__)
    tag = parsed_version.base_version
    local = parsed_version.local

    if local is None:
        # we're running from a "clean" (tagged/released) repo
        # get the git info from GitHub directly
        from subprocess import CalledProcessError, check_output

        gitcmd = [
            "git",
            "ls-remote",
            "https://github.com/HERA-Team/librarian.git",
            f"v{tag}",
        ]

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
    from . import bgtasks

    version_string, git_hash = get_version_info()
    logger.info('starting up Librarian %s (%s)', version_string, git_hash)
    app.config['_version_string'] = version_string
    app.config['_git_hash'] = git_hash

    server = app.config.get('server', 'flask')
    host = app.config.get('host', None)
    port = app.config.get('port', 21106)
    debug = app.config.get('flask_debug', False)
    n_server_processes = app.config.get('n_server_processes', 1)

    if host is None:
        print('note: no "host" set in configuration; server will not be remotely accessible',
              file=sys.stderr)

    with app.app_context():
        maybe_add_stores()

    if n_server_processes > 1:
        if server != 'tornado':
            print('error: can only use multiple processes with Tornado server', file=sys.stderr)
            sys.exit(1)

    if server == 'tornado':
        # Need to set up HTTP server and fork subprocesses before doing
        # anything with the IOLoop.
        from tornado.wsgi import WSGIContainer
        from tornado.httpserver import HTTPServer
        from tornado.ioloop import IOLoop
        from tornado import web
        from .webutil import StreamFile

        flask_app = WSGIContainer(app)
        tornado_app = web.Application([
            (r'/stream/.*', StreamFile),
            (r'.*', web.FallbackHandler, {'fallback': flask_app}),
        ])

        http_server = HTTPServer(tornado_app)
        http_server.bind(port, address=host)
        http_server.start(n_server_processes)
        with app.app_context():
            db.engine.dispose()  # force new connection after potentially forking

        # add graceful shutdown capabilities
        MAX_WAIT = 3  # seconds
        def sig_handler(sig, frame):
            io_loop = IOLoop.instance()

            def stop_loop(deadline):
                now = time.time()
                if now < deadline:
                    logging.info("Waiting for next tick")
                    io_loop.add_timeout(now + 1, stop_loop, deadline)
                else:
                    io_loop.stop()
                    logging.info("Shutdown finally")

            def shutdown():
                logging.info("Stopping http server")
                http_server.stop()
                logging.info("Will shutdown in %s seconds...", MAX_WAIT)
                stop_loop(time.time() + MAX_WAIT)

            logging.warning("Caught signal: %s", sig)
            io_loop.add_callback_from_signal(shutdown)

        signal.signal(signal.SIGTERM, sig_handler)
        signal.signal(signal.SIGINT, sig_handler)

    if server == 'tornado':
        # Set up periodic report on background task status; also reminds us
        # that the server is alive.
        bgtasks.register_background_task_reporter()

        if is_primary_server():
            # Primary server is also in charge of checking out whether there's
            # anything to do with our standing orders.
            from tornado.ioloop import IOLoop
            from . import search
            IOLoop.current().add_callback(search.queue_standing_order_copies)
            search.register_standing_order_checkin()

        # Hack the logger to indicate which server we are.
        import tornado.process
        taskid = tornado.process.task_id()
        if taskid is not None:
            fmtr = logging.getLogger('').handlers[0].formatter
            fmtr._fmt = fmtr._fmt.replace(': ', ' #%d: ' % taskid)

    if server == 'flask':
        print('note: using "flask" server, so background operations will not work',
              file=sys.stderr)
        app.run(host=host, port=port, debug=debug)
    elif server == 'tornado':
        from tornado.ioloop import IOLoop
        IOLoop.current().start()
    else:
        print('error: unknown server type %r' % server, file=sys.stderr)
        sys.exit(1)

    use_globus = app.config.get("use_globus", False)
    if use_globus:
        have_all_info = True
        # make sure we have the other information that we need
        if "globus_client_id" not in app.config.keys():
            print(
                "error: globus_client_id must be in the config file to use "
                "globus.",
                file=sys.stderr,
            )
            have_all_info = False
        if "globus_transfer_token" not in app.config.keys():
            print(
                "error: globus_transfer_token must be in the config file to use "
                "globus.",
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
    from .orm.storemetadata import StoreMetadata
    from hera_librarian.stores import StoreNames

    for name, cfg in app.config.get('add-stores', {}).items():
        prev = StoreMetadata.query.filter(StoreMetadata.name == name).first()
        if prev is None:
            store = StoreMetadata(
                name=name,
                store_type=StoreNames[cfg["type"]],
                store_data={**cfg, "name": name},
                transfer_manager_data=cfg["transfer"]
            )
            db.session.add(store)

    try:
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        raise  # this only happens on startup, so just refuse to start


# Import routes

from .api.upload import *