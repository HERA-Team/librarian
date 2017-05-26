# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"""The way that Flask is designed, we have to read our configuration and
initialize many things on module import, which is a bit lame. There are
probably ways to work around that but things work well enough as is.

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import sys


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

    config_path = os.environ.get('LIBRARIAN_CONFIG_PATH', 'server-config.json')
    with open(config_path) as f:
        config = json.load(f)

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

def commandline(argv):
    server = app.config.get('server', 'flask')
    host = app.config.get('host', None)
    port = app.config.get('port', 21106)
    debug = app.config.get('flask_debug', False)

    if host is None:
        print('note: no "host" set in configuration; server will not be remotely accessible',
              file=sys.stderr)

    maybe_add_stores()

    if server == 'flask':
        print('note: using "flask" server, so background operations will not work',
              file=sys.stderr)
        app.run(host=host, port=port, debug=debug)
    elif server == 'tornado':
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

        # Set up to check out whether there's anything to do with our standing
        # orders.
        from . import search
        IOLoop.instance().add_callback(search.queue_standing_order_copies)
        search.register_standing_order_checkin()

        # Set up periodic report on background task status; also reminds us
        # that the server is alive.
        from . import bgtasks
        bgtasks.register_background_task_reporter()

        http_server = HTTPServer(tornado_app)
        http_server.listen(port, address=host)
        IOLoop.instance().start()
    else:
        print('error: unknown server type %r' % server, file=sys.stderr)
        sys.exit(1)

    bgtasks.maybe_wait_for_threads_to_finish()


def maybe_add_stores():
    """Add any stores specified in the configuration file that we didn't already
    know about.

    """
    from .store import Store

    for name, cfg in app.config.get('add-stores', {}).iteritems():
        prev = Store.query.filter(Store.name == name).first()
        if prev is None:
            store = Store(name, cfg['path_prefix'], cfg['ssh_host'])
            store.http_prefix = cfg.get('http_prefix')
            store.available = cfg.get('available', True)
            db.session.add(store)

    db.session.commit()
