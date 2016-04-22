# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"""The way that Flask is designed, we have to read our configuration and
initialize many things on module import, which is a bit lame. There are
probably ways to work around that but things work well enough as is.

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import sys


def _initialize ():
    import json, os.path
    from flask import Flask
    from flask_sqlalchemy import SQLAlchemy

    config_path = os.environ.get ('LIBRARIAN_CONFIG_PATH', 'server-config.json')
    with open (config_path) as f:
        config = json.load (f)

    if 'SECRET_KEY' not in config:
        print ('cannot start server: must define the Flask "secret key" as the item '
               '"SECRET_KEY" in "server-config.json"', file=sys.stderr)
        sys.exit (1)

    tf = os.path.join (os.path.dirname (os.path.abspath (__file__)), 'templates')
    app = Flask ('librarian', template_folder=tf)
    app.config.update (config)
    db = SQLAlchemy (app)
    return app, db

app, db = _initialize ()


# Asynchronous worker threads. Currently only used for background copies. It
# is tricky to engineer these threads to work correctly in all cases (sudden
# server shutdown, etc.) so avoid them if possible.

_worker_pool = None

def launch_thread (func, *args, **kwargs):
    """apply_async() returns a result object that we could consult later, but
    we're a web service so there's no real way to go back and check in on what
    happened.

    """
    global _worker_pool

    if _worker_pool is None:
        from multiprocessing.pool import ThreadPool
        _worker_pool = ThreadPool (app.config.get ('n_worker_threads', 8))

    _worker_pool.apply_async (func, args, kwargs)


def maybe_wait_for_threads_to_finish ():
    if _worker_pool is None:
        return

    print ('Waiting for background jobs to complete ...')
    _worker_pool.close ()
    _worker_pool.join ()
    print ('   ... done.')


# We have to manually import the modules that implement services. It's not
# crazy to worry about circular dependency issues, but everything will be all
# right.

from . import webutil
from . import observation
from . import store
from . import file
from . import misc


# Finally ...

def commandline (argv):
    host = app.config.get ('host', None)
    port = app.config.get ('port', 21106)
    debug = app.config.get ('flask-debug', False)

    if host is None:
        print ('note: no "host" set in configuration; server will not be remotely accessible',
               file=sys.stderr)

    initdb = app.config.get ('initialize-database', False)
    if initdb:
        init_database ()

    app.run (host=host, port=port, debug=debug)
    maybe_wait_for_threads_to_finish ()


def init_database ():
    """NB: make sure this code doesn't blow up if invoked on an
    already-initialized database.

    """
    db.create_all ()

    from .store import Store

    for name, cfg in app.config.get ('add-stores', {}).iteritems ():
        prev = Store.query.filter (Store.name == name).first ()
        if prev is None:
            store = Store (name, cfg['path_prefix'], cfg['ssh_host'])
            store.http_prefix = cfg.get ('http_prefix')
            store.available = cfg.get ('available', True)
            db.session.add (store)

    db.session.commit ()
