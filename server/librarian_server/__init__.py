# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"""The way that Flask is designed, we have to read our configuration and
initialize many things on module import, which is a bit lame. There are
probably ways to work around that but things work well enough as is.

"""
from __future__ import absolute_import, division, print_function, unicode_literals


def _initialize ():
    import json, os.path
    from flask import Flask
    from flask_sqlalchemy import SQLAlchemy

    with open ('server-config.json') as f:
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


# We have to manually import the modules that implement services. It's not
# crazy to worry about circular dependency issues, but everything will be all
# right.

from . import webutil
from . import observation
from . import misc


# Finally ...

def commandline (argv):
    debug = app.config.get ('flask-debug', False)
    app.run (debug=debug)
