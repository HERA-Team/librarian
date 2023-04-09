# Copyright 2019 The HERA Collaboration
# Licensed under the 2-clause BSD license

"""Setup testing environment.

"""


import pytest

import json
import logging
import os
import sys
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

_log_level_names = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


@pytest.fixture(scope="module")
def db_connection():
    # reuse most of the code from _initialize()
    config_path = os.environ.get("LIBRARIAN_CONFIG_PATH", "server-config.json")
    with open(config_path) as f:
        config = json.load(f)

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

    pardir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__))))
    tf = os.path.join(os.path.dirname(pardir), "templates")
    app = Flask("librarian", template_folder=tf)
    app.config.update(config)
    app.config["TESTING"] = True
    app.testing = True
    db = SQLAlchemy(app)
    return logger, app, db
