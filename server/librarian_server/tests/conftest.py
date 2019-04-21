# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 The HERA Collaboration
# Licensed under the 2-clause BSD license

"""Setup testing environment.

"""

from __future__ import print_function, division, absolute_import

import pytest
from .. import _initialize


@pytest.fixture(scope="module")
def db_connection():
    logger, app, db = _initialize()
    return logger, app, db
