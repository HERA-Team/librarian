# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"Observations."

from __future__ import absolute_import, division, print_function, unicode_literals

__all__ = str('''
Observation
''').split ()

from . import app, db
from .dbutil import NotNull


class Observation (db.Model):
    """An Observation is a span of time during which we have probably taken data.
    Every File is associated with a single Observation.

    """
    __tablename__ = 'observation'

    obsid = db.Column (db.Integer, primary_key=True)
    start_time_jd = NotNull (db.Float)
    start_time_jd = NotNull (db.Float)
    start_lst_hr = NotNull (db.Float)


# TODO: RPC endpoints? Web fronted pages?
