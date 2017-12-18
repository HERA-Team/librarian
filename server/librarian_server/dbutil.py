# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"""Generic database utilities.

"""
from __future__ import absolute_import, division, print_function, unicode_literals

__all__ = str('''
NotNull
''').split()

from . import db


# Useful name reexport

from sqlalchemy.exc import SQLAlchemyError

# Right now this module does almost nothing. Maybe that will change in the
# future?


def NotNull(kind, **kwargs):
    return db.Column(kind, nullable=False, **kwargs)
