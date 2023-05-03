# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"""Generic database utilities.

"""


__all__ = str(
    """
NotNull
"""
).split()

from . import db

# Useful name reexport


# Right now this module does almost nothing. Maybe that will change in the
# future?


def NotNull(kind, **kwargs):  # noqa: N802
    return db.Column(kind, nullable=False, **kwargs)
