#! /usr/bin/env python
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.


import sys

try:
    from librarian_server import commandline
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "librarian_server package not found; please install with "
        "`pip install .[sever]` in the librarian repo and try again."
    )
commandline(sys.argv)
