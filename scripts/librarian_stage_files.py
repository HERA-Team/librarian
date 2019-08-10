#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2017 the HERA Team.
# Licensed under the BSD License.

"""Tell the Librarian to stage files onto the local scratch disk. At NRAO,
this is the Lustre filesystem.

This script has been superseded by the `librarian stage-files` command.
It is kept here for backwards compatibility, and will be removed in a future
version.
"""
from __future__ import absolute_import, division, print_function

import os
import sys
import warnings
from hera_librarian.cli import main

if __name__ == "__main__":
    warnings.warn("This script has been supserseded by the `librarian stage-files` "
                  "command. This script is preserved for backwards compatibility, but "
                  "will be removed in a future version.")
    # fix up command-line args to behave correctly
    bindir = os.path.abspath(os.path.dirname(sys.argv[0]))
    librarian_script = os.path.join(bindir, "librarian")
    del sys.argv[0]
    sys.argv.insert(0, librarian_script)
    sys.argv.insert(1, "stage-files")
    sys.exit(main(*sys.argv))
