# -*- coding: utf-8 -*-
# Copyright 2019 The HERA Collaboration
# Licensed under the 2-clause BSD License
"""Define config for pytest

"""

# ignore symlinked directories
def pytest_ignore_collect(path, config):
    if path.isdir() and path.islink():
        return True
