# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD License

"""Define test data files and attributes
"""

import pytest

import os

# define where to find the data and their properties
DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_data")

ALL_FILES = pytest.mark.datafiles(
    os.path.join(DATA_DIR, "zen.2458432.34569.uvh5"),
    os.path.join(DATA_DIR, "zen.2458043.12552.xx.HH.uvA"),
    keep_top_dir=True,
)

filetypes = ["uvh5", "uvA"]

obsids = [1225829886, 1192201262]  # uvh5, miriad

md5sums = ["291a451139cf16e73d880437270dd0ed", "ab038eee080348eaa5abd221ec702a67"]  # uvh5  # miriad

pathsizes = [224073, 983251]  # uvh5, miriad
