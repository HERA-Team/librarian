#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2018 the HERA Team.
# Licensed under the BSD License.

"""Search for files in the librarian.
"""
from __future__ import absolute_import, division, print_function

import argparse
import os.path
import sys
import time

import hera_librarian


# from https://stackoverflow.com/questions/17330139/python-printing-a-dictionary-as-a-horizontal-table-with-headers
def print_table(myDict, colList=None, colNames=None):
    """Pretty print a list of dictionaries as a dynamically sized table.

    Args:
        myDict -- list of dictionaries
        colList -- list of dictionary keys to print in the specified order.
            If not specified, all keys are printed in random order.
        colNames -- names of column headers. Must be the same size as colList.
            If not specified, dictionary key names are used.

    Returns:
        None

    Author: Thierry Husson - Use it as you want but don't blame me.
    """
    if colList is None:
        colList = list(myDict[0].keys() if myDict else [])
    if colNames is None:
        myList = [colList]  # 1st row = header
    else:
        if len(colNames) != len(colList):
            raise ValueError("Number of column headers specified must match number of columns")
        myList = [colNames]
    for item in myDict:
        myList.append([str(item[col] or '') for col in colList])
    colSize = [max(map(len, col)) for col in zip(*myList)]
    formatStr = ' | '.join(["{{:<{}}}".format(i) for i in colSize])
    myList.insert(1, ['-' * i for i in colSize])  # Seperating line
    for item in myList:
        print(formatStr.format(*item))


# from https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
def sizeof_fmt(num, suffix='B'):
    """Format the size of a file in human-readable values.

    Args:
        num (int) -- file size in bytes
        suffix (str) -- suffix to use
    Returns:
        output (str) -- human readable filesize

    Notes:
        Follows the Django convention of the web search where base-10 prefixes are used,
        but base-2 counting is done.
    """
    for unit in ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "{0:3.1f} {1:}{2:}".format(num, unit, suffix)
        num /= 1024.0
    return "{0:.1f} {1:}{2:}".format(num, 'Y', suffix)


p = argparse.ArgumentParser(
    description=__doc__,
    epilog="""For documentation of the JSON search format, see
https://github.com/HERA-Team/librarian/blob/master/docs/Searching.md . Wrap
your JSON in single quotes to prevent your shell from trying to interpret the
special characters. """
)

p.add_argument('conn_name', metavar='CONNECTION-NAME',
               help='Which Librarian to talk to; as in ~/.hl_client.cfg.')
p.add_argument('search', metavar='JSON-SEARCH',
               help='A JSON search specification; files that match will be staged.')
args = p.parse_args()


def die(fmt, *args):
    if not len(args):
        text = str(fmt)
    else:
        text = fmt % args
    print('error:', text, file=sys.stderr)
    sys.exit(1)


# Let's do it.

client = hera_librarian.LibrarianClient(args.conn_name)

# Get the username. We could make this a command-line option but I think it's
# better to keep this a semi-secret. Note that the server does absolutely no
# verification of the values that are passed in.

import getpass
user = getpass.getuser()

try:
    result = client.search_files(args.search)
except hera_librarian.RPCError as e:
    die('search failed: %s', e)

nresults = len(result['results'])
if nresults == 0:
    # we didn't get anything
    die('No files matched this search')

print('Found {:d} matching files'.format(nresults))
# first go through entries to format file size and remove potential null obsids
for entry in result['results']:
    entry['size'] = sizeof_fmt(entry['size'])
    if entry['obsid'] is None:
        entry['obsid'] = 'None'

# now print the results as a table
print_table(result['results'], ['name', 'create_time', 'obsid', 'type', 'size'],
            ['Name', 'Created', 'Observation', 'Type', 'Size'])
