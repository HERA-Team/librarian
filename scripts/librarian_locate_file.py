#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Team.
# Licensed under the BSD License.

"""Ask the Librarian where to find a file. The file location is returned
as an SCP-ready string of the form "<host>:<full-path-on-host>".

"""
from __future__ import absolute_import, division, print_function

import argparse
import os.path
import sys

import hera_librarian


p = argparse.ArgumentParser(
    description=__doc__,
)

p.add_argument('conn_name', metavar='CONNECTION-NAME',
               help='Which Librarian to talk to; as in ~/.hl_client.cfg.')
p.add_argument('file_name', metavar='PATH',
               help='The name of the file to locate.')
args = p.parse_args()


def die(fmt, *args):
    if not len(args):
        text = str(fmt)
    else:
        text = fmt % args
    print('error:', text, file=sys.stderr)
    sys.exit(1)


# Let's do it.

# In case the user has provided directory components:
file_name = os.path.basename(args.file_name)
client = hera_librarian.LibrarianClient(args.conn_name)

try:
    result = client.locate_file_instance(file_name)
except hera_librarian.RPCError as e:
    die('couldn\'t locate file: %s', e)

print('%(store_ssh_host)s:%(full_path_on_store)s' % result)
