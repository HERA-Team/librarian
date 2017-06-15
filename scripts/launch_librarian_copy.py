#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Team.
# Licensed under the BSD License.

"""Launch a copy from one Librarian to another. Note that the filename
argument is treated just as the name of a file known to the source Librarian:
it does NOT have to be a file that exists on this particular machine. The
source Librarian will look up an existing instance of the file (on any
available store) and copy it over.

"""
from __future__ import absolute_import, division, print_function

import argparse
import os.path
import sys

import hera_librarian
from hera_librarian import utils


p = argparse.ArgumentParser(
    description=__doc__,
)
p.add_argument('--dest', type=str,
               help='The path in which the file should be stored at the destination. Default is the same as used locally.')
p.add_argument('--pre-staged', dest='pre_staged', metavar='STORENAME:SUBDIR',
               help='Specify that the data have already been staged at the destination.')
p.add_argument('source_conn_name', metavar='SOURCE-CONNECTION-NAME',
               help='Which Librarian originates the copy; as in ~/.hl_client.cfg.')
p.add_argument('dest_conn_name', metavar='DEST-CONNECTION-NAME',
               help='Which Librarian receives the copy; as in ~/.hl_client.cfg.')
p.add_argument('file_name', metavar='FILE-NAME',
               help='The name of the file to copy; need not be a local path.')

args = p.parse_args()


def die(fmt, *args):
    if not len(args):
        text = str(fmt)
    else:
        text = fmt % args
    print('error:', text, file=sys.stderr)
    sys.exit(1)


# Argument validation is pretty simple

known_staging_store = None
known_staging_subdir = None

if args.pre_staged is not None:
    known_staging_store, known_staging_subdir = args.pre_staged.split(':', 1)


# Let's do it.

file_name = os.path.basename(args.file_name)  # in case the user has spelled out a path
client = hera_librarian.LibrarianClient(args.source_conn_name)

try:
    client.launch_file_copy(file_name, args.dest_conn_name, remote_store_path=args.dest,
                            known_staging_store=known_staging_store,
                            known_staging_subdir=known_staging_subdir)
except hera_librarian.RPCError as e:
    die('launch failed: %s', e)
