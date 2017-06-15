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

import optparse
import os.path
import sys

import hera_librarian
from hera_librarian import utils


o = optparse.OptionParser()
o.set_usage('launch_librarian_copy.py <source-connection> <dest-connection> <file-name>')
o.set_description(__doc__)
o.add_option('--dest', type=str,
             help='The path in which the file should be stored at the destination. Default is the same as used locally.')
p.add_argument('--pre-staged', dest='pre_staged', metavar='STORENAME:SUBDIR',
               help='Specify that the data have already been staged at the destination.')

opts, args = o.parse_args(sys.argv[1:])


def die(fmt, *args):
    if not len(args):
        text = str(fmt)
    else:
        text = fmt % args
    print('error:', text, file=sys.stderr)
    sys.exit(1)


# Argument validation is pretty simple

if len(args) != 3:
    die('expect exactly three non-option arguments')

known_staging_store = None
known_staging_subdir = None

if args.pre_staged is not None:
    known_staging_store, known_staging_subdir = args.pre_staged.split(':', 1)

source_connection, dest_connection, file_name = args


# Let's do it.

file_name = os.path.basename(file_name)  # in case the user has spelled out a path
client = hera_librarian.LibrarianClient(source_connection)

try:
    client.launch_file_copy(file_name, dest_connection, remote_store_path=opts.dest,
                            known_staging_store=known_staging_store,
                            known_staging_subdir=known_staging_subdir)
except hera_librarian.RPCError as e:
    die('launch failed: %s', e)
