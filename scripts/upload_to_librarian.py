#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Team.
# Licensed under the BSD License.

"""upload_to_librarian.py

Upload a file to a Librarian. This script is a bit clunky when used manually,
but is invoked by other Librarians when copying files.

"""

from __future__ import absolute_import, division, print_function

import optparse, sys

import hera_librarian
from hera_librarian import utils


o = optparse.OptionParser()
o.set_usage('upload_to_librarian.py <connection-name> <path/to/local/file> <destination/path>')
o.set_description(__doc__)
o.add_option('--type', type=str,
             help='The "file type" that will be registered with the Librarian.')
o.add_option('--obsid', type=float,
             help='The file\'s associated observation ID that will be registered with the Librarian.')
o.add_option('--start-jd', type=float,
             help='The file\'s associated start Julian Date that may be registered with the Librarian.')
o.add_option('--create-time', type=int,
             help='The file\'s associated creation time that will be registered with the Librarian, as a Unix timestamp.')

opts, args = o.parse_args(sys.argv[1:])


def die (fmt, *args):
    if not len (args):
        text = str (fmt)
    else:
        text = fmt % args
    print ('error:', text, file=sys.stderr)
    sys.exit (1)


# Argument validation is pretty simple

if len (args) != 3:
    die ('expect exactly three non-option arguments')

conn_name, local_path, dest_store_path = args

# Let's do it.

client = hera_librarian.LibrarianClient (conn_name)

try:
    client.upload_file (local_path, dest_store_path, type=opts.type, start_jd=opts.start_jd,
                        obsid=opts.obsid, create_time=ops.create_time)
except hera_librarian.RPCError as e:
    die ('upload failed: %s', e)
