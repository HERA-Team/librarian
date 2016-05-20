#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Team.
# Licensed under the BSD License.

"""Upload a file to a Librarian. This script is a bit clunky when used
manually, but is invoked by other Librarians when copying files. Do NOT use
this script if the file that you wish to upload is already known to the local
Librarian. In that case, use the "launch_librarian_copy.py" script -- it will
make sure to preserve the associated metadata correctly. Under the hood,
"launch_librarian_copy.py" ends up invoking this script.

"""
from __future__ import absolute_import, division, print_function

import optparse, os.path, sys

import hera_librarian
from hera_librarian import utils


o = optparse.OptionParser()
o.set_usage('upload_to_librarian.py <connection-name> <path/to/local/file> <destination/path>')
o.set_description(__doc__)
o.add_option('--meta', type=str, default='infer',
             help='How to gather metadata: "json-stdin" or "infer"')

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

if os.path.isabs (dest_store_path):
    die ('destination path must be relative to store top; got %r', dest_store_path)

if opts.meta == 'json-stdin':
    import json
    try:
        rec_info = json.load (sys.stdin)
    except Exception as e:
        die ('cannot parse stdin as JSON data: %s', e)
    meta_mode = 'direct'
elif opts.meta == 'infer':
    rec_info = {}
    meta_mode = 'infer'
else:
    die ('unexpected metadata-gathering method %r', opts.meta)


# Let's do it.

client = hera_librarian.LibrarianClient (conn_name)

try:
    client.upload_file (local_path, dest_store_path, meta_mode, rec_info)
except hera_librarian.RPCError as e:
    die ('upload failed: %s', e)
