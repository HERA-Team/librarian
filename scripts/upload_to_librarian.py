#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Team.
# Licensed under the BSD License.

"""Upload a file to a Librarian. Do NOT use this script if the file that you
wish to upload is already known to the local Librarian. In that case, use the
"launch_librarian_copy.py" script -- it will make sure to preserve the
associated metadata correctly.

"""
from __future__ import absolute_import, division, print_function

import argparse, os.path, sys

import hera_librarian
from hera_librarian import utils


p = argparse.ArgumentParser (
    description=__doc__,
    epilog="""The LOCAL-PATH specifies where to find the source data on this machine, and
    can take any form. The DEST-PATH specifies where the data should be stored
    in the Librarian and should look something like "2345678/data.txt". The
    'basename' of DEST-PATH gives the unique filename under which the data
    will be stored. The other pieces (the 'store path'; "2345678" in the
    example) give a subdirectory where the file will be stored on one of the
    Librarian's stores; this location is not meaningful but is helpful for
    grouping related files. Unlike the "cp" command, it is incorrect to give
    the DEST-PATH as just "2345678": that will cause the file to be ingested
    under the name "2345678" with an empty 'store path'.

    """
)

p.add_argument ('--meta', dest='meta', nargs=1, default='infer',
                help='How to gather metadata: "json-stdin" or "infer"')
p.add_argument ('conn_name', metavar='CONNECTION-NAME',
                help='Which Librarian to talk to; as in ~/.hl_client.cfg.')
p.add_argument ('local_path', metavar='LOCAL-PATH',
                help='The path to the data on this machine.')
p.add_argument ('dest_store_path', metavar='DEST-PATH',
                help='The destination location: combination of "store path" and file name.')

args = p.parse_args ()


def die (fmt, *args):
    if not len (args):
        text = str (fmt)
    else:
        text = fmt % args
    print ('error:', text, file=sys.stderr)
    sys.exit (1)


# Argument validation is pretty simple

if os.path.isabs (args.dest_store_path):
    die ('destination path must be relative to store top; got %r', args.dest_store_path)

if args.meta == 'json-stdin':
    import json
    try:
        rec_info = json.load (sys.stdin)
    except Exception as e:
        die ('cannot parse stdin as JSON data: %s', e)
    meta_mode = 'direct'
elif args.meta == 'infer':
    rec_info = {}
    meta_mode = 'infer'
else:
    die ('unexpected metadata-gathering method %r', args.meta)


# Let's do it.

client = hera_librarian.LibrarianClient (args.conn_name)

try:
    client.upload_file (args.local_path, args.dest_store_path, meta_mode, rec_info)
except hera_librarian.RPCError as e:
    die ('upload failed: %s', e)
