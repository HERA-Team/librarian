#!/usr/bin/env python
"""Register a list of files with the librarian.

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

p.add_argument('conn_name', metavar='CONNECTION-NAME',
               help='Which Librarian to talk to; as in ~/.hl_client.cfg.')
p.add_argument('store_name', metavar='NAME',
               help='The \"store\" name under which the Librarian knows this computer.')
p.add_argument('paths', metavar='PATHS', nargs='+',
               help='The paths to the files on this computer.')
args = p.parse_args()


def die(fmt, *args):
    if not len(args):
        text = str(fmt)
    else:
        text = fmt % args
    print('error:', text, file=sys.stderr)
    sys.exit(1)


# Load the info ...

print('Gathering information ...')
file_info = {}

for path in args.paths:
    path = os.path.abspath(path)
    print('   ', path)
    file_info[path] = utils.gather_info_for_path(path)


# ... and upload what we learned.

print('Registering with Librarian.')
client = hera_librarian.LibrarianClient(args.conn_name)
try:
    client.register_instances(args.store_name, file_info)
except hera_librarian.RPCError as e:
    die('RPC failed: %s' % e)
