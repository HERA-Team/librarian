#!/usr/bin/env python
"""Register a list of files with the librarian.

"""
from __future__ import absolute_import, division, print_function

import optparse, os.path, sys
import hera_librarian
from hera_librarian import utils


o = optparse.OptionParser()
o.set_usage('add_obs_librarian.py <connection-name> <store-name> <paths...>')
o.set_description(__doc__)
opts, args = o.parse_args(sys.argv[1:])


def die (fmt, *args):
    if not len (args):
        text = str (fmt)
    else:
        text = fmt % args
    print ('error:', text, file=sys.stderr)
    sys.exit (1)


# Check args

if len (args) < 3:
    die ('expect at least three non-option arguments')

conn_name, store_name = args[:2]
paths = args[2:]


# Load the info ...

print ('Gathering information ...')
file_info = {}

for path in paths:
    path = os.path.abspath (path)
    print ('   ', path)
    file_info[path] = utils.gather_info_for_path (path)


# ... and upload what we learned.

print ('Registering with Librarian.')
client = hera_librarian.LibrarianClient (conn_name)
try:
    client.register_instances (store_name, file_info)
except hera_librarian.RPCError as e:
    die ('RPC failed: %s' % e)
