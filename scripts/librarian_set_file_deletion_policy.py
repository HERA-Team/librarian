#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Team.
# Licensed under the BSD License.

"""Set the "deletion policy" of one instance of this file.

"""
from __future__ import absolute_import, division, print_function

import argparse
import os.path
import sys

import hera_librarian


p = argparse.ArgumentParser(
    description=__doc__,
)

p.add_argument('--store', metavar='STORE-NAME',
               help='Only alter instances found on the named store.')
p.add_argument('conn_name', metavar='CONNECTION-NAME',
               help='Which Librarian to talk to; as in ~/.hl_client.cfg.')
p.add_argument('file_name', metavar='FILE-NAME',
               help='The name of the file to modify.')
p.add_argument('deletion', metavar='POLICY',
               help='The new deletion policy: "allowed" or "disallowed"')
args = p.parse_args()


def die(fmt, *args):
    if not len(args):
        text = str(fmt)
    else:
        text = fmt % args
    print('error:', text, file=sys.stderr)
    sys.exit(1)


# In case they gave a full path:

file_name = os.path.basename(args.file_name)

# Let's do it.

client = hera_librarian.LibrarianClient(args.conn_name)

try:
    result = client.set_one_file_deletion_policy(file_name, args.deletion,
                                                 restrict_to_store=args.store)
except hera_librarian.RPCError as e:
    die('couldn\'t alter policy: %s', e)
