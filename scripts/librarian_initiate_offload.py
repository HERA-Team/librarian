#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2017 the HERA Team.
# Licensed under the BSD License.

"""Initiate an "offload": move a bunch of file instances from one store to
another. This tool is intended for very specialized circumstances: when you
are trying to clear out a store so that it can be shut down.

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
p.add_argument('source_name', metavar='SOURCE-NAME',
               help='The name of the source store.')
p.add_argument('dest_name', metavar='DEST-NAME',
               help='The name of the destination store.')

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

try:
    result = client.initiate_offload(args.source_name, args.dest_name)
except hera_librarian.RPCError as e:
    die('offload failed to launch: %s', e)

if 'outcome' not in result:
    die('malformed server response (no "outcome" field): %s', repr(result))

if result['outcome'] == 'store-shut-down':
    print('The store has no file instances needing offloading. It was placed offline and may now be closed out.')
elif result['outcome'] == 'task-launched':
    print('Task launched, intending to offload %s instances.' %
          (result.get('instance-count', '???')))
    print()
    print('A noop-ified command to delete offloaded instances from the source store is:')
    print("  librarian_delete_files.py --noop --store '%s' '%s' '{\"at-least-instances\": 2}'" %
          (args.source_name, args.conn_name))
else:
    die('malformed server response (unrecognized "outcome" field): %s', repr(result))
