#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Team.
# Licensed under the BSD License.

"""Request to delete instances of files matching a given query.

"""
from __future__ import absolute_import, division, print_function

import argparse
import os.path
import sys

import hera_librarian


p = argparse.ArgumentParser(
    description=__doc__,
    epilog="""The deletion requests may not be honored depending on the deletion policies
    associated with the various instances of the files.

    Be careful about shell escaping when specifying the query string.

    Note that the "instances" of files are deleted, but the records with
    information about the files remain.

    """
)

p.add_argument('-n', '--noop', dest='noop', action='store_true',
               help='Enable no-op mode: nothing is actually deleted.')
p.add_argument('--store', metavar='STORE-NAME',
               help='Only delete instances found on the named store.')
p.add_argument('conn_name', metavar='CONNECTION-NAME',
               help='Which Librarian to talk to; as in ~/.hl_client.cfg.')
p.add_argument('query', metavar='QUERY',
               help='The JSON-formatted search identifying files to delete.')
args = p.parse_args()


def die(fmt, *args):
    if not len(args):
        text = str(fmt)
    else:
        text = fmt % args
    print('error:', text, file=sys.stderr)
    sys.exit(1)


def str_or_huh(x):
    if x is None:
        return '???'
    return str(x)


# Let's do it.

client = hera_librarian.LibrarianClient(args.conn_name)

if args.noop:
    print('No-op mode enabled: files will not actually be deleted.')
    print()
    itemtext = 'todelete'
    summtext = 'would have been deleted'
    mode = 'noop'
else:
    itemtext = 'deleted'
    summtext = 'were deleted'
    mode = 'standard'

try:
    result = client.delete_file_instances_matching_query(args.query,
                                                         mode=mode,
                                                         restrict_to_store=args.store)
    allstats = result['stats']
except hera_librarian.RPCError as e:
    die('multi-delete failed: %s', e)

n_files = 0
n_noinst = 0
n_deleted = 0
n_error = 0

for fname, stats in sorted(allstats.iteritems(), key=lambda t: t[0]):
    nd = stats.get('n_deleted', 0)
    nk = stats.get('n_kept', 0)
    ne = stats.get('n_error', 0)

    if nd + nk + ne == 0:
        # This file had no instances. Don't bother printing it.
        n_noinst += 1
        continue

    n_files += 1
    n_deleted += nd
    n_error += ne
    deltext = str_or_huh(stats.get('n_deleted'))
    kepttext = str_or_huh(stats.get('n_kept'))
    errtext = str_or_huh(stats.get('n_error'))

    print('%s: %s=%s kept=%s error=%s' % (fname, itemtext, deltext, kepttext, errtext))

if n_files:
    print('')
print('%d files were matched, %d had instances; %d instances %s'
      % (n_files + n_noinst, n_files, n_deleted, summtext))

if n_error:
    print('WARNING: %d error(s) occurred; see server logs for information' % n_error)
    sys.exit(1)
