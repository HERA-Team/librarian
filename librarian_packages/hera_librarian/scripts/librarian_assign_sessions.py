#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Team.
# Licensed under the BSD License.

"""Tell the Librarian to assign any recent Observations to grouped "observing
sessions". You should only do this if no data are currently being taken,
because otherwise the currently-active session will be incorrectly described.
The RTP only ingests data from observations that have been assigned to
sessions, so this command must be run before the RTP will start working on a
night's data.

"""
from __future__ import absolute_import, division, print_function

import argparse
import os.path
import sys

import hera_librarian


p = argparse.ArgumentParser(
    description=__doc__,
)
p.add_argument('--min-start-jd', dest='minimum_start_jd', metavar='JD', type=float,
               help='Only consider observations starting after JD.')
p.add_argument('--max-start-jd', dest='maximum_start_jd', metavar='JD', type=float,
               help='Only consider observations starting before JD.')
p.add_argument('conn_name', metavar='CONNECTION-NAME',
               help='Which Librarian to talk to; as in ~/.hl_client.cfg.')

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
    result = client.assign_observing_sessions(
        minimum_start_jd=args.minimum_start_jd,
        maximum_start_jd=args.maximum_start_jd,
    )
except hera_librarian.RPCError as e:
    die('assignment failed: %s', e)

try:
    n = 0

    for info in result['new_sessions']:
        if n == 0:
            print('New sessions created:')
        print('  %(id)d: start JD %(start_time_jd)f, stop JD %(stop_time_jd)f, n_obs %(n_obs)d' % info)
        n += 1

    if n == 0:
        print('No new sessions created.')
except Exception as e:
    die('sessions created, but failed to print info: %s', e)
