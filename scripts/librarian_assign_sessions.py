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

import optparse, os.path, sys

import hera_librarian


o = optparse.OptionParser()
o.set_usage('librarian_assign_sessions.py <connection>')
o.set_description(__doc__)
opts, args = o.parse_args(sys.argv[1:])


def die (fmt, *args):
    if not len (args):
        text = str (fmt)
    else:
        text = fmt % args
    print ('error:', text, file=sys.stderr)
    sys.exit (1)


# Argument validation is pretty simple

if len (args) != 1:
    die ('expect exactly one non-option argument')

connection = args[0]


# Let's do it.

client = hera_librarian.LibrarianClient (connection)

try:
    result = client.assign_observing_sessions ()
except hera_librarian.RPCError as e:
    die ('assignment failed: %s', e)

try:
    n = 0

    for info in result['new_sessions']:
        if n == 0:
            print ('New sessions created:')
        print ('  %(id)d: start JD %(start_time_jd)f, stop JD %(stop_time_jd)f, n_obs %(n_obs)d' % info)
        n += 1

    if n == 0:
        print ('No new sessions created.')
except Exception as e:
    die ('sessions created, but failed to print info: %s', e)
