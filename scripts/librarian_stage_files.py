#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2017 the HERA Team.
# Licensed under the BSD License.

"""Tell the Librarian to stage files onto the local scratch disk. At NRAO,
this is the Lustre filesystem.

"""
from __future__ import absolute_import, division, print_function

import argparse
import os.path
import sys
import time

import hera_librarian


p = argparse.ArgumentParser(
    description=__doc__,
    epilog="""For documentation of the JSON search format, see
https://github.com/HERA-Team/librarian/blob/master/docs/Searching.md . Wrap
your JSON in single quotes to prevent your shell from trying to interpret the
special characters. """
)

p.add_argument('-w', '--wait', dest='wait', action='store_true',
               help='If specified, do not exit until the staging is done.')
p.add_argument('conn_name', metavar='CONNECTION-NAME',
               help='Which Librarian to talk to; as in ~/.hl_client.cfg.')
p.add_argument('dest_dir', metavar='DEST-PATH',
               help='What directory to put the staged files in.')
p.add_argument('search', metavar='JSON-SEARCH',
               help='A JSON search specification; files that match will be staged.')
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

# Get the username. We could make this a command-line option but I think it's
# better to keep this a semi-secret. Note that the server does absolutely no
# verification of the values that are passed in.

import getpass
user = getpass.getuser()

# Resolve the destination in case the user provides, say, `.`, where the
# server is not going to know what that means. This will need elaboration if
# we add options for the server to come up with a destination automatically or
# other things like that.
our_dest = os.path.realpath(args.dest_dir)

try:
    result = client.launch_local_disk_stage_operation(user, args.search, our_dest)
except hera_librarian.RPCError as e:
    die('couldn\'t start the stage operation: %s', e)

# This is a bit of future-proofing; we might teach the Librarian to choose a
# "reasonable" output directory on your behalf.
dest = result['destination']

print('Launched operation to stage %d instances (%d bytes) to %s' % (
    result['n_instances'], result['n_bytes'], dest))

if not args.wait:
    print('Operation is complete when %s/STAGING-IN-PROGRESS is removed.' % dest)
else:
    # The API call should not return until the progress-marker file is
    # created, so if we don't see that it exists, it should be the case that
    # the staging started and finished before we could check.

    if not os.path.isdir(dest):
        die('cannot wait for staging to complete: destination directory %s not '
            'visible on this machine. Missing network filesystem?', dest)

    marker_path = os.path.join(dest, 'STAGING-IN-PROGRESS')
    t0 = time.time()
    print('Started waiting for staging to finish at:', time.asctime(time.localtime(t0)))

    while os.path.exists(marker_path):
        time.sleep(3)

    if os.path.exists(os.path.join(dest, 'STAGING-SUCCEEDED')):
        print('Staging completed successfully (%.1fs elapsed).' % (time.time() - t0))
        sys.exit(0)

    try:
        with open(os.path.join(dest, 'STAGING-ERRORS'), 'rt') as f:
            print('Staging completed WITH ERRORS (%.1fs elapsed).' %
                  (time.time() - t0), file=sys.stderr)
            print('', file=sys.stderr)
            for line in f:
                print(line.rstrip(), file=sys.stderr)
        sys.exit(1)
    except IOError as e:
        if e.errno == 2:
            die('staging finished but neither \"success\" nor \"error\" indicator was '
                'created (no file %s)', os.path.join(dest, 'STAGING-ERRORS'))
        raise
