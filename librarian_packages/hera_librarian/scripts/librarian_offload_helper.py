#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2017 the HERA Team.
# Licensed under the BSD License.

"""The Librarian launches this script on stores to implement the "offload"
functionality. Regular users should never need to run it.

"""
from __future__ import absolute_import, division, print_function

import argparse
import os.path
import sys

from hera_librarian import store


p = argparse.ArgumentParser(
    description=__doc__,
)

p.add_argument('--name', required=True, help='Displayed name of the destination store.')
p.add_argument('--pp', required=True, help='"Path prefix" of the destination store.')
p.add_argument('--host', required=True, help='Target SSH host of the destination store.')
p.add_argument('--destrel', required=True, help='Destination path, relative to the path prefix.')
p.add_argument('local_path', metavar='LOCAL-PATH',
               help='The name of the file to upload on this machine.')

args = p.parse_args()


def die(fmt, *args):
    if not len(args):
        text = str(fmt)
    else:
        text = fmt % args
    print('error:', text, file=sys.stderr)
    sys.exit(1)


# Due to how the Librarian has to arrange things, it's possible that the
# instance that we want to copy was deleted before this script got run. If so,
# so be it -- don't signal an error.

if not os.path.exists(args.local_path):
    print('source path %s does not exist -- doing nothing' % args.local_path)
    sys.exit(0)

# The rare librarian script that does not use the LibrarianClient class!

try:
    dest = store.Store(args.name, args.pp, args.host)
    dest.copy_to_store(args.local_path, args.destrel)
except Exception as e:
    die(e)
