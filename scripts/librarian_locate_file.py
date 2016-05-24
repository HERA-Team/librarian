#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Team.
# Licensed under the BSD License.

"""Ask the Librarian where to find a file. The file location is returned
as an SCP-ready string of the form "<host>:<full-path-on-host>".

"""
from __future__ import absolute_import, division, print_function

import optparse, os.path, sys

import hera_librarian


o = optparse.OptionParser()
o.set_usage('librarian_locate_file.py <connection> <file-name>')
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

if len (args) != 2:
    die ('expect exactly two non-option arguments')

connection, file_name = args
file_name = os.path.basename (file_name) # in case it's got directory components


# Let's do it.

client = hera_librarian.LibrarianClient (connection)

try:
    result = client.locate_file_instance (file_name)
except hera_librarian.RPCError as e:
    die ('couldn\'t locate file: %s', e)

print ('%(store_ssh_host)s:%(full_path_on_store)s' % result)
