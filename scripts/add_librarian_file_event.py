#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Team.
# Licensed under the BSD License.

"""Add an "event" record for a File known to the Librarian. These records are
essentially freeform. The event data are specified as "key=value" items on the
command line after the event type. The data are parsed as JSON before being
sent to the Librarian. This makes it possible to use data structures like
JSON-format lists, dictionaries, and so on. However, when the data item is
intended to simply be a string, make sure that it comes through the shell as a
JSON string by writing it like 'foo=\\"bar\\"' (without the single quotes).

"""
from __future__ import absolute_import, division, print_function

import json
import optparse
import os.path
import sys

import hera_librarian
from hera_librarian import utils


def die(fmt, *args):
    if not len(args):
        text = str(fmt)
    else:
        text = fmt % args
    print('error:', text, file=sys.stderr)
    sys.exit(1)


# Deal with arguments.

o = optparse.OptionParser()
o.set_usage('add_librarian_file_event.py <connection-name> <path/to/file> <event-type> <key=val1...>')
o.set_description(__doc__)
opts, args = o.parse_args(sys.argv[1:])

if len(args) < 4:
    die('expect exactly at least four non-option arguments')

conn_name, path, event_type = args[:3]

payload = {}

for arg in args[3:]:
    bits = arg.split('=', 1)
    if len(bits) != 2:
        die('argument %r must take the form "key=value"', arg)

    # We parse each "value" as JSON ... and then re-encode it as JSON when
    # talking to the server. So this is mostly about sanity checking.

    key, text_val = bits
    try:
        value = json.loads(text_val)
    except ValueError:
        die('value %r for keyword %r does not parse as JSON', text_val, key)

    payload[key] = value

path = os.path.basename(path)  # in case user provided a real filesystem path


# Let's do it.

client = hera_librarian.LibrarianClient(conn_name)

try:
    client.create_file_event(path, event_type, **payload)
except hera_librarian.RPCError as e:
    die('event creation failed: %s', e)
