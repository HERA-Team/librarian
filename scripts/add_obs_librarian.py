#!/usr/bin/env python
"""add_obs_librarian.py /path/to/file1 /path/to/file2 ...

Register a list of files with the librarian. The paths must exist and must be
absolute (i.e., begin with "/").

"""
import optparse
import os.path
import sys
import re
import glob
import numpy as n
from astropy.time import Time
import aipy as a
import hera_librarian
from hera_librarian import utils


o = optparse.OptionParser()
o.set_usage('add_obs_librarian.py *.uv')
o.set_description(__doc__)
o.add_option('-t', action='store_true',
             help='Test. Only print, do not touch db')
o.add_option('--overwrite', action='store_true',
             help='Default action is to skip obsrvations already in the db. ' +
             'Setting this option overrides this safety feature and ' +
             'attempts anyway')
o.add_option('--site', type=str, default='Karoo',
             help='The "site" name to use when creating the new records ' +
             '(default: %default).')
o.add_option('--store', type=str, default='pot2_data1',
             help='The "store" name to use when creating the new records ' +
             '(default: %default).')
o.add_option('--store_path', type=str, default=None,
             help='The store paths, to be prepended to the filenames to get ' +
             'the full path')
opts, args = o.parse_args(sys.argv[1:])


# check that all files exist
errors = False

if opts.store_path is None:
    print >>sys.stderr, 'error: store_path must be set'
    errors = True

if not opts.store_path.startswith('/'):
    print >>sys.stderr, 'error: store_path must be an absolute path; got %r' % (opts.store_path,)
    errors = True

if not opts.store_path.endswith('/'):
    opts.store_path = opts.store_path + '/'

full_filepaths = []
files = []
for filename in args:
    full_files = glob.glob(opts.store_path+filename)
    for f in full_files:
        full_filepaths.append(f)
        files.append(f[len(opts.store_path):])

for filename in full_filepaths:
    if not os.path.exists(filename):
        print >>sys.stderr, 'error: file argument %r does not exist' % (filename,)
        errors = True

if errors:
    sys.exit(1)

client = hera_librarian.LibrarianClient(opts.site)

for i, filename in enumerate(files):
    full_filename = full_filepaths[i]

    start_jd = utils.get_start_jd_from_path (full_filename)
    type = utils.get_type_from_path (full_filename)
    obsid = utils.get_obsid_from_path (full_filename)
    size = utils.get_size_from_path (full_filename)
    md5 = utils.get_md5_from_path (full_filename)

    print start_jd, obsid

    try:
        client.create_file_instance(opts.store, full_filename[len(opts.store_path):])
    except hera_librarian.RPCFailedError as e:
        print >>sys.stderr, 'failed to create instance record %s: %s' % (filename, e)

print "done"
