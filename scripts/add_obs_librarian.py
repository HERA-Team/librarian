#!/usr/bin/env python
"""add_obs_librarian.py /path/to/file1 /path/to/file2 ...

Register a list of files with the librarian. The paths must exist and must be
absolute (i.e., begin with "/").

"""
from ddr_compress.dbi import gethostname, jdpol2obsnum
import optparse
import os
import sys
import re
import glob
import numpy as n
from astropy.time import Time
import aipy as a
import hera_librarian

# from /a/b/c/d, return c/d


def file2jd(zenuv):
    return re.findall(r'\d+\.\d+', zenuv)[0]


def file2pol(zenuv):
    return re.findall(r'\.(.{2})\.', zenuv)[0]

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


def obsid_from_file(filename):
    # Get the obsnum from the file
    # return None if not found
    uv = a.miriad.UV(filename)
    try:
        return uv['obsid']
    except(KeyError):
        return None


def obsid_from_filename(filename):
    # get the obsid from the file NAME
    # only do this if we don't have a obsid in the file
    jd = file2jd(filename)
    return n.floor(Time(float(jd), scale='utc', format='jd').gps)

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
for filename in args:
    files = glob.glob(opts.store_path+filename)
    for f in files:
        full_filepaths.append(f)

for filename in full_filepaths:
    if not os.path.exists(filename):
        print >>sys.stderr, 'error: file argument %r does not exist' % (filename,)
        errors = True

if errors:
    sys.exit(1)

client = hera_librarian.LibrarianClient(opts.site)

for filename in full_filepaths:
    start_jd = float(file2jd(filename))
    obsid = obsid_from_file(filename)
    filetype = filename.split('.')[-1]
    if obsid is None:
        obsid = obsid_from_filename(filename)
    print start_jd, obsid
    try:
        client.create_observation(obsid, start_jd)
    except hera_librarian.RPCFailedError as e:
        print >>sys.stderr, 'failed to create observation record %s: %s' % (filename, e)

    try:
        print opts.store, filename, filetype, obsid, -1, ''
        client.create_file(opts.store, filename, filetype, obsid, -1, '')

    except hera_librarian.RPCFailedError as e:
        print >>sys.stderr, 'failed to create file record %s: %s' % (filename, e)
print "done"
