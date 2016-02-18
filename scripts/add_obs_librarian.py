#!/usr/bin/env python
"""add_obs_librarian.py /path/to/file1 /path/to/file2 ...

Register a list of files with the librarian. The paths must exist and must be
absolute (i.e., begin with "/").

"""
from ddr_compress.dbi import gethostname, jdpol2obsnum
import optparse, os, sys, re, numpy as n
from astropy.time import Time
import aipy as a
import hera_librarian

# from /a/b/c/d, return c/d
#
def dirfilename(path):
    x = os.path.split(path)
    y = os.path.split(x[0])
    return os.path.join(y[1], x[1]);

def file2jd(zenuv):
    return re.findall(r'\d+\.\d+', zenuv)[0]
def file2pol(zenuv):
    return re.findall(r'\.(.{2})\.',zenuv)[0]

o = optparse.OptionParser()
o.set_usage('add_obs_librarian.py *.uv')
o.set_description(__doc__)
o.add_option('--length',type=float,
        help='length of the input observations in minutes [default=average difference between filenames]')
o.add_option('-t',action='store_true',
       help='Test. Only print, do not touch db')
o.add_option('--overwrite',action='store_true',
    help='Default action is to skip obsrvations already in the db. Setting this option overrides this safety feature and attempts anyway')
o.add_option('--site',type=str,default='Karoo',
             help='The "site" name to use when creating the new records (default: %default).')
o.add_option('--store',type=str,default='pot2_data1',
             help='The "store" name to use when creating the new records (default: %default).')
opts, args = o.parse_args(sys.argv[1:])

def obsid_from_file(filename):
    # Get the obsnum from the file
    # return None if not found
    uv  = a.miriad.UV(filename)
    try:
        return uv['obsid']
    except(KeyError):
	return None
def obsid_from_filename(filename):
    #get the obsid from the file NAME
    # only do this if we don't have a obsid in the file
    jd = file2jd(filename)
    return n.floor(Time(float(jd),scale='utc',format='jd').gps)
# check that all files exist
for filename in args:
    assert(filename.startswith('/'))
    assert(os.path.exists(filename))

# now run through all the files and build the relevant information for the db
# get the pols
pols = []
jds = []
for filename in args:
    pols.append(file2pol(filename))
    jds.append(float(file2jd(filename)))
jds = n.array(jds)
nights = list(set(jds.astype(n.int)))
if not opts.length is None:
    djd =  opts.length/60./24
else:
    jds_onepol = n.sort([jd for i,jd in enumerate(jds) if pols[i]==pols[0] and jd.astype(int)==nights[0]])
    djd = n.mean(n.diff(jds_onepol))
    print "setting length to ",djd,' days'

client = hera_librarian.LibrarianClient (opts.site)

for filename in args:
    jd = float(file2jd(filename))
    pol = file2pol(filename)
    fname = dirfilename(filename)
    #obsnum = jdpol2obsnum(jd, pol, djd)
    obsid = obsid_from_file(filename)
    if obsid is None:
        obsid = obsid_from_filename(filename)
    print jd, pol, djd, obsid
    try:
        client.create_observation(obsid, jd, pol, djd)
    except hera_librarian.RPCFailedError as e:
        print >>sys.stderr, 'failed to create observation record %s: %s' % (filename, e)

    try:
        client.create_file(opts.store, fname, "uv", obsid, -1, '')
    except hera_librarian.RPCFailedError as e:
        print >>sys.stderr, 'failed to create file record %s: %s' % (filename, e)
print "done"
