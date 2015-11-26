#!/usr/bin/python
"""
Input a list of files and insert into the librarian.
The files must exist and be findable on the filesystem
NB filenames must be FULL PATH. If the root is not '/' for all files it will exit

KEY NOTE: Assumes all files are contiguous.  I sort the files by jd and then match up neighboring pols as neighbors for the
   ddr algorithm

"""


from ddr_compress.dbi import gethostname, jdpol2obsnum
import optparse,os,sys,re,numpy as n

import hera_librarian

# from /a/b/c/d, return c/d
def dirfilename(path):
    x = os.path.split(path)
    y = os.path.split(x[0])
    return os.path.join(y[1], x[1]);

def file2jd(zenuv):
    return re.findall(r'\d+\.\d+', zenuv)[0]
def file2pol(zenuv):
    return re.findall(r'\.(.{2})\.',zenuv)[0]
o = optparse.OptionParser()
o.set_usage('add_observations.py *.uv')
o.set_description(__doc__)
o.add_option('--length',type=float,
        help='length of the input observations in minutes [default=average difference between filenames]')
o.add_option('-t',action='store_true',
       help='Test. Only print, do not touch db')
o.add_option('--overwrite',action='store_true',
    help='Default action is to skip obsrvations already in the db. Setting this option overrides this safety feature and attempts anyway')
opts, args = o.parse_args(sys.argv[1:])

#check that all files exist
for filename in args:
    assert(filename.startswith('/'))
    assert(os.path.exists(filename))
#now run through all the files and build the relevant information for the db
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

site_name = 'Karoo'
store_name = 'pot2_data1'
for filename in args:
    jd = float(file2jd(filename))
    pol = file2pol(filename)
    fname = dirfilename(filename)
    obsnum = jdpol2obsnum(jd, pol, djd)
    print jd, pol, djd, obsnum
    hera_librarian.create_observation(site_name, obsnum, jd, pol, djd)
    hera_librarian.create_file(site_name, store_name, fname, "uv", obsnum, -1, '')

print "done"
