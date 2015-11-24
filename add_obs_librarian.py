#!/usr/bin/python
"""
Input a list of files and insert into the librarian.
The files must exist and be findable on the filesystem
NB filenames must be FULL PATH. If the root is not '/' for all files it will exit

KEY NOTE: Assumes all files are contiguous.  I sort the files by jd and then match up neighboring pols as neighbors for the
   ddr algorithm

"""


from ddr_compress.dbi import DataBaseInterface,gethostname,jdpol2obsnum
import optparse,os,sys,re,numpy as n

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
#connect to the database
dbi = DataBaseInterface()

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

site_name = '"Karoo'
store_name = 'Store 0'
for filename in args:
    jd = float(file2jd(filename))
    pol = file2pol(filename)
    obsnum = jdpol2obsnum(jd, pol, djd)
    create_observation(site_name, obsnum, jd, pol, djd)
    create_file(site_name, store_name, "uv", obsnum, -1, '')

sys.exit(0)

pols = list(set(pols))#these are the pols I have to iterate over
print "found the following pols",pols
print "found the following nights",nights
for night in nights:
    print "adding night" ,night
    obsinfo = []
    nightfiles = [filename for filename in args if int(float(file2jd(filename)))==night]
    print len(nightfiles)
    for pol in pols:
        files = [filename for filename in nightfiles if file2pol(filename)==pol]#filter off all pols but the one I'm currently working on
        files.sort()
        for i,filename in enumerate(files):
            obsnum = jdpol2obsnum(float(file2jd(filename)),file2pol(filename),djd)
            print "obs num: ", obsnum
            
#            try:
#                dbi.get_obs(jdpol2obsnum(float(file2jd(filename)),file2pol(filename),djd))
#                if opts.overwrite:
#                    raise(StandardError)
#                print filename, "found in db, skipping"
#            except:
            obsinfo.append({
                'julian_date' : float(file2jd(filename)),
                'pol'     :     file2pol(filename),
                'host' :        gethostname(),
                'filename' :    filename,
                'length'  :     djd #note the db likes jd for all time units
                    })
    for i,obs in enumerate(obsinfo):
        filename = obs['filename']
        if i!=0:
            if n.abs(obsinfo[i-1]['julian_date']-obs['julian_date'])<(djd*1.2):
                obsinfo[i].update({'neighbor_low':obsinfo[i-1]['julian_date']})
        if i!=(len(obsinfo)-1):
            if n.abs(obsinfo[i+1]['julian_date']-obs['julian_date'])<(djd*1.2):
                obsinfo[i].update({'neighbor_high':obsinfo[i+1]['julian_date']})
    #assert(len(obsinfo)==len(args))
    if opts.t:
        print "NOT ADDING OBSERVATIONS TO DB"
        print "HERE is what would have been added"
        for obs in obsinfo:
            print obs['filename'],jdpol2obsnum(obs['julian_date'],obs['pol'],obs['length']),
            print "neighbors",obs.get('neighbor_low',None),obs.get('neighbor_high',None)
    elif len(obsinfo)>0:
        print "adding {len} observations to the still db".format(len=len(obsinfo))
#        try:
#            dbi.add_observations(obsinfo)
#        except:
#            print "problem!"
        #dbi.test_db()
print "done"
