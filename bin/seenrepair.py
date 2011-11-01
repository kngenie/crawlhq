#!/usr/bin/python
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '../lib'))
from optparse import OptionParser
import hqconfig
import leveldb

opt = OptionParser('%prog JOB')
opt.add_option('-f', action='store_true', dest='force', default=False,
               help='force repair even if this script guessed it is not a'
               'valid level db directory')
options, args = opt.parse_args()
if len(args) == 0:
    opt.error('specify job name')
job = args[0]    
seendir = hqconfig.seendir(job)
if not os.path.isdir(seendir):
    opt.error('%s is not a directory')
if not os.path.isfile(os.path.join(seendir, 'CURRENT')):
    print >>sys.stderr, "%s does not seem to be a LevelDB directory" % seendir
    if not options.force:
        exit(1)
seendir_owner = os.stat(seendir).st_uid
if seendir_owner != os.geteuid():
    print >>sys.stderr, "%s owner (%d) does not match effective user (%d)" % (
        seendir, seendir_owner, os.geteuid())
    if not options.force:
        exit(1)
leveldb.IntHash.repair_db(hqconfig.seendir(job))

