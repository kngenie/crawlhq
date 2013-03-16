#!/usr/bin/python
import sys, os
LIBDIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib'))
if LIBDIR not in sys.path: sys.path.append(LIBDIR)
import seen
import hqconfig
from optparse import OptionParser

opt = OptionParser('%prog [OPTIONS] JOBNAME')
opt.add_option('--binary', action='store_const', const='binary',
               dest='format', default='text')
options, args = opt.parse_args()
if len(args) < 1:
  opt.error('specify job name')

job = args[0]

seendb = seen.SeenFactory()(job)
seendb.dump(format=options.format)
