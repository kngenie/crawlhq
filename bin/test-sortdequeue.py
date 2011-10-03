import sys, os
import logging
from optparse import OptionParser

sys.path[0:0] = (os.path.realpath(os.path.join(sys.path[0], '../lib')),)
from sortdequeue import *
from cfpgenerator import FPGenerator

opt = OptionParser()
opt.add_option('-v', action='store_const', dest='loglevel', const=logging.DEBUG,
               default=logging.INFO,
               help='set loglevel to DEBUG')
options, args = opt.parse_args()
logging.basicConfig(level=options.loglevel)

_fp64 = FPGenerator(0xD74307D3FD3382DB, 64)
def urikey(o):
    return _fp64.sfp(o['u'])
if len(args) > 1:
    r = MergeSortingQueueFileReader(args, urikey, noupdate=True)
else:
    r = SortingQueueFileReader(args[0], urikey, noupdate=True)
for o in r:
    print o
print >>sys.stderr, str(r.get_status())
