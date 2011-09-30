import sys, os
import logging

logging.basicConfig(level=logging.DEBUG)

sys.path[0:0] = (os.path.realpath(os.path.join(sys.path[0], '../lib')),)

from sortdequeue import *
from cfpgenerator import FPGenerator
_fp64 = FPGenerator(0xD74307D3FD3382DB, 64)
def urikey(o):
    return _fp64.sfp(o['u'])
if len(sys.argv) > 2:
    r = MergeSortingQueueFileReader(sys.argv[1:], urikey, noupdate=True)
else:
    r = SortingQueueFileReader(sys.argv[1], urikey, noupdate=True)
for o in r:
    print o
print >>sys.stderr, str(r.get_status())
