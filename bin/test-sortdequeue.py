import sys, os
import logging

logging.basicConfig(level=logging.INFO)

sys.path[0:0] = (os.path.realpath(os.path.join(sys.path[0], '../lib')),)

from sortdequeue import SortingQueueFileReader
from cfpgenerator import FPGenerator
_fp64 = FPGenerator(0xD74307D3FD3382DB, 64)
class FPSortingQueueFileReader(SortingQueueFileReader):
    def urikey(self, o):
        return _fp64.sfp(o['u'])
r = FPSortingQueueFileReader(sys.argv[1], noupdate=True)
for o in r:
    print o
print >>sys.stderr, str(r.get_status())
