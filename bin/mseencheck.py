#!/usr/bin/python
#
import sys
import os
import re
import struct

from optparse import OptionParser

opt = OptionParser()
options, args = opt.parse_args()

seenfn, = args

reccount = 0
errorcount = 0
firstuid = None
lastuid = None

with open(seenfn) as f:
    while 1:
        pos = f.tell()
        rec = f.read(8)
        reccount += 1
        if rec == '':
            break
        elif len(rec) < 8:
            print >>sys.stderr, "invalid record of length %d" % (len(rec),)
            errorcount += 1
            break
        uid = struct.unpack('l', rec)[0]
        if firstuid is None: firstuid = uid
        if lastuid is not None:
            if uid < lastuid:
                print >>sys.stderr, "out of order record: %d at %d after %d" \
                    % (uid, pos, lastuid)
                errorcount += 1
            elif uid == lastuid:
                print >>sys.stderr, "duplicate record %d at %d" % (uid, pos)
                errorcount += 1
        lastuid = uid

print >>sys.stderr, "%d records, %d errors" % (reccount, errorcount)
print >>sys.stderr, "first %d, last %d" % (firstuid, lastuid)



