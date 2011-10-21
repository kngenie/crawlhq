#!/usr/bin/python
# test for figuring out optimal batch size for IncomingQueue compression.
#
import os
import sys
from optparse import OptionParser
from gzip import GzipFile
import tempfile

opt = OptionParser(usage='%prog QFILE')
options, args = opt.parse_args()

if len(args) < 1:
    opt.error('specify QFILE')

lines = 1
increment = 1
f = open(args[0])

eof = False
while not eof:
    t = tempfile.NamedTemporaryFile()
    z = GzipFile(fileobj=t, mode='wb')
    f.seek(0, 0)
    for i in xrange(lines):
        l = f.readline()
        if not l:
            eof = True
            break
        z.write(l)
    z.close()
    osize = f.tell()
    csize = t.tell()
    t.close()

    print "%d %d->%d (%.3f)" %  (lines, osize, csize, csize/float(osize))

    lines += increment

