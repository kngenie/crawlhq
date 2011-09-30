#!/usr/bin/python
import sys, os
sys.path.append('/opt/hq/lib')
import leveldb
import struct

if len(sys.argv) < 2:
  print >>sys.stderr, "specify job name"
  exit(1)
job = sys.argv[1]

h = leveldb.IntHash(os.path.join('/1/crawling/hq/seen', job))
it = h.new_iterator()
it.seek_to_first()
while it.valid():
    it.next()
    if not it.valid(): break
    k, = struct.unpack('l', it.key())
    print k
del it
del h

