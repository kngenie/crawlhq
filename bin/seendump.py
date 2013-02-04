#!/usr/bin/python
import sys, os
LIBDIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib'))
if LIBDIR not in sys.path: sys.path.append(LIBDIR)
import leveldb
import struct
import hqconfig

if len(sys.argv) < 2:
  print >>sys.stderr, "specify job name"
  exit(1)
job = sys.argv[1]

h = leveldb.IntHash(hqconfig.seendir(job))
try:
  it = h.new_iterator()
  it.seek_to_first()
  while it.valid():
      it.next()
      if not it.valid(): break
      k, = struct.unpack('l', it.key())
      print k
  del it
finally:
  del h
