#!/usr/bin/python
import sys, os
sys.path.append('/opt/hq/lib')
import leveldb
import struct

h = leveldb.IntHash('/1/crawling/hq/seen')
it = h.new_iterator()
it.seek_to_first()
while it.valid():
    it.next()
    if not it.valid(): break
    k, = struct.unpack('l', it.key())
    print k
del it
del h

