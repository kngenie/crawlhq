#!/usr/bin/python
#
import sys, os
sys.path[0:0] = (os.path.join(sys.path[0], '../lib'),)
import pymongo
import snappy

conn = pymongo.Connection()
db = conn.crawl

count = 0
osumlen = 0
sumlen = 0
cur = db.seen.wide.find().limit(50000)
for d in cur:
    count += 1
    osumlen += len(d['u'])
    sumlen += len(snappy.compress(d['u']))

print "count=%d, osumlen=%d, sumlen=%d" % (count, osumlen, sumlen)
print "avg olen=%.2f avg len=%.2f" % (float(osumlen) / count,
                                      float(sumlen) / count)

    
