#!/usr/bin/python

import sys
sys.path[0:0] = ('/opt/hq/lib',)
import os
import re
from fileinq import IncomingQueue
import pymongo
from threading import Thread, RLock

QUEUE_DIRECTORY = '/1/incoming/hq-test'

conn = pymongo.Connection()
db = conn.crawl
collinq = db.inq.wide

class MongoBucketReader(object):
    def __init__(self):
        self.conn = pymongo.Connection()
        self.db = self.conn.crawl
        self.collinq = self.db.inq.wide
        self.readlock = RLock()
        self.cur = self.collinq.find(dict(q={'$gt':0}))
        self.count = 0
    def __iter__(self):
        return self
    def next(self):
        with self.readlock:
            bucket = next(self.cur)
            self.count += 1
            return bucket

class SequenceBucketReader(object):
    def __init__(self, total, step):
        self.total = total
        self.step = step
        self.readlock = RLock()
        self.count = 0
        self.seq = 0
    def __iter__(self):
        return self
    def next(self):
        if self.count >= self.total:
            raise StopIteration
        with self.readlock:
            d = []
            for i in xrange(self.step):
                d.append(dict(u='http://example.com/%d' % self.seq))
                self.seq += 1
            self.count += 1
            return dict(d=d)

class Emitter(Thread):
    def __init__(self, buckets, inq):
        Thread.__init__(self)
        self.buckets = buckets
        self.inq = inq
        self.count = 0
    def run(self):
        for bucket in self.buckets:
            self.inq.add(bucket['d'])
            self.count += 1

inq = IncomingQueue('wide', QUEUE_DIRECTORY)
#bucketreader = BucketReader()
bucketreader = SequenceBucketReader(100, 100)
emitters = [Emitter(bucketreader, inq) for i in range(3)]
for e in emitters:
    e.start()
    
for e in emitters:
    while 1:
        e.join(1.0)
        if not e.is_alive(): break
        sys.stderr.write('\r%d %s' % (bucketreader.count, inq.get_status()))
sys.stderr.write('\n')
inq.close()

# reading queue out
nread = 0
while 1:
    o = inq.get(timeout=0.01)
    if o is None: break
    nread += 1

print >>sys.stderr, "read out %d items" % nread

    

