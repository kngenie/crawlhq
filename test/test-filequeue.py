#!/usr/bin/python
#
import sys
import os
from fixture import *
import unittest
import json
import time

from filequeue import *

DATADIR = '/tmp/hq'
if not os.path.isdir(DATADIR):
    os.makedirs(DATADIR)

class FileQueueTestCase(unittest.TestCase):
    def setUp(self):
        self.datadir = TestDatadir()
        self.datadir.cleanup()
    def tearDown(self):
        self.datadir = None

    def testPlainWrite(self):
        q = FileEnqueue(self.datadir.path, gzip=0)
        data = dict(a=1, b=2, c=3)
        q.queue(data)
        q.close()
        
        fn = os.listdir(self.datadir.path)[0]
        assert not fn.endswith('.open'), '%s has .open suffix' % fn
        with open(os.path.join(self.datadir.path, fn)) as f:
            j = json.loads(f.readline().rstrip()[1:])
            assert j == data, 'expected %s, got %s' % (data, j)

            assert f.readline() == '', 'expected EOF'
        
    def testReadResume(self):
        data = [ dict(id=i) for i in xrange(10) ]
        q = FileEnqueue(self.datadir.path, gzip=0)
        for d in data:
            q.queue(d)
        q.close()
        r = FileDequeue(self.datadir.path)
        for i in xrange(5):
            d = r.get()
            assert d == data[i], 'expected %s, got %s' % (data[i], d)
        # close, reopen the same queue. it should start reading
        # from the next item of the last read.
        r.close()
        r = FileDequeue(self.datadir.path)
        for i in xrange(5, 10):
            d = r.get()
            assert d == data[i], 'expected %s, got %s' % (data[i], d)
        r.close()

    def testReadGzipped(self):
        data = [ dict(id=i) for i in xrange(10) ]
        q = FileEnqueue(self.datadir.path, gzip=9)
        for d in data:
            q.queue(d)
        q.close()
        fn = os.listdir(self.datadir.path)[0]
        assert not fn.endswith('.open'), '%s has .open suffix' % fn
        assert fn.endswith('.gz'), '%s has no .gz suffix' % fn
        with open(os.path.join(self.datadir.path, fn)) as f:
            sig = f.read(2)
            assert sig == '\x1f\x8b', 'expected 1F8B, got %s' % sig
        r = FileDequeue(self.datadir.path)
        for i in xrange(10):
            d = r.get(timeout=0.01)
            assert d == data[i], 'expected %s, got %s' % (data[i], d)
        r.close()

    def testRollover(self):
        data = [ dict(id=i, v='x'*256) for i in xrange(1024/16) ]
        datasize = sum(len(json.dumps(d, separators=',:'))+2 for d in data)
        #print "datasize=%d" % datasize
        MAXSIZE = 8*1024
        q = FileEnqueue(self.datadir.path, maxsize=MAXSIZE, gzip=0)
        for d in data:
            q.queue(d)
            # XXX currently rollover should not happen within a second
            time.sleep(0.05)
        q.close()
        fns = os.listdir(self.datadir.path)
        assert len(fns) > 1, 'expected >1 qfiles, got %s (size %d)' % (
            fns, os.stat(os.path.join(self.datadir.path, fns[0])).st_size)
        # last one could be smaller than maxsize
        fns.sort(key=int)
        for fn in fns[:-1]:
            size = os.stat(os.path.join(self.datadir.path, fn)).st_size
            print "%s: size=%s" % (fn, size)
            assert size > MAXSIZE, '%s: expected size>%d, got %d' % (
                fn, MAXSIZE, size)
            ubound = MAXSIZE + max(len(json.dumps(d, separators=',:'))+2
                                   for d in data)
            assert size < ubound, '%s: expected size<%d, but got %d' % (
                fn, ubound, size)

        r = FileDequeue(self.datadir.path)
        for i in xrange(1024/16):
            d = r.get(timeout=0.01)
            self.assertEquals(
                d, data[i],
                'expected %s, got %s (reading queue files in wrong order?)' %
                (data[i], d))
            
if __name__ == '__main__':
    unittest.main()
