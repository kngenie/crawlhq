#!/usr/bin/python
#
import sys
import os
import testhelper
import unittest
import json
import time
import shutil

from priorityqueue import PriorityEnqueue, PriorityDequeue

DATADIR = '/tmp/hq'
if not os.path.isdir(DATADIR):
    os.makedirs(DATADIR)

class PriorityQueueTestCase(unittest.TestCase):
    NOCLEAN = False

    def setUp(self):
        if not os.path.isdir(DATADIR):
            os.makedirs(DATADIR)
        else:
            for fn in os.listdir(DATADIR):
                path = os.path.join(DATADIR, fn)
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
    def tearDown(self):
        if not self.NOCLEAN:
            if os.path.exists(DATADIR):
                shutil.rmtree(DATADIR)

    def testPlainWrite(self):
        datalist = [
            dict(id=1, p='', expected=0),
            dict(id=2, p='R', expected=1),
            dict(id=3, p='E', expected=2),
            dict(id=4, p='I', expected=2),
            dict(id=5, p='L', expected=3),
            dict(id=6, p='X', expected=3)
            ]
        q = PriorityEnqueue(DATADIR, gzip=0)
        q.queue(datalist)
        q.close()

        # following tests depends on internal implementation specifics
        fns = os.listdir(DATADIR)
        assert all(fn in fns for fn in map(str, (0, 1, 2, 3))), \
            "%s: %s" % (DATADIR, ' '.join(fns))

        for p in (0, 1, 2, 3):
            fn = os.listdir(os.path.join(DATADIR, str(p)))[0]
            assert not fn.endswith('.open'), '%s has .open suffix' % fn

            with open(os.path.join(DATADIR, str(p), fn)) as f:
                for j in (json.loads(l.strip()) for l in f):
                    assert j['expected'] == p, '%r is in wrong queue %s' % (
                        j, p)
        
    def testDequeue(self):
        datalist = [
            dict(id=5, p='L', expected=3),
            dict(id=1, p='', expected=0),
            dict(id=6, p='X', expected=3),
            dict(id=3, p='E', expected=2),
            dict(id=2, p='R', expected=1),
            dict(id=4, p='I', expected=2)
            ]
        q = PriorityEnqueue(DATADIR, gzip=0)
        for data in datalist:
            q.queue(data)
        q.close()

        qr = PriorityDequeue(DATADIR)
        rdata = []
        while 1:
            d = qr.get(timeout=0.01)
            if d is None: break
            rdata.append(d)
            
        assert len(rdata) == len(datalist), "only %d of %d are read out" % (
            len(rdata), len(datalist))
        pp = -1
        for d in rdata:
            assert pp <= d['expected'], '%r: priority < expected %d' % (
                d, pp)
            pp = d['expected']

    def ls_datadir(self):
        os.system('/bin/ls -R %s' % DATADIR)

    def testMaxDispense(self):
        q = PriorityEnqueue(DATADIR)
        q.queue([dict(id=i, p='L', expected=3) for i in xrange(200)])
        q.close()

        self.ls_datadir()

        data = []
        qr = PriorityDequeue(DATADIR)
        # set lower MAX_DISPENSE for testing
        qr.MAX_DISPENSE = 10
        # start reading; this should set current queue to priority=3
        u = qr.get(timeout=0.01)
        assert u, "qr.get->%r" % u
        assert u['expected'] == 3
        data.append(u)
        # then add data with higher priority
        q.queue([dict(id=i, p='', expected=0) for i in xrange(200, 220)])
        q.close()
        # continue reading
        for i in xrange(qr.MAX_DISPENSE * 2):
            u = qr.get(timeout=0.01)
            assert u
            data.append(u)

        assert len(data) == 1 + qr.MAX_DISPENSE * 2

        # check the result
        d1 = data[qr.MAX_DISPENSE - 1]
        d2 = data[qr.MAX_DISPENSE]
        assert d1['expected'] == 3
        assert d2['expected'] == 0, "data[%d]=%r" % (
            qr.MAX_DISPENSE, d2)

    def testGetStatus(self):
        # more tests?
        q = PriorityEnqueue(DATADIR)
        s = q.get_status()
        assert s, "get_status returned %r" % s

    def testBulkreader(self):
        q = PriorityEnqueue(DATADIR)
        q.queue([dict(id=i, p='L', expected=3) for i in xrange(2000)])
        q.close()
        # count how many files it created
        fns = os.listdir(os.path.join(DATADIR, '3'))
        nfiles = len(fns)

        dq = PriorityDequeue(DATADIR)
        count = 0
        while nfiles > 0:
            reader = dq.bulkreader()
            assert reader is not None
            for curi in reader:
                count += 1
            nfiles -= 1
        assert count == 2000
        reader = dq.bulkreader()
        assert reader is None
        
if __name__ == '__main__':
    unittest.main()
