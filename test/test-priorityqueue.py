#!/usr/bin/python
#
import sys
import os
#from fixture import *
import json
import time

from priorityqueue import PriorityEnqueue, PriorityDequeue

DATADIR = '/tmp/hq'
if not os.path.isdir(DATADIR):
    os.makedirs(DATADIR)

def testPlainWrite(tmpdir):
    datalist = [
        dict(id=1, p='', expected=0),
        dict(id=2, p='R', expected=1),
        dict(id=3, p='E', expected=2),
        dict(id=4, p='I', expected=2),
        dict(id=5, p='L', expected=3),
        dict(id=6, p='X', expected=3)
        ]
    q = PriorityEnqueue(str(tmpdir), gzip=0)
    q.queue(datalist)
    q.close()

    # following tests depends on internal implementation specifics
    fns = os.listdir(str(tmpdir))
    assert all(fn in fns for fn in map(str, (0, 1, 2, 3))), \
        "%s: %s" % (tmpdir, ' '.join(fns))

    for p in (0, 1, 2, 3):
        fn = tmpdir.join(str(p)).listdir()[0]
        assert fn.ext != '.open', '%s has .open suffix' % fn

        with fn.open() as f:
            for j in (json.loads(l.strip()) for l in f):
                assert j['expected'] == p, '%r is in wrong queue %s' % (
                    j, p)

def testDequeue(tmpdir):
    datalist = [
        dict(id=5, p='L', expected=3),
        dict(id=1, p='', expected=0),
        dict(id=6, p='X', expected=3),
        dict(id=3, p='E', expected=2),
        dict(id=2, p='R', expected=1),
        dict(id=4, p='I', expected=2)
        ]
    q = PriorityEnqueue(str(tmpdir), gzip=0)
    for data in datalist:
        q.queue(data)
    q.close()

    qr = PriorityDequeue(str(tmpdir))
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

def ls_datadir(d):
    os.system('/bin/ls -R %s' % str(d))

def testMaxDispense(tmpdir):
    q = PriorityEnqueue(str(tmpdir))
    q.queue([dict(id=i, p='L', expected=3) for i in xrange(200)])
    q.close()

    ls_datadir(tmpdir)

    data = []
    qr = PriorityDequeue(str(tmpdir))
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

def testGetStatus(tmpdir):
    # more tests?
    q = PriorityEnqueue(str(tmpdir))
    s = q.get_status()
    assert s, "get_status returned %r" % s

def testBulkreader(tmpdir):
    q = PriorityEnqueue(str(tmpdir))
    q.queue([dict(id=i, p='L', expected=3) for i in xrange(2000)])
    q.close()
    # count how many files it created
    fns = tmpdir.join('3').listdir()
    nfiles = len(fns)

    dq = PriorityDequeue(str(tmpdir))
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
