#!/usr/bin/python
#
import sys
import os
import json
import time

from filequeue import *

def testPlainWrite(tmpdir):
    q = FileEnqueue(str(tmpdir), gzip=0)
    data = dict(a=1, b=2, c=3)
    q.queue(data)
    q.close()

    fn = tmpdir.listdir()[0]
    assert fn.ext != '.open', '%s has .open suffix' % fn
    with fn.open() as f:
        j = json.loads(f.readline().rstrip()[1:])
        assert j == data, 'expected %s, got %s' % (data, j)

        assert f.readline() == '', 'expected EOF'

def testReadResume(tmpdir):
    data = [ dict(id=i) for i in xrange(10) ]
    q = FileEnqueue(str(tmpdir), gzip=0)
    for d in data:
        q.queue(d)
    q.close()
    r = FileDequeue(str(tmpdir))
    for i in xrange(5):
        d = r.get()
        assert d == data[i], 'expected %s, got %s' % (data[i], d)
    # close, reopen the same queue. it should start reading
    # from the next item of the last read.
    r.close()
    r = FileDequeue(str(tmpdir))
    for i in xrange(5, 10):
        d = r.get()
        assert d == data[i], 'expected %s, got %s' % (data[i], d)
    r.close()

def testReadGzipped(tmpdir):
    data = [ dict(id=i) for i in xrange(10) ]
    q = FileEnqueue(str(tmpdir), gzip=9)
    for d in data:
        q.queue(d)
    q.close()
    fn = tmpdir.listdir()[0]
    assert fn.ext != '.open', '%s has .open suffix' % fn
    assert fn.ext == '.gz', '%s has no .gz suffix' % fn
    with fn.open() as f:
        sig = f.read(2)
        assert sig == '\x1f\x8b', 'expected 1F8B, got %s' % sig
    r = FileDequeue(str(tmpdir))
    for i in xrange(10):
        d = r.get(timeout=0.01)
        assert d == data[i], 'expected %s, got %s' % (data[i], d)
    r.close()

def testRollover(tmpdir):
    data = [ dict(id=i, v='x'*256) for i in xrange(1024/16) ]
    datasize = sum(len(json.dumps(d, separators=',:'))+2 for d in data)
    #print "datasize=%d" % datasize
    MAXSIZE = 8*1024
    q = FileEnqueue(str(tmpdir), maxsize=MAXSIZE, gzip=0)
    for d in data:
        q.queue(d)
        # XXX currently rollover should not happen within a second
        time.sleep(0.05)
    q.close()
    fns = tmpdir.listdir()
    assert len(fns) > 1, 'expected >1 qfiles, got %s (size %d)' % (
        len(fns), fns[0].size() if fns else 0)
    # last one could be smaller than maxsize
    fns.sort(key=lambda x: int(os.path.basename(str(x))))
    for fn in fns[:-1]:
        size = fn.size()
        print "%s: size=%s" % (fn, size)
        assert size > MAXSIZE, '%s: expected size>%d, got %d' % (
            fn, MAXSIZE, size)
        ubound = MAXSIZE + max(len(json.dumps(d, separators=',:'))+2
                               for d in data)
        assert size < ubound, '%s: expected size<%d, but got %d' % (
            fn, ubound, size)

    r = FileDequeue(str(tmpdir))
    for i in xrange(1024/16):
        d = r.get(timeout=0.01)
        assert d == data[i], (
            'expected %s, got %s (reading queue files in wrong order?)' %
            (data[i], d))
