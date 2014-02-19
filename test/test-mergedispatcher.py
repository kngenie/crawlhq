#
import sys
import os
from fixture import testdatadir, testdomaininfo
#import unittest
import pytest
import urihash
import struct
import random
import subprocess
import logging

from mergedispatcher import MergeDispatcher, SeenFile, copy_seenfile
from fileinq import IncomingQueue

logging.basicConfig(level=logging.DEBUG)

def rand(a, b):
    """generates uniformly random integer in range [a, b)."""
    return int(random.uniform(a, b))

# class MergeDispatcherTestCase(unittest.TestCase):
class TestMapper(object):
    nworksets = 10
    worksetclient = [0]
    _workset = 0
    def workset(self, furi):
        return self._workset
class TestScheduler(object):
    """if failat is a number, schedule() will fail at failat-th
    call. this simulates abnormal termination."""
    def __init__(self, failat=None):
        self.curis = []
        self.failat = failat
    def is_active(self, clid):
        return True
    def flush_workset(self, wsid):
        pass
    def schedule(self, curi):
        if self.failat is not None and len(self.curis) + 1 >= self.failat:
            raise RuntimeException()
        self.curis.append(curi)

@pytest.fixture
def testmapper():
    return TestMapper()
@pytest.fixture
def testscheduler():
    return TestScheduler()

def create_seen(dispatcher, urls):
    seendir = dispatcher.seendir
    if not os.path.isdir(seendir):
        os.makedirs(seendir)
    seenfile = os.path.join(seendir, 'SEEN')
    for url in urls:
        hash = urihash.urikey(url['u'])
        url['id'] = hash
    urls.sort(key=lambda url: url['id'])
    with SeenFile(seenfile, 'wb') as sw:
        for url in urls:
            sw.write((url['id'], 0))
    return seenfile

def generate_random_urls(n):
    TLDS = ['com', 'net', 'org', 'gov', 'edu', 'biz',
            'info', 'us', 'jp']
    urls = set()
    while len(urls) < n:
        l = rand(3, 10)
        dom = ''.join(chr(rand(ord('a'), ord('z')+1))
                      for i in xrange(l))
        urls.add('http://%s.%s/' % (
                dom, TLDS[rand(0, len(TLDS))]))
    return [dict(u=url) for url in urls]

def testBasic(testdatadir, testdomaininfo, testmapper, testscheduler):
    inq = IncomingQueue(testdatadir.inqdir('wide'))
    dispatcher = MergeDispatcher(testdomaininfo, 'wide',
                                 testmapper, testscheduler, inq)

    urls = generate_random_urls(100)
    for url in urls:
        print url['u']

    seenurls = urls[:50]
    novelurls = urls[50:]
    seenfile = create_seen(dispatcher, seenurls)

    print "processinq #1"

    inq.add(urls)
    inq.close()

    result = dispatcher.processinq(0)

    assert result['processed'] == 100, result
    assert result['excluded'] == 0, result
    assert result['saved'] == 0, result
    assert result['scheduled'] == 50, result

    scheduled = set(url['u'] for url in testscheduler.curis)
    assert all(url['u'] not in scheduled for url in seenurls)
    assert all(url['u'] in scheduled for url in novelurls)

    print "processinq #2"

    inq.add(urls)
    inq.close()

    testscheduler.curis = []
    result = dispatcher.processinq(0)

    assert result['processed'] == 100, result
    assert result['excluded'] == 0, result
    assert result['saved'] == 0, result
    assert result['scheduled'] == 0, result

    assert len(testscheduler.curis) == 0

    check_seenfile(seenfile)

def testSameSeenURLsInInput(testdatadir, testdomaininfo, testmapper,
                            testscheduler):
    inq = IncomingQueue(testdatadir.inqdir('wide'))
    dispatcher = MergeDispatcher(testdomaininfo, 'wide', testmapper,
                                 testscheduler, inq)

    urls = generate_random_urls(100)
    seenurls = urls[:50]
    novelurls = urls[50:]
    seenfile = create_seen(dispatcher, seenurls)

    dupseenurls = [dict(url) for url in seenurls[:25]]

    input = urls + dupseenurls
    inq.add(input)
    inq.close()

    result = dispatcher.processinq(0)

    assert result['processed'] == len(input), result
    assert result['excluded'] == 0, result
    assert result['saved'] == 0, result
    assert result['scheduled'] == len(novelurls), result

    check_seenfile(seenfile)

def testSameUnseenURLsInInput(testdatadir, testdomaininfo, testmapper,
                              testscheduler):
    inq = IncomingQueue(testdatadir.inqdir('wide'))
    dispatcher = MergeDispatcher(testdomaininfo, 'wide',
                                 testmapper, testscheduler, inq)

    urls = generate_random_urls(100)
    seenurls = urls[:50]
    novelurls = urls[50:]
    seenfile = create_seen(dispatcher, seenurls)

    dupseenurls = [dict(url) for url in novelurls[:25]]

    input = urls + dupseenurls
    inq.add(input)
    inq.close()

    result = dispatcher.processinq(0)

    assert result['processed'] == len(input), result
    assert result['excluded'] == 0, result
    assert result['saved'] == 0, result
    assert result['scheduled'] == len(novelurls), result

    check_seenfile(seenfile)

def check_seenfile(seenfile):
    # check if SEEN file is error free
    print >>sys.stderr, "checking SEEN file integrity"
    count = 0
    with SeenFile(seenfile, 'rb') as f:
        lastuid = None
        while 1:
            rec = f.next()
            if not rec.key: break
            count += 1
            uid = rec.key
            if lastuid is not None:
                assert lastuid < uid
            lastuid = uid
    return count

def testRecovery(testdatadir, testdomaininfo, testmapper, testscheduler):
    """tests recovery run after processinq is terminated during
    scheduling (phase 2)."""
    inq = IncomingQueue(testdatadir.inqdir('wide'))
    dispatcher = MergeDispatcher(testdomaininfo, 'wide',
                                 testmapper, testscheduler, inq)
    # TODO: there's another case of getting terminated during
    # phase 1 - actually it's more likely to happen as it takes
    # longer than phase 2. fortunately phase 1 recovery is simpler
    # than phase 2 recovery - just starting over.
    urls1 = generate_random_urls(50)
    inq.add(urls1)
    inq.close()

    seenfile = create_seen(dispatcher, [])

    # let TestScheduler exit on 20th (i.e. after scheduling 19) cURLs.
    testscheduler.failat = 20
    try:
        dispatcher.processinq(0)
        assert False, 'should raise RuntimeException'
    except Exception as ex:
        # expected
        pass

    assert len(testscheduler.curis) == 19

    #subprocess.call(['ls', '-l', os.path.dirname(seenfile)])

    testscheduler.failat = None
    # enqueue another 50 URLs to verify they are not consumed by
    # next processinq run.
    urls2 = generate_random_urls(50)
    inq.add(urls2)

    dispatcher.processinq(0)

    # TODO: want to check all intermediate files are cleaned up?
    #subprocess.call(['ls', '-l', os.path.dirname(seenfile)])

    n = check_seenfile(seenfile)
    # check: all of urls1 are now seen, none from urls2
    assert n == len(urls1)
    # check: all of urls1 are scheduled, no duplicates
    assert len(testscheduler.curis) == len(urls1)
    scheduled_urls = [u['u'] for u in testscheduler.curis]
    missing = []
    for u in urls1:
        found = (u['u'] in scheduled_urls)
        print >>sys.stderr, "{} {}".format(u['u'], found)
        if not found: missing.append(u)
    assert len(missing) == 0, "missing {} URLs {}".format(
        len(missing), missing)

def testSeenConvert(tmpdir):
    seenfile = os.path.join(str(tmpdir), 'SEEN')
    with SeenFile(seenfile, 'wb', 1) as f1:
        for uid in range(100):
            f1.write((uid, 0))
    copy_seenfile(seenfile, seenfile+'v2', 1, 2)
    with SeenFile(seenfile+'v2', 'rb', 2) as f2:
        for uid in range(100):
            rec = f2.next()
            assert rec.key == uid
            assert rec.ts == 0
        rec = f2.next()
        assert rec.key is None
