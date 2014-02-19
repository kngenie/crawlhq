# this is a test for LevelDispatcher. named test-dispatcher.py for historical
# reasons (well, actually it's not testing LevelDB part of it. it uses
# in-memory stub seen database.)

from fixture import testdatadir, TestDomainInfo

from dispatcher import *
from leveldispatcher import LevelDispatcher
from filequeue import FileEnqueue

from fixture.testseen import *

import pytest
import py.path
import gzip

# tests for real WorksetMapper, not a fixture version defined below
def testHosthash():
    mapper = WorksetMapper(8)

    h1 = mapper.hosthash('http://www.archive.org/')

    h2 = mapper.hosthash('http://archive.org/')
    assert h1 == h2

    h3 = mapper.hosthash('http://download.archive.org/')
    assert h3 == h1

    h4 = mapper.hosthash('http://www.yahoo.com/')
    assert h4 != h1

def testWorkset():
    mapper = WorksetMapper(8)
    curi = dict(u='http://www.archive.org/')
    ws1 = mapper.workset(curi)
    assert 0 <= ws1 <= 255, '%s is not within 8 bits' % ws1

class TestMapper(object):
    nworksets = 10
    worksetclient = [0]
    _workset = 0
    def workset(self, furi):
        return self._workset
class TestScheduler(object):
    def __init__(self):
        self._client_active = True
        self.curis = []
    def is_active(self, clid):
        return self._client_active
    def flush_workset(self, wsid):
        pass
    def schedule(self, curi):
        self.curis.append(curi)

@pytest.fixture
def domaininfo():
    return TestDomainInfo()
@pytest.fixture
def mapper():
    return TestMapper()
@pytest.fixture
def scheduler():
    return TestScheduler()

def readqueue(qdir):
    deq = FileDequeue(qdir)
    items = []
    while 1:
        d = deq.get(0.01)
        if d is None: break
        items.append(d)
    return items

def testRegular(testdatadir, domaininfo, mapper, scheduler):

    dispatcher = LevelDispatcher(domaininfo, 'wide', mapper, scheduler)

    enq = FileEnqueue(testdatadir.inqdir('wide'))

    curi = dict(u='http://test.example.com/1')
    enq.queue([curi])
    enq.close()

    r = dispatcher.processinq(10)

    assert r['processed'] == 1, r
    assert r['scheduled'] == 1, r
    assert r['excluded'] == 0, r
    assert r['saved'] == 0, r

    assert len(scheduler.curis) == 1
    assert scheduler.curis[0]['u'] == curi['u']

def testSeen(testdatadir, domaininfo, mapper, scheduler):
    dispatcher = LevelDispatcher(domaininfo, 'wide', mapper, scheduler)
    enq = FileEnqueue(testdatadir.inqdir('wide'))

    curi1 = dict(u='http://test.example.com/2')
    dispatcher.init_seen()
    dispatcher.seen.already_seen(curi1)

    enq.queue([curi1])
    enq.close()

    #subprocess.call('zcat /tmp/hq/wide/inq/*.gz', shell=1)

    r = dispatcher.processinq(10)

    assert r['processed'] == 1, r
    assert r['scheduled'] == 0, r
    assert r['excluded'] == 0, r
    assert r['saved'] == 0, r

    assert len(scheduler.curis) == 0, scheduler.curis

def testExcluded(testdatadir, domaininfo, mapper, scheduler):
    dispatcher = LevelDispatcher(domaininfo, 'wide', mapper, scheduler)
    enq = FileEnqueue(testdatadir.inqdir('wide'))

    curi = dict(u='http://test.example.com/3')
    domaininfo.excluded = 1

    enq.queue([curi])
    enq.close()

    r = dispatcher.processinq(10)

    assert r['processed'] == 1, r
    assert r['scheduled'] == 0, r
    assert r['excluded'] == 1, r
    assert r['saved'] == 0, r

    dispatcher.shutdown()

    # print exclude qfile content
    for q in py.path.local(dispatcher.excludedlist.qdir).listdir(
        fil=lambda p: p.ext == '.gz'):
        with gzip.open(str(q)) as f:
            print f.read()

    items = readqueue(dispatcher.excludedlist.qdir)
    assert len(items) == 1, items
    assert isinstance(items[0], dict), items[0]
    assert items[0]['u'] == curi['u']

def testOutOfScope(testdatadir, domaininfo, mapper, scheduler):
    dispatcher = LevelDispatcher(domaininfo, 'wide', mapper, scheduler)
    enq = FileEnqueue(testdatadir.inqdir('wide'))

    curi = dict(u='http://test.example.com/')
    scheduler._client_active = False

    enq.queue([curi])
    enq.close()

    r = dispatcher.processinq(10)

    assert r['processed'] == 1, r
    assert r['scheduled'] == 0, r
    assert r['excluded'] == 0, r
    assert r['saved'] == 1, r

    dispatcher.shutdown()

    items = readqueue(dispatcher.diverter.getqueue('0').qdir)
    assert len(items) == 1, items
    assert isinstance(items[0], dict), items[0]
    assert items[0]['u'] == curi['u']
