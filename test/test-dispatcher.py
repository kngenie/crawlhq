#
from testhelper import *
import unittest

from dispatcher import *
from filequeue import FileEnqueue

class WorksetMapperTestCase(unittest.TestCase):
    def setUp(self):
        self.mapper = WorksetMapper(8)
        
    def testHosthash(self):
        h1 = self.mapper.hosthash('http://www.archive.org/')

        h2 = self.mapper.hosthash('http://archive.org/')
        assert h1 == h2

        h3 = self.mapper.hosthash('http://download.archive.org/')
        assert h3 == h1

        h4 = self.mapper.hosthash('http://www.yahoo.com/')
        assert h4 != h1

    def testWorkset(self):
        curi = dict(u='http://www.archive.org/')
        ws1 = self.mapper.workset(curi)
        assert 0 <= ws1 <= 255, '%s is not within 8 bits' % ws1

class DispatcherTestCase(unittest.TestCase):
    class TestDomainInfo(object):
        def __init__(self):
            self.excluded=0
        def get_byurl(self, url):
            return dict(exclude=self.excluded)
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

    def setUp(self):
        self.testdatadir = TestDatadir()
        self.domaininfo = self.TestDomainInfo()
        self.mapper = self.TestMapper()
        self.scheduler = self.TestScheduler()
        self.dispatcher = Dispatcher(self.domaininfo,
                                     'wide', self.mapper, self.scheduler)

        # plain FileEnqueue for passing CURL to Dispatcher
        self.enq = FileEnqueue(self.testdatadir.inqdir('wide'))

    def readqueue(self, qdir):
        deq = FileDequeue(qdir)
        items = []
        while 1:
            d = deq.get(0.01)
            if d is None: break
            items.append(d)
        return items

    def testRegular(self):
        curi = dict(u='http://test.example.com/')
        self.enq.queue([curi])
        self.enq.close()

        r = self.dispatcher.processinq(10)

        assert r['processed'] == 1, r
        assert r['scheduled'] == 1, r
        assert r['excluded'] == 0, r
        assert r['saved'] == 0, r

        assert len(self.scheduler.curis) == 1
        assert self.scheduler.curis[0]['u'] == curi['u']

    def testSeen(self):
        curi1 = dict(u='http://test.example.com/')
        self.dispatcher.init_seen()
        self.dispatcher.seen.already_seen(curi1)

        self.enq.queue([curi1])
        self.enq.close()

        #subprocess.call('zcat /tmp/hq/wide/inq/*.gz', shell=1)

        r = self.dispatcher.processinq(10)

        assert r['processed'] == 1, r
        assert r['scheduled'] == 0, r
        assert r['excluded'] == 0, r
        assert r['saved'] == 0, r

        assert len(self.scheduler.curis) == 0, self.scheduler.curis

    def testExcluded(self):
        curi = dict(u='http://test.example.com/')
        self.domaininfo.excluded = 1

        self.enq.queue([curi])
        self.enq.close()

        r = self.dispatcher.processinq(10)

        assert r['processed'] == 1, r
        assert r['scheduled'] == 0, r
        assert r['excluded'] == 1, r
        assert r['saved'] == 0, r

        self.dispatcher.shutdown()

        subprocess.check_call(
            'zcat %s/*.gz' % self.dispatcher.excludedlist.qdir,
            shell=1)

        items = self.readqueue(self.dispatcher.excludedlist.qdir)
        assert len(items) == 1, items
        assert isinstance(items[0], dict), items[0]
        assert items[0]['u'] == curi['u']

    def testOutOfScope(self):
        curi = dict(u='http://test.example.com/')
        self.scheduler._client_active = False

        self.enq.queue([curi])
        self.enq.close()

        r = self.dispatcher.processinq(10)

        assert r['processed'] == 1, r
        assert r['scheduled'] == 0, r
        assert r['excluded'] == 0, r
        assert r['saved'] == 1, r

        self.dispatcher.shutdown()

        items = self.readqueue(self.dispatcher.diverter.getqueue('0').qdir)
        assert len(items) == 1, items
        assert isinstance(items[0], dict), items[0]
        assert items[0]['u'] == curi['u']


if __name__ == '__main__':
    unittest.main()
