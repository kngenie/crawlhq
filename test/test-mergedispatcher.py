#
from testhelper import *
import unittest
import urihash
import struct
import random

from mergedispatcher import MergeDispatcher
from fileinq import IncomingQueue

def rand(a, b):
    """generates uniformly random integer in range [a, b)."""
    return int(random.uniform(a, b))

class MergeDispatcherTestCase(unittest.TestCase):
    class TestDomainInfo(object):
        def get_byurl(self, url):
            return dict(exclude=False)
    class TestMapper(object):
        nworksets = 10
        worksetclient = [0]
        _workset = 0
        def workset(self, furi):
            return self._workset
    class TestScheduler(object):
        def __init__(self):
            self.curis = []
        def is_active(self, clid):
            return True
        def flush_workset(self, wsid):
            pass
        def schedule(self, curi):
            self.curis.append(curi)

    def setUp(self):
        self.testdatadir = TestDatadir()
        self.domaininfo = self.TestDomainInfo()
        self.mapper = self.TestMapper()
        self.scheduler = self.TestScheduler()
        self.inq = IncomingQueue(self.testdatadir.inqdir('wide'))

        self.dispatcher = MergeDispatcher(self.domaininfo, 'wide',
                                          self.mapper, self.scheduler,
                                          self.inq)

    def create_seen(self, urls):
        if not os.path.isdir(self.dispatcher.seendir):
            os.makedirs(self.dispatcher.seendir)
        seenfile = os.path.join(self.dispatcher.seendir, 'SEEN')
        for url in urls:
            hash = urihash.urikey(url['u'])
            url['id'] = hash
        urls.sort(key=lambda url: url['id'])
        with open(seenfile, 'w') as sw:
            for url in urls:
                sw.write(struct.pack('l', url['id']))
        return seenfile

    def generate_random_urls(self, n):
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

    def testBasic(self):
        urls = self.generate_random_urls(100)
        for url in urls:
            print url['u']

        seenurls = urls[:50]
        novelurls = urls[50:]
        seenfile = self.create_seen(seenurls)

        print "processinq #1"

        self.inq.add(urls)
        self.inq.close()

        result = self.dispatcher.processinq(0)

        assert result['processed'] == 100, result
        assert result['excluded'] == 0, result
        assert result['saved'] == 0, result
        assert result['scheduled'] == 50, result

        scheduled = set(url['u'] for url in self.scheduler.curis)
        assert all(url['u'] not in scheduled for url in seenurls)
        assert all(url['u'] in scheduled for url in novelurls)

        print "processinq #2"

        self.inq.add(urls)
        self.inq.close()

        self.scheduler.curis = []
        result = self.dispatcher.processinq(0)

        assert result['processed'] == 100, result
        assert result['excluded'] == 0, result
        assert result['saved'] == 0, result
        assert result['scheduled'] == 0, result

        assert len(self.scheduler.curis) == 0

        self.check_seenfile(seenfile)

    def testSameURLsInInput(self):
        urls = self.generate_random_urls(100)
        seenurls = urls[:50]
        novelurls = urls[50:]
        seenfile = self.create_seen(seenurls)

        dupseenurls = [dict(url) for url in seenurls[:25]]

        input = urls + dupseenurls
        self.inq.add(input)
        self.inq.close()

        result = self.dispatcher.processinq(0)
        
        assert result['processed'] == len(input), result
        assert result['excluded'] == 0, result
        assert result['saved'] == 0, result
        assert result['scheduled'] == len(novelurls), result

        self.check_seenfile(seenfile)

    def check_seenfile(self, seenfile):
        # check if SEEN file is error free
        print >>sys.stderr, "checking SEEN file integrity"
        with open(seenfile, 'r') as f:
            lastuid = None
            while 1:
                rec = f.read(8)
                if rec == '': break
                assert len(rec) == 8
                uid = struct.unpack('l', rec)
                if lastuid is not None:
                    assert lastuid < uid
                lastuid = uid

if __name__ == '__main__':
    unittest.main()
