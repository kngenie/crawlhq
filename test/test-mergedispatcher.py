#
from fixture import *
import unittest
import urihash
import struct
import random
import subprocess
import logging

from mergedispatcher import MergeDispatcher, SeenFile
from fileinq import IncomingQueue

logging.basicConfig(level=logging.DEBUG)

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
        with SeenFile(seenfile, 'wb') as sw:
            for url in urls:
                sw.write((url['id'], 0))
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

    def testSameSeenURLsInInput(self):
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

    def testSameUnseenURLsInInput(self):
        urls = self.generate_random_urls(100)
        seenurls = urls[:50]
        novelurls = urls[50:]
        seenfile = self.create_seen(seenurls)

        dupseenurls = [dict(url) for url in novelurls[:25]]

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

    def testRecovery(self):
        """tests recovery run after processinq is terminated during
        scheduling (phase 2)."""
        # TODO: there's another case of getting terminated during
        # phase 1 - actually it's more likely to happen as it takes
        # longer than phase 2. fortunately phase 1 recovery is simpler
        # than phase 2 recovery - just starting over.
        urls1 = self.generate_random_urls(50)
        self.inq.add(urls1)
        self.inq.close()

        seenfile = self.create_seen([])

        # let TestScheduler exit on 20th (i.e. after scheduling 19) cURLs.
        self.scheduler.failat = 20
        try:
            self.dispatcher.processinq(0)
            assert False, 'should raise RuntimeException'
        except Exception as ex:
            # expected
            pass

        assert len(self.scheduler.curis) == 19

        #subprocess.call(['ls', '-l', os.path.dirname(seenfile)])

        self.scheduler.failat = None
        # enqueue another 50 URLs to verify they are not consumed by
        # next processinq run.
        urls2 = self.generate_random_urls(50)
        self.inq.add(urls2)

        self.dispatcher.processinq(0)

        # TODO: want to check all intermediate files are cleaned up?
        #subprocess.call(['ls', '-l', os.path.dirname(seenfile)])

        n = self.check_seenfile(seenfile)
        # check: all of urls1 are now seen, none from urls2
        assert n == len(urls1)
        # check: all of urls1 are scheduled, no duplicates
        assert len(self.scheduler.curis) == len(urls1)
        scheduled_urls = [u['u'] for u in self.scheduler.curis]
        missing = []
        for u in urls1:
            found = (u['u'] in scheduled_urls)
            print >>sys.stderr, "{} {}".format(u['u'], found)
            if not found: missing.append(u)
        assert len(missing) == 0, "missing {} URLs {}".format(
            len(missing), missing)

if __name__ == '__main__':
    unittest.main()
