#!/usr/bin/python
#
import sys
import os
import testhelper
import unittest
import json
import subprocess
from urllib import urlencode

DATADIR = '/tmp/hq'
if not os.path.isdir(DATADIR):
    os.makedirs(DATADIR)
# TODO: too bad to use production mongodb for testing - use mock.
os.environ['HQCONF'] = '''datadir=%s
mongo=crawl403
''' % DATADIR

sys.path.append(os.path.join(os.path.dirname(__file__), '../ws'))
import hq
# we want to see plain stack trace rather than HTML error page.
hq.web.config.debug = False

class HQTestCase(unittest.TestCase):
    def setUp(self):
        # clean up data dir
        print >>sys.stderr, "removing %s/*..." % DATADIR
        subprocess.check_call('/bin/rm -rf %s/*' % DATADIR, shell=1)

    def testDiscovered(self):
        print >>sys.stderr, "testDiscovered..."
        data = [dict(u='http://archive.org/',
                    p='L',
                    v='http://www.archive.org/',
                    x='a/@href')]
        r = hq.app.request('/wide/mdiscovered', method='POST',
                           data=json.dumps(data))
        assert r.status == '200 OK', r
        assert r.headers['content-type'] == 'text/json', r
        data = json.loads(r.data)
        assert data.get('processed') == 1, r
        assert type(data.get('t')) == float

    def testFinished(self):
        print >>sys.stderr, "testFinished..."
        data = [dict(u='http://archive.org/',
                     )]
        r = hq.app.request('/wide/mfinished', method='POST',
                           data=json.dumps(data))
        assert r.status == '200 OK', r
        assert r.headers['content-type'] == 'text/json', r
        data = json.loads(r.data)
        assert data.get('processed') == 1, r
        assert type(data.get('t')) == float

    def testFeed(self):
        print >>sys.stderr, "testFeed..."
        r = hq.app.request('/wide/feed', data=urlencode(dict(name=0)))
        print >>sys.stderr, r

if __name__ == '__main__':
    unittest.main()
