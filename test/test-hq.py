#!/usr/bin/python
#
import sys
import os
import testhelper
import unittest
import json
import subprocess
from urllib import urlencode
import time

DATADIR = '/tmp/hq'
if not os.path.isdir(DATADIR):
    os.makedirs(DATADIR)
# TODO: too bad to use production mongodb for testing - use mock.
os.environ['HQCONF'] = '''datadir=%s
mongo=crawl403
''' % DATADIR

import logging
logging.basicConfig(level=logging.DEBUG)

sys.path.append(os.path.join(os.path.dirname(__file__), '../ws'))
import hq
# we want to see plain stack trace rather than HTML error page.
hq.web.config.debug = False

class HQTestCase(unittest.TestCase):
    def setUp(self):
        # clean up data dir
        print >>sys.stderr, "removing %s/*..." % DATADIR
        subprocess.check_call('/bin/rm -rf %s/*' % DATADIR, shell=1)

    def testDiscoveredAndProcessinq(self):
        print >>sys.stderr, "testDiscoveredAndProcessinq..."
        # put several CURLs into inq
        data = [dict(u='http://archive.org/%s' % n,
                     p='L',
                     v='http://www.archive.org/',
                     x='a/@href') for n in range(10)]
        r = hq.app.request('/wide/mdiscovered', method='POST',
                           data=json.dumps(data))
        assert r.status == '200 OK', r
        result = json.loads(r.data)
        assert result.get('processed') == len(data), r
        assert type(result.get('t')) == float

        r = hq.app.request('/wide/flush')
        time.sleep(2.0)
        r = hq.app.request('/wide/flush')
        time.sleep(2.0)
        os.system('/bin/ls -R /tmp/hq/wide/inq')

        r = hq.app.request('/wide/processinq?' + urlencode(dict(max=100)))
        assert r.status == '200 OK', r
        assert r.headers['content-type'] == 'text/json', r
        result = json.loads(r.data)
        assert result.get('job') == 'wide', result
        assert result.get('max') == 100, result
        assert result.get('processed') == len(data), result
        # there's no active client. test with active client is done
        # in test-dispatcher.py
        assert result.get('scheduled') == 0, result
        assert result.get('saved') == len(data), result
        assert result.get('excluded') == 0, result
        assert type(result.get('td')) == float, result
        assert type(result.get('ts')) == float, result
                           
        print >>sys.stderr, "testDiscovered..."
        data = [dict(u='http://archive.org/d',
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
        r = hq.app.request('/wide/feed?' + urlencode(dict(name=0)))
        print >>sys.stderr, r

    def testStatus(self):
        print >>sys.stderr, "testStatus..."
        r = hq.app.request('/wide/status')
        #print >>sys.stderr, r
        assert r.status == '200 OK', r
        assert r.headers['content-type'] == 'text/json', r
        data = json.loads(r.data)
        assert data.get('success') == 1, data
        assert data.get('r'), data
        assert data['r'].get('job') == 'wide'

if __name__ == '__main__':
    unittest.main()
