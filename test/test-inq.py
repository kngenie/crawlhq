#!/usr/bin/python
#
import sys
import os

import testhelper
import unittest
import json
import subprocess
from urllib import urlencode

from jobconfigs import JobConfig

DATADIR = '/tmp/hq'
datadir = testhelper.TestDatadir(DATADIR)

import testjobconfigs

sys.path.append(os.path.join(os.path.dirname(__file__), '../ws'))
import inq
# we want to see plain stack trace rather than HTML error page.
inq.web.config.debug = False

class InqTestCase(unittest.TestCase):
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

        r = inq.app.request('/wide/mdiscovered', method='POST',
                           data=json.dumps(data))
        assert r.status == '200 OK', r
        assert r.headers['content-type'] == 'text/json', r
        data = json.loads(r.data)
        assert data.get('processed') == 1, r
        assert type(data.get('t')) == float

        """note inq retains state from previous test method."""
        r = inq.app.request('/wide/status')
        print r

        assert r.status == '200 OK', r
        assert r.headers['content-type'] == 'text/json'
        data = json.loads(r.data)
        assert data['job'] == 'wide'
        assert data['ok'] == 1
        assert data['r']
        assert data['r'].get('deq')
        assert data['r'].get('enq')

        # TODO: shutdown inq, check if incoming queues are properly
        # flushed.

    def testDiscoveredNonexistentJob(self):
        data = [dict(u='http://archive.org/', p='L', x='a/@href',
                     v='http://www.archive.org/')]
        r = inq.app.request('/nonexistent-job/mdiscovered', method='POST',
                            data=json.dumps(data))
        assert r.status == '404 Not Found', r
        

if __name__ == '__main__':
    unittest.main()
