#!/usr/bin/env python
#
import sys
import os
from fixture import *
import unittest
import json
import subprocess
from urllib import urlencode
import time
import logging

import hqconfig
from fixture.testjobconfigs import *
from fixture.testseen import *
import localcoord

class TestDomainInfo(object):
    def __init__(self):
        pass
    def load(self):
        pass
    def shutdown(self):
        pass
hqconfig.factory.coordinator = localcoord.Coordinator
hqconfig.factory.domaininfo = TestDomainInfo

logging.basicConfig(level=logging.DEBUG)

sys.path.append(os.path.join(os.path.dirname(__file__), '../ws'))
import hq
# we want to see plain stack trace rather than HTML error page.
hq.web.config.debug = False

class HQTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.datadir = TestDatadir()
        cls.logger = logging.getLogger('HQTestCase')
    @classmethod
    def tearDownClass(cls):
        # let Headquarter flush queues etc.
        hq.hq = None
        time.sleep(3.0)
        path = cls.datadir.path
        cls.datadir = None
        #subprocess.check_call('/bin/ls -R /tmp/hq', shell=1)
        assert not os.path.exists(path)

    def setUp(self):
        self.datadir.cleanup()

    def testDiscoveredAndProcessinq(self):
        self.logger.info('testDiscoveredAndProcessinq')
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
                           
        self.logger.info('testDiscovered')
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
        self.logger.info('testFinished')
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
        self.logger.info('testFeed')
        r = hq.app.request('/wide/feed?' + urlencode(dict(name=0)))
        self.logger.info('%s', r)

    def testStatus(self):
        self.logger.info('testStatus')
        r = hq.app.request('/wide/status')
        self.logger.debug('%r', r)
        assert r.status == '200 OK', r
        assert r.headers['content-type'] == 'text/json', r
        data = json.loads(r.data)
        assert data.get('success') == 1, data
        assert data.get('r'), data
        assert data['r'].get('job') == 'wide'

if __name__ == '__main__':
    unittest.main()
