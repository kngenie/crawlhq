#!/usr/bin/env python
#
import sys
import os
from fixture import testdatadir, TestDomainInfo
import json
import subprocess
from urllib import urlencode
import time
import logging

import hqconfig
from fixture.testjobconfigs import *
from fixture.testseen import *
import localcoord

hqconfig.factory.coordinator = localcoord.Coordinator
hqconfig.factory.domaininfo = TestDomainInfo

logging.basicConfig(level=logging.DEBUG)

sys.path.append(os.path.join(os.path.dirname(__file__), '../ws'))
import hq
# we want to see plain stack trace rather than HTML error page.
hq.web.config.debug = False

logger = logging.getLogger('HQTestCase')

def testDiscoveredAndProcessinq(testdatadir):
    logger.info('testDiscoveredAndProcessinq')
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
    os.system('/bin/ls -R {}'.format(testdatadir.inqdir('wide')))

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

    logger.info('testDiscovered')
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

def testFinished(testdatadir):
    logger.info('testFinished')
    data = [dict(u='http://archive.org/',
                 )]
    r = hq.app.request('/wide/mfinished', method='POST',
                       data=json.dumps(data))
    assert r.status == '200 OK', r
    assert r.headers['content-type'] == 'text/json', r
    data = json.loads(r.data)
    assert data.get('processed') == 1, r
    assert type(data.get('t')) == float

def testFeed(testdatadir):
    logger.info('testFeed')
    r = hq.app.request('/wide/feed?' + urlencode(dict(name=0)))
    logger.info('%s', r)

def testStatus(testdatadir):
    logger.info('testStatus')
    r = hq.app.request('/wide/status')
    logger.debug('%r', r)
    assert r.status == '200 OK', r
    assert r.headers['content-type'] == 'text/json', r
    data = json.loads(r.data)
    assert data.get('success') == 1, data
    assert data.get('r'), data
    assert data['r'].get('job') == 'wide'
