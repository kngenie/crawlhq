#!/usr/bin/python
#
import sys
import os

from fixture import *
import pytest
import json
import subprocess
from urllib import urlencode

from jobconfigs import JobConfig

from fixture.testjobconfigs import *

sys.path.append(os.path.join(os.path.dirname(__file__), '../ws'))
import inq
# we want to see plain stack trace rather than HTML error page.
inq.web.config.debug = False

def testDiscovered(testdatadir):
    print >>sys.stderr, "testDiscovered (testdatadir={}...".format(testdatadir)
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

def testDiscoveredNonexistentJob(testdatadir):
    data = [dict(u='http://archive.org/', p='L', x='a/@href',
                 v='http://www.archive.org/')]
    r = inq.app.request('/nonexistent-job/mdiscovered', method='POST',
                        data=json.dumps(data))
    assert r.status == '404 Not Found', r
