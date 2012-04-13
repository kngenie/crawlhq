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

        # TODO: shutdown inq, check if incoming queues are properly
        # flushed.

if __name__ == '__main__':
    unittest.main()
