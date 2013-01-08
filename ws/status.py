#!/usr/bin/python
#
# Headquarters server for crawling cloud control
#
# make sure to specify "lib" directory in python-path in WSGI config
#
import sys, os
import json
import time
import re
from urlparse import urlsplit, urlunsplit
import atexit
import logging

setup_problems = []
try:
    import web
    from web.utils import Storage, storify
    import pymongo
except ImportError, ex:
    setup_problems.append(ex)
import hqconfig
try:
    import urihash
    from mongocrawlinfo import CrawlInfo
except ImportError, ex:
    setup_problems.append(ex)

from mongojobconfigs import JobConfigs
from weblib import BaseApp
from zkcoord import Coordinator

try:
    mongo = pymongo.Connection(host=hqconfig.get('mongo'))
    db = mongo.crawl
except:
    mongo = None
    db = None

coord = Coordinator(hqconfig.get('zkhosts'))
jobconfigs = JobConfigs(db)

urls = (
    '/?', 'Status',
    '/q/(.*)', 'Query'
    )
app = web.application(urls, globals())

class Status(BaseApp):
    '''implements control web user interface for crawl headquarters'''
    TMPLDIR = os.path.join(os.path.dirname(__file__), 't')
    def GET(self):
        if setup_problems:
            web.header('content-type', 'text/html')
            return self.render('error_setup', setup_problems)
        if db is None:
            web.header('content-type', 'text/html')
            return ('MongoDB connection is not available.'
                    ' Make sure mongodb is running at %s' %
                    hqconfig.get('mongo'))

        errors = None
        try:
            jobs = jobconfigs.get_alljobs()
        except Exception, ex:
            errors = [str(ex)]
            jobs = []

        db.connection.end_request()

        status = coord.get_status_of()
        if not status:
            # Coordinator is down
            errors = [coord.get_status_text()]
            jobs = []
        elif status['jobs']:
            timelimit = time.time() - 24*3600
            for j in status['jobs']:
                print >>sys.stderr, "%s" % (j,)
            activejobs = set((j['name'] for j in status['jobs']
                              if j['ts'] > timelimit))
        else:
            activejobs = set()
        for j in jobs:
            j.active = j.name in activejobs
        web.header('content-type', 'text/html')
        return self.render('status', jobs, errors)

class Query:
    def __init__(self):
        pass
    def GET(self, c):
        if not re.match(r'[a-z]+$', c):
            raise web.notfound('bad action ' + c)
        if not hasattr(self, 'do_' + c):
            raise web.notfound('bad action ' + c)
        return getattr(self, 'do_' + c)()

    def do_searchseen(self):
        p = web.input(q='')
        if p.q == '':
            return '[]'
        q = re.sub(r'"', '\\"', p.q)
        r = []
        for d in db.seen.find({'$where':'this.u.match("%s")' % q}).limit(10):
            del d['_id']
            r.append(d)

        return json.dumps(r)

    #_fp64 = FPGenerator(0xD74307D3FD3382DB, 64)

    def do_seen(self):
        p = web.input(u=None, j=None)
        r = dict(u=p.u, j=p.j)
        url, job = p.u, p.j
        if url is None or job is None:
            return json.dumps(r)
        h = urihash.urikey(url) #self._fp64.sfp(url)
        d = db.seen.find_one({'_id': h})
        if d is None:
            r.update(d=None, msg='not found')
        elif d['u'] != url:
            r.update(d=None, msg='fp conflict', alt=d['u'])
        else:
            r.update(d=d)
        return json.dumps(r)

    def do_jobstat(self):
        p = web.input(job=None)
        r = {}
        if p.job:
            r['seencount'] = db.seen[p.job].count()
        return json.dumps(r)

    def do_crawlinfocount(self):
        try:
            r = db.seen.wide.count()
            return json.dumps(dict(success=1, r=r))
        except Exception as ex:
            return json.dumps(dict(success=0, err=str(ex)))

    def do_test(self):
        return json.dumps(dict(__file__=__file__, path=sys.path, env=str(web.ctx.env)))

if __name__ == '__main__':
    logging.basicConfig(filename='/tmp/status.log', level=logging.INFO)
    try:
        app.run()
    except Exception as ex:
        logging.critical('app.run() terminated with error', exc_info=1)
else:
    # for debugging
    web.config.debug = hqconfig.get('web')['debug']
    application = app.wsgifunc()
