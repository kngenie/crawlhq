#!/usr/bin/python
#
# Headquarters server for crawling cloud control
#
# make sure to specify "lib" directory in python-path in WSGI config
#
import sys, os
import web
from web.utils import Storage, storify
import pymongo
import bson
import json
import time
import re
import itertools
from mako.template import Template
from mako.lookup import TemplateLookup
from cfpgenerator import FPGenerator
from urlparse import urlsplit, urlunsplit
import threading
import random
import atexit

mongo = pymongo.Connection()
db = mongo.crawl

urls = (
    '/?', 'Status',
    '/q/(.*)', 'Query'
    )
app = web.application(urls, globals())

def lref(name):
    # note: SCRIPT_FILENAME is only available in mod_wsgi
    if 'SCRIPT_FILENAME' not in web.ctx.environ:
        return os.path.join(sys.path[0], name)
    path = web.ctx.environ['SCRIPT_FILENAME']
    return os.path.join(os.path.dirname(path), name)

class Status:
    '''implements control web user interface for crawl headquarters'''
    def __init__(self):
        self.templates = TemplateLookup(directories=[lref('t')])

    def render(self, tmpl, **kw):
        t = self.templates.get_template(tmpl)
        return t.render(**kw)

    def GET(self):
        jobs = [storify(j) for j in db.jobconfs.find()]
        for j in jobs:
            qc = db.jobs[j.name].find({'co':{'$gte':0}}).count()
            coqc = 0 # db.jobs[j.name].find({'co':{'$gt':0}}).count()
            inqc = db.inq[j.name].count()
            if j.name == 'wide':
                j.seen = db.seen.count()
            else:
                j.seen = db.seen[j.name].count()
            j.queue = Storage(count=qc, cocount=coqc, inqcount=inqc)

        db.connection.end_request()
        
        web.header('content-type', 'text/html')
        return self.render('status.html', jobs=jobs)

        return html

class Query:
    def __init__(self):
        pass
    def GET(self, c):
        if not re.match(r'[^a-z]+$', c):
            raise web.notfound('c')
        if not hasattr(self, 'do_' + c):
            raise web.notfound('c')
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

if __name__ == '__main__':
    app.run()
else:
    application = app.wsgifunc()
