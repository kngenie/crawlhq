#!/usr/bin/python
#
# Headquarters server for crawling cloud control
#
import sys
# actually adding this directory to sys.path is not recommended
LIB='/var/www/crawling/hq'
if sys.path[-1] != LIB:
    sys.path.append(LIB)
import web
from web.utils import Storage, storify
import pymongo
import json
import time
from mako.template import Template
from mako.lookup import TemplateLookup
from hashcrawlmapper import fingerprint

urls = (
    '/?', 'Status',
    '/jobs/(.*)/(.*)', 'Headquarters',
    )
app = web.application(urls, globals())

mongo = pymongo.Connection()
db = mongo.crawl

class Headquarters:
    def GET(self, job, action):
        if action == 'feed':
            return self.POST(job, action)
        else:
            return '<html><body>job=%s, action=%s</body></html>' % (job, action)

    def POST(self, job, action):
        a = 'do_' + action
        if hasattr(self, a):
            return (getattr(self, a))(job)
        else:
            raise web.notfound('hoge')

    def schedule(self, job, curi):
        db.jobs[job].save(curi)

    def do_finished(self, job):
        p = web.input(data='{}')
        uri = p.uri
        # data: JSON with properties: content-digest, etag, last-modified
        data = json.loads(p.data)

        # 1. update seen record
        finished = time.time()
        # seen status expires after 2 months.
        # TODO: allow custom expiration per job
        expire = finished + 60 * 24 * 3600
        # important: keep 'c' value (see do_discovered below)
        db.seen.update({'uri':uri}, {'$set':{'data':data,
                                             'finished':finished,
                                             'expire':expire}},
                       upsert=True)

        # 2. remove curi from scheduled URIs collection
        # should we get _id from seen collection for use in remove()?
        # (it's more efficient)
        db.jobs[job].remove({'uri':uri})
        
    def do_discovered(self, job):
        '''receives URLs found as 'outlinks' in just downloaded page.
        do_discovered runs already-seen test and then schedule a URL
        for crawling with last-modified and content-hash obtained
        from seen database (if previously crawled)'''
        
        p = web.input(path='', via=None, context=None)
        #uri = p.uri
        #path = p.path
        #via = p.via
        #context = p.context

        result = dict(processed=0, scheduled=0)
        if self.schedule_unseen(job, **p):
            result.update(scheduled=1)
        result.update(processed=1)
        return json.dumps(result, check_circular=False, separators=',:')

    def crawl_now(self, curi):
        return curi['c'] == 1 or \
            curi.get('expire', sys.maxint) < time.time()

    def schedule_unseen(self, job,
                        uri, path=None, via=None, context=None, **rest):
        # check with seed list - use of find_and_modify prevents
        # the same URI being submitted concurrently from being scheduled
        # as well. with new=True, find_and_modify() returns updated
        # (newly created) object.
        #curi = db.seen.find_and_modify({'uri':uri},
        #                               update={'$set':{'uri':uri},
        #                                       '$inc':{'c':1}},
        #                               upsert=True, new=True)
        # mthod find_and_modify is available only in 1.10+
        result = db.command('findAndModify', 'seen',
                            query={'uri': uri},
                            update={'$set': {'uri': uri},
                                    '$inc': {'c': 1}},
                            upsert=True,
                            new=True)
        # TODO: check result.ok
        curi = result['value']
        if self.crawl_now(curi):
            try:
                fp = fingerprint(uri)
            except Exception, ex:
                print >>sys.stderr, "fingerprint(%s) failed: " % uri, ex
                return False
            # Mongodb supports up to 64-bit *signed* int. This is also
            # compatible with H3's HashCrawlMapper.
            if fp >= (1<<63):
                fp = (1<<64) - fp
            curi.update(path=path, via=via, context=context, fp=fp)
            del curi['c']
            curi.pop('expire', None)
            self.schedule(job, curi)
            return True
        return False

    def do_mdiscovered(self, job):
        '''same as do_discovered, but can receive multiple URLs at once.'''
        result = dict(processed=0, scheduled=0)
        p = web.input(u='')
        if p.u == '':
            return json.dumps(result)
        curis = json.loads(p.u)
        if isinstance(curis, list):
            for curi in curis:
                result['processed'] += 1
                if self.schedule_unseen(job,
                                        curi['uri'], path=curi.get('path'),
                                        via=curi.get('via'),
                                        context=curi.get('context')):
                    result['scheduled'] += 1
        return json.dumps(result, check_circular=False, separators=',:') + "\n"
                
    def do_feed(self, job):
        p = web.input(n=5, name=0, nodes=1)
        name = int(p.name)
        nodes = int(p.nodes)
        count = int(p.n)

        # return an JSON array of objects with properties:
        # uri, path, via, context and data
        q = db.jobs[job]
        r = []
        for i in xrange(count):
            #o = q.find_and_modify({'co':None, '$where': hashmap},
            #                      update={'$set':{'co': time.time()}},
            #                      upsert=False)
            query = {'co':None, 'fp':{'$mod':[nodes, name]}}
            result = db.command('findAndModify', 'jobs.%s' % job,
                                query=query,
                                update={'$set': {'co': time.time()}},
                                upsert=False,
                                allowable_errors=['No matching object found'])
            if result['ok'] != 0:
                o = result['value']
                #if o is None:
                #    break
                del o['_id']
                if 'c' in o: del o['c']
                r.append(o)
            else:
                break
        web.header('content-type', 'text/json')
        # this separators arg makes generated JSON more compact. it's usually
        # a tuple, but 2-length string works here.
        return json.dumps(r, check_circular=False, separators=',:') + "\n"
             
def lref(name):
    path = web.ctx.environ['SCRIPT_FILENAME']
    p = path.rfind('/')
    if p != -1:
        return path[:p+1] + name
    else:
        return '/' + name
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
            qc = db.jobs[j.name].count()
            j.queue = Storage(count=qc)

        seenCount =db.seen.count()

        return self.render('status.html',
                    jobs=jobs,
                    seen=Storage(count=seenCount))

        return html

if __name__ == "__main__":
    app.run()
else:
    application = app.wsgifunc()

