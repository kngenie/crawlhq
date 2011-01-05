#!/usr/bin/python
#
# Headquarters server for crawling cloud control
#
import web
import sys
import pymongo
import json
import time
import hashcrawlmapper

urls = (
    '/jobs/(.*)/(.*)', 'Headquarters',
    '/status', 'Status',
    )
app = web.application(urls, globals())

mongo = pymongo.Connection()
db = mongo.crawl

class Headquarters:
    def GET(self, job, action):
        return '<html><body>job=%s, action=%s</body></html>' % (job, action)

    def POST(self, job, action):
        if hasattr(self, 'do_' . action):
            self[action](self, job)
        else:
            raise web.notfound()

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
        def crawl_now(curi):
            return curi['c'] == 1 or \
                curi.pop('expire', sys.maxint) < time.time()
        
        p = web.input(path='', via=None)
        uri = p.uri
        path = p.path
        via = p.via

        # check with seed list - use of find_and_modify prevents
        # the same URI being submitted concurrently from being scheduled
        # as well. with new=True, find_and_modify() returns updated
        # (newly created) object.
        curi = db.seen.find_and_modify({'uri':uri},
                                       update={'$set':{'uri':uri},
                                               '$inc':{'c':1}},
                                       upsert=True, new=True)
        if crawl_now(curi):
            curi.path = path
            curi.via = via
            self.schedule(curi)

    def do_feed(self, job):
        p = web.input(n=5, name=0)
        name = int(p.name)
        count = p.n

        # TODO: return an JSON array of objects with properties:
        # uri, path, via, context and data
        q = db.jobs[job]
        r = []
        for i in xrange(count):
            o = q.find_and_modify({'co':None, 'node':name},
                                  update={'$set':{'co': time.time()}},
                                  upsert=False)
            if o is None:
                break
            del o['_id']
            r.append(o)
        web.header('content-type', 'text/json')
        return json.dumps(r, check_circular=False, separators=(',',':'))
             
class Status:
    def GET(self):
        coll = db.jobs
        jobs = coll.getCollectionNames()
        html = '<html><body>'
        for job in jobs:
            html .= '<div>' . job . '</div>'
        html .= '</body></html>'

        return html

if __name__ == "__main__":
    app.run()

