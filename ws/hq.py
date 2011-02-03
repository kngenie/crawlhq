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
import re
from mako.template import Template
from mako.lookup import TemplateLookup
from hashcrawlmapper import fingerprint

urls = (
    '/?', 'Status',
    '/q/(.*)', 'Query',
    '/jobs/(.*)/(.*)', 'Headquarters',
    )
app = web.application(urls, globals())

mongo = pymongo.Connection()
db = mongo.crawl

class Headquarters:
    def GET(self, job, action):
        if action in ('feed', 'ofeed', 'reset', 'processinq'):
            return self.POST(job, action)
        else:
            return '<html><body>job=%s, action=%s</body></html>' % (job, action)

    def POST(self, job, action):
        a = 'do_' + action
        if hasattr(self, a):
            return (getattr(self, a))(job)
        else:
            raise web.notfound('hoge')

    def jsonres(self, r):
        web.header('content-type', 'text/json')
        return json.dumps(r, check_circular=False, separators=',:') + "\n"
    
    def schedule(self, job, curi):
        db.jobs[job].save(curi)

    def update_seen(self, uri, data, finished, expire=None):
        if expire is None:
            # seen status expires after 2 months
            # TODO: allow custom expiration per job/domain/URI
            expire = finished + 60 * 24 * 3600
        db.seen.update({'u': uri},
                       {'$set': {'a': curi['a'],
                                 'f': finished,
                                 'e': expire}},
                       upsert=True)
            
    def do_mfinished(self, job):
        '''process finished event in a batch'''
        result = dict(processed=0)
        payload = web.data()
        curis = json.loads(payload)
        finished = time.time()
        # seen status expires after 2 months
        expire = finished + 60 * 24 * 3600
        for curi in curis:
            uri = curi['u']
            self.update_seen(uri, curi['a'], finished)
            db.jobs[job].remove({'u': uri})
            result['processed'] += 1
        return self.jsonres(result)

    def do_finished(self, job):
        p = web.input(a='{}')
        uri = p.u
        # data: JSON with properties: content-digest, etag, last-modified
        data = json.loads(p.a)

        # 1. update seen record
        finished = time.time()
        # seen status expires after 2 months.
        # TODO: allow custom expiration per job
        expire = finished + 60 * 24 * 3600
        # important: keep 'c' value (see do_discovered below)
        updatestart = time.time()
        self.update_seen(uri, data, finished)

        # 2. remove curi from scheduled URIs collection
        # should we get _id from seen collection for use in remove()?
        # (it's more efficient)
        removestart = time.time()
        db.jobs[job].remove({'u':uri})
        print >>sys.stderr, \
            "finished: seen.update in %f, jobs.%s.remove in %f" \
            % ((removestart - updatestart), job, (time.time() - removestart))
        
    def do_discovered(self, job):
        '''receives URLs found as 'outlinks' in just downloaded page.
        do_discovered runs already-seen test and then schedule a URL
        for crawling with last-modified and content-hash obtained
        from seen database (if previously crawled)'''
        
        p = web.input(p='', v=None, x=None)

        result = dict(processed=0, scheduled=0)
        if self.schedule_unseen(job, p.u, path=p.p, via=p.v, context=p.x):
            result.update(scheduled=1)
        result.update(processed=1)
        return self.jsonres(result)

    def crawl_now(self, curi):
        return curi['c'] == 1 or \
            curi.get('e', sys.maxint) < time.time()

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
        # method find_and_modify is available only in 1.10+
        seentime = time.time()
        result = db.command('findAndModify', 'seen',
                            query={'u': uri},
                            update={'$set': {'u': uri},
                                    '$inc': {'c': 1}},
                            upsert=True,
                            new=True)
        seentime = time.time() - seentime
        # TODO: check result.ok
        curi = result['value']
        if self.crawl_now(curi):
            fp = curi.get('fp')
            if fp is None:
                try:
                    fp = fingerprint(uri)
                except Exception, ex:
                    print >>sys.stderr, "fingerprint(%s) failed: " % uri, ex
                    return False
                # Mongodb supports up to 64-bit *signed* int. This is also
                # compatible with H3's HashCrawlMapper.
                if fp >= (1<<63):
                    fp = (1<<64) - fp
                db.seen.update({'_id':curi['_id']},{'$set':{'fp':fp}})
            curi.update(path=path, via=via, context=context, fp=fp)
            del curi['c']
            curi.pop('e', None)
            scheduletime = time.time()
            self.schedule(job, curi)
            scheduletime = time.time() - scheduletime
            #print >>sys.stderr, "schedule_unseen seen=%f, schedule=%f" % \
            #    (seentime, scheduletime)
            return True
        #print >>sys.stderr, "schedule_unseen seen=%f, no schedule" % \
        #    seentime
        return False

    def do_mdiscovered(self, job):
        result = dict(processed=0)
        try:
            data = web.data()
            curis = json.loads(data)
        except:
            web.debug("json.loads error:data=%s" % data)
            return self.jsonres(result)

        start = time.time()
        db.inq[job].save(dict(d=curis))
        #print >>sys.stderr, "mdiscovered %s in %fs" % \
        #    (result, (time.time() - start))
        result.update(processed=len(curis), t=(time.time() - start))
        return self.jsonres(result)

    def do_processinq(self, job):
        p = web.input(max=5000)
        maxn = int(p.max)
        result = dict(inq=0, processed=0, scheduled=0, max=maxn)
        start = time.time()
        while maxn > 0:
            r = db.command('findAndModify', 'inq.%s' % job,
                           remove=True,
                           allowable_errors=['No matching object found'])
            if r['ok'] != 0:
                result['inq'] += 1
                e = r['value']
                if 'd' in e:
                    # discovered
                    for curi in e['d']:
                        result['processed'] += 1
                        # transitional support
                        if 'uri' in curi: curi['u'] = curi.pop('uri')
                        if 'path' in curi: curi['p'] = curi.pop('path')
                        if 'via' in curi: curi['v'] = curi.pop('via')
                        if 'context' in curi: curi['x'] = curi.pop('context')
                        if self.schedule_unseen(job,
                                                curi['u'],
                                                path=curi.get('p'),
                                                via=curi.get('v'),
                                                context=curi.get('x')):
                            result['scheduled'] += 1
            else:
                break
            maxn -= 1
        result.update(t=(time.time() - start))
        return self.jsonres(result)

    def do_processinq2(self, job):
        p = web.input(max=5000)
        maxn = int(p.max)
        result = dict(inq=0, processed=0, scheduled=0, max=maxn)
        start = time.time()
        cur = db.inq[job].find()
        if maxn > 0:
            cur = cur.limit(maxn)
        for e in cur:
            result['inq'] += 1
            if 'd' in e:
                # discovered
                for curi in e['d']:
                    result['processed'] += 1
                    if self.schedule_unseen(job,
                                            curi['u'], path=curi.get('p'),
                                            via=curi.get('v'),
                                            context=curi.get('x')):
                        result['scheduled'] += 1
            db.inq[job].remove(e['_id'])
        result.update(t=(time.time() - start))
        return self.jsonres(result)

    def do_mdiscovered2(self, job):
        '''same as do_discovered, but can receive multiple URLs at once.'''
        result = dict(processed=0, scheduled=0)
        p = web.input(u='')
        data = p.u
        if data == '':
            data = web.data()
            
        try:
            curis = json.loads(data)
        except:
            web.debug("json.loads error:data=%s" % data)
            raise
        start = time.time()
        if isinstance(curis, list):
            for curi in curis:
                result['processed'] += 1
                if self.schedule_unseen(job,
                                        curi['u'], path=curi.get('p'),
                                        via=curi.get('v'),
                                        context=curi.get('x')):
                    result['scheduled'] += 1
        print >>sys.stderr, "mdiscovered %s in %fs" % \
            (result, (time.time() - start))
        return self.jsonres(result)
                
    def do_ofeed(self, job):
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
        return self.jsonres(r)

    def do_feed(self, job):
        p = web.input(n=5, name=0, nodes=1)
        name = int(p.name)
        nodes = int(p.nodes)
        count = int(p.n)

        # return an JSON array of objects with properties:
        # uri, path, via, context and data
        #q = db.jobs[job]
        #r = []
        # find and save - faster
        feedfunc1 = 'function(j,i,n,c){var t=Date.now();' \
            'var a=db.jobs[j].find({co:null,fp:{$mod:[n,i]}},' \
            '{uri:1,via:1,path:1,context:1}).limit(c).toArray();' \
            'a.forEach(function(u){' \
            'u.co=t;db.jobs[j].save(u);delete u._id});' \
            'return a;}'
        # findAndModify and loop - slower
        feedfunc2 = 'function(j,i,n,c){var t=Date.now(), a=[];' \
            'while(c-- > 0){' \
            'r=db.jobs[j].findAndModify({' \
            'query:{co:null,fp:{$mod:[n,i]}},' \
            'update:{$set:{co:t}},' \
            'fields:{u:1,v:1,p:1,x:1},' \
            'upsert:false});' \
            'if(r){delete r._id;a.push(r);}else{break;}' \
            '}return a;}'

        r = db.eval(feedfunc1, job, name, nodes, count)
        web.header('content-type', 'text/json')
        # this separators arg makes generated JSON more compact. it's usually
        # a tuple, but 2-length string works here.
        return self.jsonres(r)

    def do_reset(self, job):
        '''resets URIs' check-out state, make them schedulable to crawler'''
        p = web.input(name=None, nodes=None)
        name = int(p.name)
        nodes = int(p.nodes)
        r = dict(name=name, nodes=nodes)
        if name is None or nodes is None:
            r.update(msg='name and nodes are required')
            return self.jsonres(r)
        e = db.jobs[job].update({'co':{'$gt': 0}, 'fp':{'$mod':[nodes, name]}},
                                {'$unset':{'co':1}},
                                multi=True, safe=True)
        r.update(e=e)
        print >>sys.stderr, "reset %s" % str(r)
        # TODO: return number of URIs reset
        return self.jsonres(r)

    def do_test(self, job):
        web.debug(web.data())
        return str(web.ctx.env)
             
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
            coqc = db.jobs[j.name].find({'co':{'$gt':0}}).count()
            inqc = db.inq[j.name].count()
            j.queue = Storage(count=qc, cocount=coqc, inqcount=inqc)

        seenCount =db.seen.count()

        return self.render('status.html',
                    jobs=jobs,
                    seen=Storage(count=seenCount))

        return html

class Query:
    def __init__(self):
        pass
    def GET(self, c):
        if re.match(r'[^a-z]', c):
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
    
if __name__ == "__main__":
    app.run()
else:
    application = app.wsgifunc()
