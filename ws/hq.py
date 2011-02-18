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
#import hqdb
import json
import time
import re
import itertools
from mako.template import Template
from mako.lookup import TemplateLookup
from hashcrawlmapper import fingerprint
import fpgenerator
from urlparse import urlsplit, urlunsplit
import threading
import random
import atexit

urls = (
    '/?', 'Status',
    '/q/(.*)', 'Query',
    '/jobs/(.*)/(.*)', 'Headquarters',
    )
app = web.application(urls, globals())

def loading():
    global mongo, db
    print >>sys.stderr, "opening mongodb connection"
    mongo = pymongo.Connection()
    db = mongo.crawl
def unloading():
    global mongo, db
    print >>sys.stderr, "closing mongodb connection"
    mongo.disconnect()

#app.add_processor(web.loadhook(loading))
#app.add_processor(web.unloadhook(unloading))
if 'mongo' not in globals():
    mongo = pymongo.Connection(host='localhost', port=27017)
db = mongo.crawl
#mongo = hqdb.get_connection()
#db = mongo.crawl
atexit.register(mongo.disconnect)

_fp12 = fpgenerator.make(0xE758000000000000, 12)

class Headquarters:
    def __init__(self):
        #print >>sys.stderr, "new Headquarters instance created"
        pass
    def GET(self, job, action):
        if action in ('feed', 'ofeed', 'reset', 'processinq'):
            return self.POST(job, action)
        else:
            web.header('content-type', 'text/html')
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
    
    def longkeyhash32(self, s):
        return ("#%x" % (fpgenerator.std32(s) >> 32))
    def longkeyhash64(self, s):
        return ("#%x" % fpgenerator.std64(s))

    longkeyhash = longkeyhash64

    def urlkey_shpq(self, url):
        scheme, netloc, path, query, fragment = urlsplit(url)
        k = dict(s=scheme, h=netloc)
        if len(path) < 800:
            k.update(p=path)
        else:
            k.update(P=path, p=self.longkeyhash(path))
        if len(query) < 800:
            k.update(q=query)
        else:
            k.update(Q=query, q=self.longkeyhash(query))
        return k

    def keyurl_shpq(self, u):
        return urlunsplit(u['s'], u['h'],
                          u['P'] if 'op' in u else u['p'],
                          u['Q'] if 'oq' in u else u['q'])

    def uriquery_shpq(self, uri):
        return {'u.s': uri['s'],
                'u.h': uri['h'],
                'u.p': uri['p'],
                'u.q': uri['q']}
    
    # always use fp - this is way too slow (>1.6s/80URIs)
    def urlkey_du(self, url):
        k = dict(h=self.longkeyhash64(url), u=url)
        return k

    def keyurl_du(self, k):
        return k['u']

    def uriquery_du(self, k):
        return k

    # split long URL, use fp for the tail (0.02-0.04s/80URIs)
    def urlkey_ud(self, url):
        # 790 < 800 - (32bit/4bit + 1)
        if len(url) > 790:
            u1, u2 = url[:790], url[790:]
            k = dict(u1=u1, u2=u2, h=self.longkeyhash32(u2))
        else:
            k = dict(u1=url, h='')
        return k
    def keyurl_ud(self, k):
        return k['u1']+k['u2'] if 'u2' in k else k['u1']
    def keyhost_ud(self, k):
        try:
            # assuming netloc part is shorter than 790
            url = k['u1']
            p1 = url.index('://')
            p2 = url.index('/', p1+3)
            return url[p1+3:p2]
        except:
            return ''
    def uriquery_ud(self, k):
        return {'u.u1':k['u1'], 'u.h':k['h']}

    urlkey = urlkey_ud
    keyurl = keyurl_ud
    keyhost = keyhost_ud
    uriquery = uriquery_ud

    NQUEUES = 4096

    def setqueueid(self, curi):
        if 'fp' not in curi:
            curi['fp'] = (_fp12(self.keyhost(curi['u'])) >> (64-12))

    def queuename(self, n):
        return "q%03x" % n

    def queueforcuri(self, curi):
        self.setqueueid(curi)
        return self.queuename(curi['fp'])

    def queueidsfornode(self, name, nodes):
        '''return list of queue ids for node name of nodes-node cluster'''
        qids = range(0, self.NQUEUES, nodes)
        random.shuffle(qids)
        return qids
        
    def schedule(self, job, curi):
        '''curi must have u parameter in urlkey format'''
        curi.pop('c', None)
        curi.pop('e', None)
        curi['co'] = 0
        #q = self.queueforcuri(curi)
        #db.jobs[job][q].save(curi)
        self.setqueueid(curi)
        db.jobs[job].save(curi)

    def deschedule(self, job, curi):
        '''remove curi from queue - curi must have _id from seen database'''
        #q = self.queueforcuri(curi)
        #db.jobs[job][q].remove({'_id':curi['_id']})
        db.jobs[job].remove({'_id':curi['_id']})

    def _update_seen(self, id, uri, data, finished, expire):
        '''update seen database with data specified.
        returned curi have _id and u properties only'''
        assert type(uri) == dict
        update = {'a': data, 'f': finished, 'e': expire}
        if id:
            curi = {'_id': bson.objectid.ObjectId(id), 'u': uri}
        else:
            curi = db.seen.find_one(self.uriquery(uri), {'u':1, 'fp':1})
        if curi:
            db.seen.update({'_id': curi['_id']},
                           {'$set':update},
                           multi=False, upsert=False)
            return curi
        else:
            update.update(u=uri)
            db.seen.insert(update)
            return None

    def _update_seen_fnm(self, id, uri, data, finished, expire):
        '''variant of _update_seen that uses findAndModify for updating
        curi. this is generally much slower than _update_seen, based on
        experiment result'''
        assert type(uri) == dict
        r = db.command('findAndModify', 'seen',
                       query=self.uriquery(uri),
                       update={'$set': {'u': uri,
                                        'a': data,
                                        'f': finished,
                                        'e': expire}},
                       fields={'u':1, 'fp':1},
                       upsert=True,
                       new=False)
        curi = r['value']
        # findAndModify returns {} when newly insered
        return curi if curi else None
        
    def update_seen(self, id, uri, data, finished, expire=None):
        if expire is None:
            # seen status expires after 2 months
            # TODO: allow custom expiration per job/domain/URI
            expire = finished + 60 * 24 * 3600
        t0 = time.time()
        u = self._update_seen(id, uri, data, finished, expire)
        #print >>sys.stderr, "update_seen %f" % (time.time() - t0)
        return u
        # important: keep 'c' value (see do_discovered below)
            
        #db.seen.update({'u': uri},
        #               {'$set': {'a': data,
        #                         'f': finished,
        #                         'e': expire}},
        #               upsert=True, multi=False)
    
    def do_mfinished(self, job):
        '''process finished event in a batch. array of finished crawl URIs
           in the body. each crawl URI is an object with following properties:
           u: URL,
           f: finished time (optional; current time will be used if omitted),
           a: an object with response information:
              m: last-modified as Unix timestamp,
              d: content-digest,
              s: HTTP status code,
              (additional properties are allowed - HQ does not check)'''
        result = dict(processed=0)
        payload = web.data()
        curis = json.loads(payload)
        now = time.time()
        for curi in curis:
            uri = self.urlkey(curi['u'])
            finished = curi.get('f', now)
            #print >>sys.stderr, "mfinished uri=", uri
            u = self.update_seen(curi.get('id'), uri, curi['a'], finished)
            if u:
                self.deschedule(job, u)
            #db.jobs[job].remove({'u': uri})
            result['processed'] += 1
        result.update(t=(time.time() - now))
        print >>sys.stderr, "mfinished ", result
        db.connection.end_request()
        return self.jsonres(result)

    def do_finished(self, job):
        p = web.input(a='{}', f=time.time(), id=None)
        uri = p.u
        id = p.id
        u = self.urlkey(uri)
        # data: JSON with properties: content-digest, etag, last-modified
        data = json.loads(p.a)
        finished = p.f

        result = dict(processed=0)
        # 1. update seen record
        updatestart = time.time()
        u = self.update_seen(id, u, data, finished)

        # 2. remove curi from scheduled URIs collection
        # should we get _id from seen collection for use in remove()?
        # (it's more efficient)
        removestart = time.time()
        #db.jobs[job].remove({'u':u})

        if u:
            self.unschedule(job, u)
        result['processed'] += 1
        now = time.time()
        result.update(t=(now - updatestart()),
                      tu=(removestart - updatestart),
                      tr=(now - removestart))
        print >>sys.stderr, "finished", result
        db.connection.end_request()
        return self.jsonres(result)
        
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
        db.connection.end_request()
        return self.jsonres(result)

    def crawl_now(self, curi):
        # no 'e' value means 'do not crawl'
        return curi['c'] == 1 or \
            curi.get('e', sys.maxint) < time.time()

    def schedule_unseen(self, job,
                        uri, path=None, via=None, context=None,
                        expire=None,
                        **rest):
        '''schedules uri if not "seen" (or seen status has expired)
        in current implemtation, seeds (path='') are scheduled
        regardless of "seen" status.
        uri: string'''
        # check with seed list - use of find_and_modify prevents
        # the same URI being submitted concurrently from being scheduled
        # as well. with new=True, find_and_modify() returns updated
        # (newly created) object.
        u = self.urlkey(uri)
        seentime = time.time()
        if True:
            update={'$set':{'u': u}, '$inc':{'c': 1}}
            if expire is not None:
                update['$set']['e'] = expire
            result = db.command('findAndModify', 'seen',
                                query=self.uriquery(u),
                                update=update,
                                upsert=True,
                                new=True)
            seentime = time.time() - seentime
            # TODO: check result.ok
            curi = result['value']
        else:
            curi = db.seen.find_one(self.uriquery(u))
            if curi is None:
                curi = {'u':u, 'c':1}
                if expire is not None:
                    curi['e'] = expire
                db.seen.insert(curi)
            else:
                if expire is not None:
                    curi['e'] = expire
                curi['c'] = 2
                                    
        if self.crawl_now(curi):
            #if 'fp' not in curi:
            #    try:
            #        fp = fingerprint(uri)
            #    except Exception, ex:
            #        print >>sys.stderr, "fingerprint(%s) failed: " % uri, ex
            #        return False
            #    # Mongodb supports up to 64-bit *signed* int. This is also
            #    # compatible with H3's HashCrawlMapper.
            #    if fp >= (1<<63):
            #        fp = (1<<64) - fp
            #    db.seen.update({'_id':curi['_id']},{'$set':{'fp':fp}})
            #    curi['fp'] = fp
            curi.update(p=path, v=via, x=context)
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
        '''receives submission of "discovered" events in batch.
        this version simply queues data submitted in incoming queue
        to minimize response time. entries in the incoming queue
        will be processed by separate processinq call.'''
        result = dict(processed=0)
        try:
            data = web.data()
            curis = json.loads(data)
        except:
            web.debug("json.loads error:data=%s" % data)
            return self.jsonres(result)

        start = time.time()
        if False:
            # this takes 2x longer
            for curi in curis:
                db.inq[job].insert(curi)
        else:
            db.inq[job].save(dict(d=curis))
        result.update(processed=len(curis), t=(time.time() - start))
        print >>sys.stderr, "mdiscovered %s in %fs" % \
            (result, (time.time() - start))
        db.connection.end_request()
        return self.jsonres(result)

    def processinq(self, job, queue, result):
        try:
            for curi in queue():
                result['processed'] += 1
                t0 = time.time()
                if self.schedule_unseen(job,
                                        curi['u'],
                                        path=curi.get('p'),
                                        via=curi.get('v'),
                                        context=curi.get('x'),
                                        expire=curi.get('e')):
                    result['scheduled'] += 1
                result['ts'] += (time.time() - t0)
        except Exception as ex:
            print >>sys.stderr, ex
            
    def do_processinq(self, job):
        '''process incoming queue. max parameter advise upper limit on
        number of URIs processed. actually processed URIs may exceed that
        number if incoming queue is storing URIs in chunks'''
        p = web.input(max=5000)
        maxn = int(p.max)
        result = dict(inq=0, processed=0, scheduled=0, max=maxn,
                      td=0.0, ts=0.0)
        start = time.time()

        def getinq():
            while result['processed'] < maxn:
                r = db.command('findAndModify', 'inq.%s' % job,
                               remove=True,
                               allowable_errors=['No matching object found'])
                if r['ok'] == 0:
                    break

                result['inq'] += 1
                e = r['value']
                if 'd' in e:
                    # discovered
                    for curi in e['d']:
                        yield curi
                else:
                    yield e

        def getinq2():
            '''less reliable, but ok for single thread'''
            while result['processed'] < maxn:
                start = time.time()
                e = db.inq[job].find_one()
                if e is None:
                    break
                db.inq[job].remove({'_id':e['_id']})
                result['td'] += (time.time() - start)
                result['inq'] += 1
                if 'd' in e:
                    for curi in e['d']:
                        yield curi
                else:
                    yield e

        if False:
            th1 = threading.Thread(target=self.processinq, args=(job, getinq, result))
            th2 = threading.Thread(target=self.processinq, args=(job, getinq, result))
            th1.start()
            th2.start()
            th1.join()
            th2.join()
        else:
            self.processinq(job, getinq2, result)

        result.update(t=(time.time() - start))
        db.connection.end_request()
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
        db.connection.end_request()
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
        db.connection.end_request()
        return self.jsonres(result)
                
    def makecuri(self, o):
        curi = dict(u=self.keyurl(o['u']), id=str(o['_id']), p=o.get('p',''))
        for k in ('v', 'x', 'a', 'fp'):
            if k in o: curi[k] = o[k]
        return curi
        
    # safe for multi-threaded access to a work set.
    def dequeue1_fam(self, job, qn):
        q = 'jobs.%s' % job
        f = {'co':0, 'fp':qn}
        result = db.command('findAndModify', q,
                            query=f,
                            update={'$set':{'co': time.time()}},
                            upsert=False,
                            allowable_errors=['No matching object found'])
        if result['ok'] != 0:
            return self.makecuri(result['value'])
        else:
            return None
    
    # only safe for single thread per work set.
    def dequeue1_f1u(self, job, qn):
        f = dict(co=0, fp=qn)
        o = db.jobs[job].find_one(f)
        if o:
            db.jobs[job].update({'_id':o['_id']},
                                {'$set':{'co':time.time()}},
                                upsert=False,
                                multi=False)
            return self.makecuri(o)
        else:
            return None
            
    dequeue1 = dequeue1_f1u
    
    def do_feed_batch(self, job):
        '''a bit faster, but tends to return URIs in a single working set,
        leading to less per-host queues in crawler, slow crawl rate.'''
        p = web.input(n=5, name=0, nodes=1)
        name = int(p.name)
        nodes = int(p.nodes)
        count = int(p.n)
        if count < 1: count = 5

        start = time.time()
        queues = self.queueidsfornode(name, nodes)

        f = dict(co=0, fp={'$in':queues})
        r = []
        for o in db.jobs[job].find(f, limit=count, sort=[('$natural',1)]):
            db.jobs[job].update({'_id':o['_id']},
                                {'$set':{'co':time.time()}},
                                upsert=False, multi=False)
            r.append(self.makecuri(o))
        db.connection.end_request()
        print >>sys.stderr, "feed %s/%s %s in %.4fs" % (
            name, nodes, len(r), time.time() - start)
        web.header('content-type', 'text/json')
        return self.jsonres(r)

    def do_feed(self, job):
        p = web.input(n=5, name=0, nodes=1)
        name = int(p.name)
        nodes = int(p.nodes)
        count = int(p.n)
        if count < 1: count = 5

        # repeat over queue numbers (could use itertools.cycle, but
        # this generator does it without making a copy of list
        def queue_sequence():
            queues = self.queueidsfornode(name, nodes)
            while 1:
                for qn in queues:
                    yield qn

        start = time.time()
        # return an JSON array of objects with properties:
        # uri, path, via, context and data
        excount = 0
        r = []
        for qn in queue_sequence():
            curi = self.dequeue1(job, qn)
            if curi:
                excount = 0
                r.append(curi)
                if len(r) == count: break
            else:
                excount += 1
                if excount > len(queues): break
        db.connection.end_request()
        print >>sys.stderr, "feed %s/%s %s in %.4fs" % (
            name, nodes, len(r), time.time() - start)
        web.header('content-type', 'text/json')
        return self.jsonres(r)

    def do_feed_eval(self, job):
        '''perform feed operation with server side scripting. this is faster
        than issueing multiple commands from headquarters script, but will not
        work if database is shareded'''
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
            '{u:1,v:1,p:1,x:1}).limit(c).toArray();' \
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
        queues = self.queueidsfornode(name, nodes)
        le = []
        for qn in queues:
            e = db.jobs[job].update(
                {'co':{'$gt': 0}, 'fp':{'$in':queues}},
                {'$set':{'co':0}},
                multi=True, safe=True)
            le.append(le)
        r.update(e=le)
        print >>sys.stderr, "reset %s" % str(r)
        # TODO: return number of URIs reset
        return self.jsonres(r)

    def do_test(self, job):
        web.debug(web.data())
        return str(web.ctx.env)
             
def lref(name):
    # note: SCRIPT_FILENAME is only available in mod_wsgi
    if 'SCRIPT_FILENAME' not in web.ctx.environ:
        return sys.path[0] + '/' + name
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
            coqc = 0 # db.jobs[j.name].find({'co':{'$gt':0}}).count()
            inqc = db.inq[j.name].count()
            j.queue = Storage(count=qc, cocount=coqc, inqcount=inqc)

        seenCount =db.seen.count()
        db.connection.end_request()
        
        web.header('content-type', 'text/html')
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
