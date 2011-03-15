#!/usr/bin/python
#
# Headquarters server for crawling cloud control
#
# make sure to specify "lib" directory in python-path in WSGI config
#
import sys, os
import web
import pymongo
import bson
import json
import time
import re
import itertools
from cfpgenerator import FPGenerator
from urlparse import urlsplit, urlunsplit
import threading
import random
import atexit

urls = (
    '/(.*)/(.*)', 'ClientAPI',
    )
app = web.application(urls, globals())

if 'mongo' not in globals():
    mongo = pymongo.Connection(host='localhost', port=27017)
db = mongo.crawl
#mongo = hqdb.get_connection()
#db = mongo.crawl
atexit.register(mongo.disconnect)

_fp12 = FPGenerator(0xE758000000000000, 12)
_fp32 = FPGenerator(0x9B6C9A2F80000000, 32)
_fp63 = FPGenerator(0xE1F8D6B3195D6D97, 63)
_fp64 = FPGenerator(0xD74307D3FD3382DB, 64)

class seen_shpq(object):
    def longkeyhash(self, s):
        return ("#%x" % _fp64.fp(s))

    def urlkey(self, url):
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

    def keyurl(self, u):
        return urlunsplit(u['s'], u['h'],
                          u['P'] if 'op' in u else u['p'],
                          u['Q'] if 'oq' in u else u['q'])

    def uriquery(self, uri):
        return {'u.s': uri['s'],
                'u.h': uri['h'],
                'u.p': uri['p'],
                'u.q': uri['q']}
    
class seen_du(object):
    def longkeyhash64(self, s):
        return ("#%x" % _fp64.fp(s))

    # always use fp - this is way too slow (>1.6s/80URIs)
    def urlkey(self, url):
        k = dict(h=self.longkeyhash64(url), u=url)
        return k

    def keyurl(self, k):
        return k['u']

    def uriquery(self, k):
        return k
    
class seen_ud(object):
    # split long URL, use fp for the tail (0.02-0.04s/80URIs)
    def longkeyhash32(self, s):
        return ("#%x" % (_fp32.fp(s) >> 32))
    def hosthash(self, h):
        # mongodb can only handle upto 64bit signed int
        return (_fp63.fp(h) >> 1)

    def urlkey(self, url):
        k = {}
        # 790 < 800 - (32bit/4bit + 1)
        if len(url) > 790:
            u1, u2 = url[:790], url[790:]
            k.update(u1=u1, u2=u2, h=self.longkeyhash32(u2))
        else:
            k.update(u1=url, h='')
        return k
    def keyurl(self, k):
        return k['u1']+k['u2'] if 'u2' in k else k['u1']
    def keyfp(self, k):
        url = k['u1']
        p1 = url.find('://')
        if p1 > 0:
            p2 = url.find('/', p1+3)
            host = url[p1+3:p2] if p2 >= 0 else url[p1+3:]
        else:
            host = ''
        return self.hosthash(host)
    def keyhost(self, k):
        return k['H']
    def uriquery(self, k):
        # in sharded environment, it is important to have shard key in
        # a query. also it is necessary for non-multi update to work.
        return {'fp':self.keyfp(k), 'u.u1':k['u1'], 'u.h':k['h']}

class WorkSet(object):
    def __init__(self, job, id):
        self.job = job
        self.id = id
        self.coll = db.jobs[self.job]

    def reset(self):
        e = self.coll.update(
            {'co':{'$gt': 0}, 'fp':self.id},
            {'$set':{'co':0}},
            multi=True, safe=True)
        return e
            
    # only safe for single thread per work set
    def dequeue1(self):
        f = dict(co=0, fp=self.id)
        o = self.coll.find_one(f)
        if o:
            self.coll.update({'_id':o['_id']},
                             {'$set':{'co':time.time()}},
                             upsert=False,
                             multi=False)
            return o
        else:
            return None

    # safe for multi-threaded access to a work set. not used because
    # findAndModify's performance drops sharply when there are multiple
    # matches for the criteria (mongodb 1.6.5)
    def dequeue1_fam(self):
        f = {'co':0, 'fp':self.id}
        result = db.command('findAndModify', self.coll.name,
                            query=f,
                            update={'$set':{'co': time.time()}},
                            upsert=False,
                            allowable_errors=['No matching object found'])
        if result['ok'] != 0:
            return result['value']
        else:
            return None

    
class ClientQueue(object):
    def __init__(self, job, worksets):
        self.job = job
        self.worksets = [WorkSet(job, id) for id in worksets]
        # persistent index into worksets
        self.next = 0

    def reset(self):
        result = dict(ws=0, ok=True)
        for ws in self.worksets:
            e = ws.reset()
            result['ws'] += 1
            result['ok'] = result['ok'] and (e['ok'] == 1)
        # FIXME returning more useful info
        return result

    def feed(self, n):
        count = n
        excount = len(self.worksets)
        r = []
        while len(r) < n and excount > 0:
            if self.next >= len(self.worksets): self.next = 0
            curi = self.worksets[self.next].dequeue1()
            self.next += 1
            if curi:
                excount = len(self.worksets)
                r.append(curi)
            else:
                excount -= 1
        return r
        
class Scheduler(seen_ud):
    '''per job scheduler'''
    def __init__(self, job):
        self.job = job
        self.clients = {}
        self.queue = db.jobs[self.job]
    
    NQUEUES = 4096

    def get_clientqueue(self, client):
        q = self.clients.get(client[0])
        if q is None:
            q = ClientQueue(self.job, self.wsidforclient(client))
            self.clients[client[0]] = q
        return q

    def wsidforclient(self, client):
        '''return list of workset ids for node name of nodes-node cluster'''
        qids = range(client[0], self.NQUEUES, client[1])
        #random.shuffle(qids)
        return qids

    def workset(self, hostfp):
        return hostfp >> (63-12)

    def schedule(self, curi):
        curi.pop('c', None)
        curi.pop('e', None)
        curi['co'] = 0
        curi['fp'] = self.workset(curi['fp'])
        self.queue.save(curi)

    def deschedule(self, curi):
        '''remove curi from workset'''
        if 'id' in curi:
            self.queue.remove({'_id':bson.objectid.ObjectId(curi['id'])})
        else:
            self.queue.remove(self.uriquery(curi['u']))

    def feed(self, client, n):
        return self.get_clientqueue(client).feed(n)

    def reset(self, client):
        return self.get_clientqueue(client).reset()
        
class IncomingQueue(object):
    def __init__(self, job, hq):
        self.job = job
        self.hq = hq
        self.coll = db.inq[self.job]

    def add(self, curis):
        result = dict(processed=0)
        if True:
            # just queue up
            if False:
                # this takes 2x longer
                for curi in curis:
                    self.coll.insert(curi)
            else:
                self.coll.insert(dict(d=curis))
            result['processed'] = len(curis)
        else:
            # schedule immediately
            for curi in curis:
                result['processed'] += 1
                if self.hq.schedule_unseen(self.job, curi):
                    result['scheduled'] += 1
        return result
                                     
    def getinq_fam(self, result, maxn):
        '''getinq with findAndModify. thread safe, but can be very slow.'''
        while result['processed'] < maxn:
            r = db.command('findAndModify', self.coll.name,
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

    def getinq_f1(self, result, maxn):
        '''getinq with fine_one and remove. not thread safe, but faster
        with large collection.'''
        while result['processed'] < maxn:
            start = time.time()
            e = self.coll.find_one()
            if e is None:
                break
            self.coll.remove({'_id':e['_id']})
            result['td'] += (time.time() - start)
            result['inq'] += 1
            if 'd' in e:
                for curi in e['d']:
                    yield curi
            else:
                yield e

    def getinq_it(self, result, maxn):
        '''getinq with fine_one and remove. not thread safe, but faster
        with large collection.'''
        it = self.coll.find()
        while result['processed'] < maxn:
            start = time.time()
            try:
                e = it.next()
            except StopIteration:
                break
            self.coll.remove({'_id':e['_id']})
            result['td'] += (time.time() - start)
            result['inq'] += 1
            if 'd' in e:
                for curi in e['d']:
                    yield curi
            else:
                yield e
        
    getinq = getinq_f1

    def process(self, maxn):
        '''process curis queued'''
        result = dict(inq=0, processed=0, scheduled=0, td=0.0, ts=0.0)

        for curi in self.getinq(result, maxn):
            result['processed'] += 1
            t0 = time.time()
            if self.hq.schedule_unseen(self.job, curi):
                result['scheduled'] += 1
            result['ts'] += (time.time() - t0)

        return result

class Headquarters(seen_ud):
    def __init__(self):
        self.schedulers = {}
        self.incomingqueues = {}

    def get_inq(self, job):
        inq = self.incomingqueues.get(job)
        if inq is None:
            inq = IncomingQueue(job, self)
            self.incomingqueues[job] = inq
        return inq

    def get_scheduler(self, job):
        jsch = self.schedulers.get(job)
        if jsch is None:
            jsch = Scheduler(job)
            self.schedulers[job] = jsch
        return jsch

    def schedule(self, job, curi):
        self.get_scheduler(job).schedule(curi)

    def schedule_unseen(self, job, curi):
        uri = curi['u']
        path = curi.get('p')
        via = curi.get('v')
        context = curi.get('x')
        expire = curi.get('e')

        '''schedules uri if not "seen" (or seen status has expired)
        in current implemtation, seeds (path='') are scheduled
        regardless of "seen" status.
        uri: string'''
        # check with seed list - use of find_and_modify prevents
        # the same URI being submitted concurrently from being scheduled
        # as well. with new=True, find_and_modify() returns updated
        # (newly created) object.
        uk = self.urlkey(uri)
        t0 = time.time()
        if False:
            update={'$set':{'u':uk, 'fp':self.keyfp(uk)}, '$inc':{'c':1}}
            if expire is not None:
                update['$set']['e'] = expire
            result = db.command('findAndModify', 'seen',
                                query=self.uriquery(uk),
                                update=update,
                                upsert=True,
                                new=True)
            curi = result['value']
            isnew = (curi['c'] == 1)
            # TODO: check result.ok
        else:
            q = self.uriquery(uk)
            curi = db.seen.find_one(q)
            if curi is None:
                curi = {'u':uk, 'fp':q['fp']}
                if expire is not None:
                    curi['e'] = expire
                db.seen.insert(curi)
                isnew = True
            else:
                if expire is not None:
                    curi['e'] = expire
                isnew = False
        #print >>sys.stderr, "seencheck:%ss" % (time.time()-t0)
        if isnew or curi.get('e', sys.maxint) < time.time():
            t1 = time.time()
            curi.update(p=path, v=via, x=context)
            self.schedule(job, curi)
            # this shows while most schedule take < 1ms, some take >8s.
            #print >>sys.stderr, "schedule:%ss" % (time.time()-t1)
            return True

        return False

    def _update_seen(self, curi, expire):
        '''updates seen database with data specified'''
        update = {'a': curi['a'], 'f': curi['f'], 'e': expire,
                  'u': curi['u']} #, 'fp': self.keyfp(curi['u'])}
        if 'id' in curi:
            flt = {'_id': bson.objectid.ObjectId(curi['id'])}
        else:
            flt = self.uriquery(curi['u'])
        #print >>sys.stderr, "flt=%s update=%s" % (flt, update)
        e = db.seen.update(flt,
                           {'$set': update},
                           multi=False, upsert=True, safe=False)
        #print >>sys.stderr, 'update_seen:%s' % e

    def update_seen(self, curi, expire=None):
        finished = curi.get('f')
        if finished is None:
            finished = curi['f'] = int(time.time())
        if expire is None:
            # seen status expires after 2 months
            # TODO: allow custom expiration per job/domain/URI
            expire =  finished + 60 * 24 * 3600
        t0 = time.time()
        self._update_seen(curi, expire)
        #print >>sys.stderr, "update_seen %f" % (time.time() - t0)
            
    def makecuri(self, o):
        curi = dict(u=self.keyurl(o['u']), id=str(o['_id']), p=o.get('p',''))
        for k in ('v', 'x', 'a', 'fp'):
            if k in o: curi[k] = o[k]
        return curi
        
    def feed(self, job, client, n):
        curis = self.get_scheduler(job).feed(client, n)
        return [self.makecuri(u) for u in curis]
        
    def finished(self, job, curis):
        '''curis has 'u' has original URL, not in db key format'''
        jsch = self.get_scheduler(job)
        result = dict(processed=0)
        for curi in curis:
            curi['u'] = self.urlkey(curi['u'])
            jsch.deschedule(curi)
            self.update_seen(curi)
            result['processed'] += 1
        return result

    def discovered(self, job, curis):
        return self.get_inq(job).add(curis)

    def reset(self, job, client):
        return self.get_scheduler(job).reset(client)

    def processinq(self, job, maxn):
        '''process incoming queue for job. maxn parameter advicse
        upper limit on numbe of URIs processed in this single call.
        number of actually processed URIs may exceed it if incoming
        queue is storing URIs in chunks.'''
        return self.get_inq(job).process(maxn)

    def seen(self, url):
        return db.seen.find_one(self.uriquery(self.urlkey(url)))

    def rehash(self, job):
        result = dict(seen=0, jobs=0)
        it = db.seen.find()
        for u in it:
            u['fp'] = self.keyfp(u['u'])
            db.seen.save(u)
            result['seen'] += 1

        it = db.jobs[job].find()
        for u in it:
            uu = db.seen.find_one({'_id':u['_id']})
            if uu:
                u['fp'] = self.workset(uu['fp'])
                db.jobs[job].save(u)
                result['jobs'] += 1

hq = Headquarters()

class ClientAPI:
    def __init__(self):
        #print >>sys.stderr, "new Headquarters instance created"
        pass
    def GET(self, job, action):
        if action in ('feed', 'ofeed', 'reset', 'processinq', 'setupindex',
                      'rehash', 'seen'):
            return self.POST(job, action)
        else:
            web.header('content-type', 'text/html')
            return '<html><body>job=%s, action=%s</body></html>' % (job, action)
    def POST(self, job, action):
        h = getattr(self, 'do_' + action, None)
        if h is None: raise web.notfound(action)
        return h(job)

    def jsonres(self, r):
        web.header('content-type', 'text/json')
        return json.dumps(r, check_circular=False, separators=',:') + "\n"
    
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
        payload = web.data()
        curis = json.loads(payload)

        start = time.time()
        result = hq.finished(job, curis)
        result['t'] = time.time() - start

        print >>sys.stderr, "mfinished ", result
        db.connection.end_request()
        return self.jsonres(result)

    def do_finished(self, job):
        p = web.input(a='{}', f=None, id=None)
        curi = dict(u=p.u, f=p.f, a=json.loads(p.a))
        if p.id: curi['id'] = p.id

        start = time.time()
        result = hq.finished(job, [curi])
        result['t'] = time.time() - start
        print >>sys.stderr, "finished", result
        db.connection.end_request()
        return self.jsonres(result)
        
    def do_discovered(self, job):
        '''receives URLs found as 'outlinks' in just downloaded page.
        do_discovered runs already-seen test and then schedule a URL
        for crawling with last-modified and content-hash obtained
        from seen database (if previously crawled)'''
        
        p = web.input(p='', v=None, x=None)
        if 'u' not in p:
            return self.jsonres({error:'u value missing'})

        result = dict(processed=0, scheduled=0)
        hq.discovered([p])
        result.update(processed=1)
        db.connection.end_request()
        return self.jsonres(result)

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
            result['error'] = 'invalid data - json parse failed'
            return self.jsonres(result)
        if not isinstance(curis, list):
            result['error'] = 'invalid data - not an array'
            return self.jsonres(result)

        start = time.time()

        result.update(hq.discovered(job, curis))

        result.update(t=(time.time() - start))
        print >>sys.stderr, "mdiscovered %s" % result
        db.connection.end_request()
        return self.jsonres(result)
            
    def do_processinq(self, job):
        '''process incoming queue. max parameter advise upper limit on
        number of URIs processed. actually processed URIs may exceed that
        number if incoming queue is storing URIs in chunks'''
        p = web.input(max=5000)
        maxn = int(p.max)
        result = dict(inq=0, processed=0, scheduled=0, max=maxn,
                      td=0.0, ts=0.0)
        start = time.time()

        # transient instance for now
        result.update(hq.processinq(job, maxn))
        
        result.update(t=(time.time() - start))
        db.connection.end_request()
        print >>sys.stderr, "processinq %s" % result
        return self.jsonres(result)

    def do_feed(self, job):
        p = web.input(n=5, name=0, nodes=1)
        name, nodes, count = int(p.name), int(p.nodes), int(p.n)
        if count < 1: count = 5

        start = time.time()
        # return an JSON array of objects with properties:
        # uri, path, via, context and data
        r = hq.feed(job, (name, nodes), count)
        db.connection.end_request()
        print >>sys.stderr, "feed %s/%s %s in %.4fs" % (
            name, nodes, len(r), time.time() - start)
        web.header('content-type', 'text/json')
        return self.jsonres(r)

    def do_reset(self, job):
        '''resets URIs' check-out state, make them schedulable to crawler'''
        p = web.input(name=None, nodes=None)
        name = int(p.name)
        nodes = int(p.nodes)
        r = dict(name=name, nodes=nodes)
        print >>sys.stderr, "received reset request: %s" % str(r)
        if name is None or nodes is None:
            r.update(msg='name and nodes are required')
            return self.jsonres(r)
        r.update(hq.reset(job, (name, nodes)))
        db.connection.end_request()
        print >>sys.stderr, "reset %s" % str(r)
        # TODO: return number of URIs reset
        return self.jsonres(r)

    def do_seen(self, job):
        p = web.input()
        u = hq.seen(p.u)
        if u:
            del u['_id']
        result = dict(u=u)
        db.connection.end_request()
        return self.jsonres(result)

    def do_test(self, job):
        web.debug(web.data())
        return str(web.ctx.env)

    def do_setupindex(self, job):
        db.seen.ensure_index([('u.u1', 1), ('u.h', 1)])
        db.jobs[job].ensure_index([('u.u1', 1), ('u.h', 1)])
        db.jobs[job].ensure_index([('co', 1), ('fp', 1)])

        r = dict(job=job, action='setupindex', sucess=1)
        return self.jsonres(r)

    def do_rehash(self, job):
        return self.jsonres(hq.rehash(job))
             
if __name__ == "__main__":
    app.run()
else:
    application = app.wsgifunc()
