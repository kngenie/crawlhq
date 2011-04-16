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
from Queue import Queue, Empty
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
    # name is incorrect
    def keyquery(self, k):
        # in sharded environment, it is important to have shard key in
        # a query. also it is necessary for non-multi update to work.
        return {'fp':self.keyfp(k), 'u.u1':k['u1'], 'u.h':k['h']}
    # old and incorrect name
    uriquery = keyquery


class ThreadPoolExecutor(object):
    '''thread pool executor with fixed number of threads'''
    def __init__(self, poolsize=15, queuesize=5000):
        self.poolsize = poolsize
        self.queuesize = queuesize
        self.work_queue = Queue(self.queuesize)
        self.__shutdown = False
        self.workers = [
            threading.Thread(target=self._work, name="poolthread-%d" % i)
            for i in range(self.poolsize)
            ]
        for th in self.workers:
            th.setDaemon(True)
            th.start()

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        if self.__shutdown: return
        print >>sys.stderr, "ThreadPoolExecutor(%s) shutting down" % id(self)
        self.__shutdown = True
        def NOP(): pass
        for th in self.workers:
            self.work_queue.put((NOP,[]))
        for th in self.workers:
            # wait up to 5 secs for thread to terminate (too long?)
            th.join(5.0)

    def _work(self):
        while 1:
            f, args = None, None
            try:
                f, args = self.work_queue.get()
                f(*args)
            except Exception as ex:
                print >>sys.stderr, "work error:%s%s -> %s" % (f, args, ex)
            if self.__shutdown:
                break

    def execute(self, f, *args):
        self.work_queue.put((f, args))

    def __call__(self, f, *args):
        self.work_queue.put((f, args))
        
class WorkSet(seen_ud):
    SCHEDULE_SPILL_SIZE = 30000
    SCHEDULE_MAX_SIZE = 50000
    SCHEDULE_LOW = 160
    def __init__(self, job, id):
        self.job = job
        self.id = id
        self.seen = db.seen
        # scheduled CURIs
        self.scheduled = Queue(maxsize=self.SCHEDULE_MAX_SIZE)
        self.loadlock = threading.RLock()
        self.loading = False
        self.activelock = threading.RLock()
        self.active = {}
        self.running = False
        #self.load_if_low()

    def is_active(self, url):
        with self.activelock:
            return url in self.active
    def add_active(self, curi):
        with self.activelock:
            self.active[self.keyurl(curi['u'])] = curi
    def remove_active(self, curi):
        with self.activelock:
            self.active.pop(self.keyurl(curi['u']), None)
    def remove_active_uri(self, url):
        '''same as remove_active, but by raw URI'''
        with self.activelock:
            self.active.pop(url, None)

    def scheduledcount(self):
        return self.scheduled.qsize()
    def activecount(self):
        return len(self.active)

    def reset(self):
        self.running = False
        with self.activelock:
            for o in self.active.values():
                o['co'] = 0
        self.unload()
        # reset those checkedout CURIs in the database
        e = self.seen.update(
            {'co':{'$gt': 0}, 'ws':self.id},
            {'$set':{'co':0}},
            multi=True, safe=True)
        return e
            
    def checkout(self, n):
        self.running = True
        r = []
        while len(r) < n:
            try:
                o = self.scheduled.get(block=False)
                o['co'] = int(time.time())
                r.append(o)
            except Empty:
                break
        self.load_if_low()
        return r

    def load_if_low(self):
        with self.loadlock:
            if self.loading: return
            if self.scheduled.qsize() < self.SCHEDULE_LOW:
                self.loading = True
                executor.execute(self._load)

    def _load(self):
        t0 = time.time()
        l = []
        try:
            qfind = dict(co=0, ws=self.id)
            cur = self.seen.find(qfind, limit=self.SCHEDULE_LOW)
            for o in cur:
                self.scheduled.put(o)
                self.add_active(o)
                l.append(o)
            for o in l:
                qup = dict(_id=o['_id'], fp=o['fp'])
                self.seen.update(qup, {'$set':{'co':1}},
                                 upsert=False, multi=False)
                o['co'] = 1
        finally:
            with self.loadlock: self.loading = False
            # release connection
            db.connection.end_request()
        print >>sys.stderr, "WorkSet(%s,%d)._load: %d in %.2fs" % \
            (self.job, self.id, len(l), (time.time()-t0))

    def unload(self):
        '''discards all CURIs in scheduled queue'''
        self.running = False
        # TODO: support partial unload (spilling), do proper synchronization
        qsize = self.scheduled.qsize()
        print >>sys.stderr, "WS(%d).unload: flushing %d URIs" % \
            (self.id, qsize)
        while qsize > 0:
            try:
                o = self.scheduled.get(block=False)
                qsize -= 1
            except Empty:
                break
        with self.activelock:
            while self.active:
                url, curi = self.active.popitem()
                db.seen.save(curi)

    def schedule(self, curi):
        curi['ws'] = self.id
        curi['co'] = 0
        if not self.running or self.scheduled.full():
            #print >>sys.stderr, "WS(%d) scheduled is full, saving into database" % (self.id,)
            self.seen.save(curi)
            return False
        else:
            curi['co'] = 1
            self.scheduled.put(curi)
            with self.activelock:
                self.active[self.keyurl(curi['u'])] = curi
            #self.seen.save(curi)
            return True
    
    def _update_seen(self, curi, expire):
        '''updates seen database from curi from finished event.'''
        # curi['u'] is a raw URI, and curi['id'] may or may not exist
        uk = self.urlkey(curi['u'])
        # there's no real need for unsetting 'ws'
        updates = {
            '$set':{
                    'a': curi['a'], 'f': curi['f'], 'e': expire,
                    'u': uk,
                    'co': -1
                    },
            '$unset':{'w':1, 'ws': 1}
            }
        if 'id' in curi:
            flt = {'_id': bson.objectid.ObjectId(curi['id']),
                   'fp': uk['fp']}
        else:
            flt = self.keyquery(uk)
        #print >>sys.stderr, "flt=%s update=%s" % (flt, update)
        self.seen.update(flt,
                         updates,
                         multi=False, upsert=True)
        #print >>sys.stderr, '_update_seen:%s' % e

    def deschedule(self, furi, expire=None):
        '''note: furi['u'] is original raw URI.'''
        finished = furi.get('f')
        if finished is None:
            finished = furi['f'] = int(time.time())
        if expire is None:
            # seen status expires after 2 months
            # TODO: allow custom expiration per job/domain/URI
            expire =  finished + 60 * 24 * 3600
        t0 = time.time()
        # update database with crawl result
        self._update_seen(furi, expire)
        #print >>sys.stderr, "update_seen %f" % (time.time() - t0)
        self.remove_active_uri(furi['u'])
        
class ClientQueue(seen_ud):
    CHECKEDOUT_SPILL_SIZE = 10000

    def __init__(self, job, worksets):
        self.job = job
        self.worksets = worksets
        if len(self.worksets) == 0:
            print >>sys.stderr, "%s: no worksets" % self
        # persistent index into worksets
        self.next = 0
        self.feedcount = 0

        ## checkedout caches CURIs scheduled for this client until they are
        ## flushed to the database (they are likely be removed before getting
        ## flushed). it is used for in-memory seen check and flushing
        #self.checkedout = {}
        ## mutex for manipulating self.checkedout
        #self.colock = threading.RLock()
        #self.spilling = False
        ## mutex for self.spilling flag
        #self.spilllock = threading.RLock()

    #def enough_room(self):
    #    return len(self.checkedout) < (self.CHECKEDOUT_SPILL_SIZE - 500)

    def get_status(self):
        r = dict(feedcount=self.feedcount,
                 next=self.next,
                 worksetcount=len(self.worksets))
        scheduledcount = 0
        activecount = 0
        for ws in self.worksets:
            scheduledcount += ws.scheduledcount()
            activecount += ws.activecount()
        r['scheduledcount'] = scheduledcount
        r['activecount'] = activecount
        return r
        
    #def is_active(self, url):
    #    return url in self.checkedout
            
    def reset(self):
        # TODO: return something more useful for diagnosing failures
        result = dict(ws=[], ok=True)
        for ws in self.worksets:
            ws.reset()
            result['ws'].append(ws.id)
        return result

    def flush_scheduled(self):
        for ws in self.worksets:
            ws.unload()

    # unused - merge into WorkSet.unload()
    def spill_checkedout(self, exh=False):
        with self.spilllock:
            if self.spilling: return
            self.spilling = True
        try:
            with self.colock:
                while self.checkedout:
                    (k, o) = self.checkedout.popitem()
                    o['co'] = 0
                    # TODO: should push back to workset's scheduled queue,
                    # rather than into database.
                    db.seen.save(o)
                    if not exh and self.enough_room():
                        break
        finally:
            db.connection.end_request()
            with self.spilllock:
                self.spilling = False

    #def clear_checkedout(self, uri):
    #    with self.colock:
    #        cocuri = self.checkedout.pop(uri, None)
    #    return cocuri

    def feed(self, n):
        checkout_per_ws = n // 4 + 1
        r = []
        for a in (1, 2, 3):
            excount = len(self.worksets)
            while len(r) < n and excount > 0:
                if self.next >= len(self.worksets): self.next = 0
                curis = self.worksets[self.next].checkout(checkout_per_ws)
                self.next += 1
                if curis:
                    excount = len(self.worksets)
                    r.extend(curis)
                else:
                    excount -= 1
            if len(r) > 0: break
            # wait a while - if we don't, client may call again too soon,
            # keeping HQ too busy responding to feed request to fill the
            # queue.
            time.sleep(0.5)
            
        #with self.colock:
        #    for curi in curis:
        #        self.checkedout[self.keyurl(curi['u'])] = curi
        #if len(self.checkedout) > self.CHECKEDOUT_SPILL_SIZE:
        #    executor.execute(self.spill_checkedout)
        self.feedcount += len(r)
        return r
        
class Scheduler(seen_ud):
    '''per job scheduler. manages assignment of worksets to each client.'''
    #NWORKSETS = 4096
    NWORKSETS = 256
    NWORKSETS_BITS = 8
    
    def __init__(self, job):
        self.job = job
        self.clients = {}
        #self.queue = db.jobs[self.job]
        self.seen = db.seen
        self.get_jobconf()

        self.worksets = [WorkSet(job, id) for id in xrange(self.NWORKSETS)]
        self.load_workset_assignment()
        
    def get_status(self):
        r = dict(
            nworksets=self.NWORKSETS,
            clients={}
            )
        for i, client in self.clients.items():
            r['clients'][i] = client.get_status()
        return r

    def get_jobconf(self):
        self.jobconf = db.jobconfs.find_one({'name':self.job})
        if self.jobconf is None:
            self.jobconf = {'name':self.job}
            db.jobconfs.save(self.jobconf)
            
    def create_default_workset_assignment(self):
        num_nodes = self.jobconf.get('nodes', 20)
        return list(itertools.islice(
                itertools.cycle(xrange(num_nodes)),
                0, len(self.worksets)))
        
    def load_workset_assignment(self):
        r = db.jobconfs.find_one({'name':self.job}, {'wscl':1})
        wscl = self.jobconf.get('wscl')
        if wscl is None:
            wscl = self.create_default_workset_assignment()
            self.jobconf['wscl'] = wscl
            db.jobconfs.save(self.jobconf)
        if len(wscl) > len(self.worksets):
            wscl[len(self.worksets):] = ()
        elif len(wscl) < len(self.worksets):
            wscl.extend(itertools.repeat(None, len(self.worksets)-len(wscl)))
        self.worksetclient = wscl

    def get_clientqueue(self, client):
        q = self.clients.get(client[0])
        if q is None:
            worksets = [self.worksets[i] for i in self.wsidforclient(client)]
            q = ClientQueue(self.job, worksets)
            self.clients[client[0]] = q
        return q

    def wsidforclient(self, client):
        '''return list of workset ids for node name of nodes-node cluster'''
        qids = [i for i in xrange(len(self.worksetclient))
                if self.worksetclient[i] == client[0]]
        return qids

    def workset(self, hostfp):
        # don't use % (mod) - FP has much less even distribution in
        # lower bits.
        return hostfp >> (63 - self.NWORKSETS_BITS)

    # Scheduler - discovered event

    def schedule(self, curi):
        ws = self.workset(curi['fp'])
        return self.worksets[ws].schedule(curi)

    def _schedule_unseen(self, incuri):
        '''schedule_unseen to be executed asynchronously'''
        uk = self.urlkey(incuri['u'])
        q = self.keyquery(uk)
        expire = incuri.get('e')

        wsid = self.workset(q['fp'])
        if self.worksets[wsid].is_active(incuri['u']):
            return False

        t0 = time.time()
        curi = self.seen.find_one(q)
        #print >>sys.stderr, "seen.find_one:%.3f" % (time.time() - t0,)
        if curi is None:
            curi = dict(u=uk, fp=q['fp'])
            if expire is not None:
                curi['e'] = expire
            curi['w'] = dict(p=incuri.get('p'), v=incuri.get('v'),
                             x=incuri.get('x'))
            self.schedule(curi)
            return True
        else:
            if 'w' in curi: return False
            if expire is not None:
                curi['e'] = expire
            if curi.get('e', sys.maxint) < time.time():
                curi['w'] = dict(p=incuri.get('p'), v=incuri.get('v'),
                                 x=incuri.get('x'))
                self.schedule(curi)
                return True
            return False

    def schedule_unseen(self, incuri):
        '''schedule incuri if not crawled recently'''
        executor.execute(self._schedule_unseen, incuri)
        return True

    # Scheduler - finished event

    def deschedule(self, furi, expire=None):
        k = self.urlkey(furi['u'])
        wsid = self.workset(self.keyfp(k))
        self.worksets[wsid].deschedule(furi)
        
    # Scheduler - feed request

    def feed(self, client, n):
        return self.get_clientqueue(client).feed(n)

    # Scheduler - reset request

    def reset(self, client):
        return self.get_clientqueue(client).reset()

    def flush_clients(self):
        # actually this can be done without going through ClientQueues
        #for cl in self.clients.values():
        #    cl.flush_scheduled()
        for ws in self.worksets:
            ws.unload()
        
class IncomingQueue(ThreadPoolExecutor):
    def __init__(self, job, scheduler):
        '''scheduler: Scheduler for job'''
        ThreadPoolExecutor.__init__(self, poolsize=5, queuesize=50)
        self.job = job
        self.scheduler = scheduler
        self.coll = db.inq[self.job]
        self.addedcount = 0
        self.processedcount = 0

        self.rmcount = 0

    def get_status(self):
        return dict(addedcount=self.addedcount,
                    processedcount=self.processedcount,
                    workqueuesize=self.work_queue.qsize())

    def add(self, curis):
        result = dict(processed=0)
        if True:
            # just queue up
            if False:
                # this takes 2x longer
                for curi in curis:
                    self.coll.insert(curi)
            elif False:
                self.coll.insert(dict(d=curis, done=0))
            else:
                self.coll.update({'done':1},
                                 {'$set':{'d':curis, 'done':0}},
                                 multi=False, upsert=True)
            result['processed'] = len(curis)
            self.addedcount += result['processed']
        else:
            # schedule immediately
            for curi in curis:
                result['processed'] += 1
                if self.scheduler.schedule_unseen(curi):
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

    def _remove_done(self):
        self.coll.remove({'done':1})

    def getinq_f1(self, result, maxn):
        '''getinq with find_one and update. not thread safe, but faster
        with large collection.'''
        result.update(inq=0, processed=0, td=0.0)
        while result['processed'] < maxn:
            start = time.time()
            e = self.coll.find_one({'done':{'$ne':1}})
            if e is None:
                break
            #self.coll.remove({'_id':e['_id']})
            self.coll.update({'_id':e['_id']}, {'$set':{'done':1}},
                             upsert=False, multi=False)
            self.rmcount += 1
            if self.rmcount > 100:
                self.execute(self._remove_done)
                self.rmcount = 0
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
                    print >>sys.stderr, str(curi)
                    yield curi
            else:
                yield e
        
    getinq = getinq_f1

    def process_0(self, maxn):
        '''process curis queued'''
        result = dict(inq=0, processed=0, scheduled=0, td=0.0, ts=0.0,
                      tu=0.0)

        for curi in self.getinq(result, maxn):
            result['processed'] += 1
            t0 = time.time()
            if self.scheduler.schedule_unseen(curi):
                result['scheduled'] += 1
                result['ts'] += (time.time() - t0)
            else:
                result['tu'] += (time.time() - t0)
            self.processedcount += 1

        return result

    def _process(self, bucket):
        result = dict(processed=0, scheduled=0, t=0.0)
        t0 = time.time()
        for duri in bucket:
            result['processed'] += 1
            if self.scheduler._schedule_unseen(duri):
                result['scheduled'] += 1
            self.processedcount += 1
        result['t'] = time.time() - t0
        print >>sys.stderr, "IncomingQueue._process:%s" % result

    def process(self, maxn):
        '''schedule_unseen CURIs queued'''
        result = dict(processed=0, buckets=0)
        bucket = []
        for duri in self.getinq(result, maxn):
            result['processed'] += 1
            bucket.append(duri)
            if len(bucket) >= 50:
                result['buckets'] += 1
                self.execute(self._process, bucket)
                bucket = []
        if len(bucket) > 0:
            result['buckets'] += 1
            self.execute(self._process, bucket)
        return result

class Headquarters(seen_ud):
    def __init__(self):
        self.schedulers = {}
        self.incomingqueues = {}

    def shutdown(self):
        for inq in self.incomingqueues.values():
            inq.shutdown()

    def get_inq(self, job):
        inq = self.incomingqueues.get(job)
        if inq is None:
            jsch = self.get_scheduler(job)
            inq = IncomingQueue(job, jsch)
            self.incomingqueues[job] = inq
        #print >>sys.stderr, "inq=%d" % id(inq)
        return inq

    def get_scheduler(self, job):
        jsch = self.schedulers.get(job)
        if jsch is None:
            jsch = Scheduler(job)
            self.schedulers[job] = jsch
        return jsch

    # Headquarter - discovery

    def schedule(self, job, curi):
        self.get_scheduler(job).schedule(curi)

    def discovered(self, job, curis):
        return self.get_inq(job).add(curis)

    def processinq(self, job, maxn):
        '''process incoming queue for job. maxn parameter advicse
        upper limit on numbe of URIs processed in this single call.
        number of actually processed URIs may exceed it if incoming
        queue is storing URIs in chunks.'''
        # process calls Schdule.schedule_unseen() on each URI
        return self.get_inq(job).process(maxn)

    # Headquarter - feed

    def makecuri(self, o):
        # note that newly discovered CURI would not have _id if they got
        # scheduled to the WorkSet directly
        curi = dict(u=self.keyurl(o['u']), p=o.get('p',''))
        if '_id' in o: curi['id'] = str(o['_id'])
        for k in ('v', 'x', 'a', 'fp'):
            if k in o: curi[k] = o[k]
        return curi
        
    def feed(self, job, client, n):
        curis = self.get_scheduler(job).feed(client, n)
        return [self.makecuri(u) for u in curis]
        
    def finished(self, job, curis):
        '''curis 'u' has original URL, not in db key format'''
        jsch = self.get_scheduler(job)
        result = dict(processed=0)
        for curi in curis:
            #curi['u'] = self.urlkey(curi['u'])
            jsch.deschedule(curi)
            result['processed'] += 1
        return result

    def reset(self, job, client):
        return self.get_scheduler(job).reset(client)

    def flush(self, job):
        return self.get_scheduler(job).flush_clients()

    def seen(self, url):
        return db.seen.find_one(self.keyquery(self.urlkey(url)))

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

    def get_status(self, job):
        r = dict(job=job, hq=id(self))
        sch = self.schedulers.get(job)
        r['sch'] = sch and sch.get_status()
        inq = self.incomingqueues.get(job)
        r['inq'] = inq and inq.get_status()
        return r

executor = ThreadPoolExecutor(poolsize=10)
hq = Headquarters()
atexit.register(executor.shutdown)
atexit.register(hq.shutdown)

class ClientAPI:
    def __init__(self):
        #print >>sys.stderr, "new Headquarters instance created"
        pass
    def GET(self, job, action):
        if action in ('feed', 'ofeed', 'reset', 'processinq', 'setupindex',
                      'rehash', 'seen', 'rediscover', 'status', 'flush'):
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
        hq.discovered(job, [p])
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
        #print >>sys.stderr, "processinq %s" % result
        return self.jsonres(result)

    def do_feed(self, job):
        p = web.input(n=5, name=None, nodes=1)
        name, nodes, count = int(p.name), int(p.nodes), int(p.n)
        if count < 1: count = 5

        start = time.time()
        # return an JSON array of objects with properties:
        # uri, path, via, context and data
        r = hq.feed(job, (name, nodes), count)
        db.connection.end_request()
        print >>sys.stderr, "feed %s/%s %s in %.4fs" % (
            name, nodes, len(r), time.time() - start)
        #print >>sys.stderr, str(r)
        web.header('content-type', 'text/json')
        return self.jsonres(r)

    def do_reset(self, job):
        '''resets URIs' check-out state, make them schedulable to crawler'''
        p = web.input(name=None, nodes=1)
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

    def do_flush(self, job):
        '''flushes cached objects into database for safe shutdown'''
        hq.flush(job)
        db.connection.end_request()
        r = dict(ok=1)
        return self.jsonres(r)

    def do_seen(self, job):
        p = web.input()
        u = hq.seen(p.u)
        if u:
            del u['_id']
        result = dict(u=u)
        db.connection.end_request()
        return self.jsonres(result)

    def do_status(self, job):
        r = hq.get_status(job)
        if executor:
            r['workqueuesize'] = executor.work_queue.qsize()
        return self.jsonres(r)

    def do_test(self, job):
        web.debug(web.data())
        return str(web.ctx.env)

    def do_setupindex(self, job):
        db.seen.ensure_index([('u.u1', 1), ('u.h', 1)])
        db.seen.ensure_index([('co', 1), ('ws', 1)])
        #db.jobs[job].ensure_index([('u.u1', 1), ('u.h', 1)])
        #db.jobs[job].ensure_index([('co', 1), ('fp', 1)])

        r = dict(job=job, action='setupindex', sucess=1)
        return self.jsonres(r)

    def do_rehash(self, job):
        return self.jsonres(hq.rehash(job))
             
if __name__ == "__main__":
    app.run()
else:
    application = app.wsgifunc()
