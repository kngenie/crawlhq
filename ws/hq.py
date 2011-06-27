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
import traceback
import atexit
from fileinq import IncomingQueue
from filequeue import FileEnqueue, FileDequeue

urls = (
    '/(.*)/(.*)', 'ClientAPI',
    )
app = web.application(urls, globals())

if 'mongo' not in globals():
    mongo = pymongo.Connection(host='localhost', port=27017)
db = mongo.crawl
atexit.register(mongo.disconnect)

_fp12 = FPGenerator(0xE758000000000000, 12)
_fp31 = FPGenerator(0xBA75BB4300000000, 31)
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
        #return (_fp63.fp(h) >> 1)
        return int(_fp31.fp(h) >> 33)

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
            th.daemon = True
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
        thname = threading.current_thread().name
        # print >>sys.stderr,"%s starting _work" % thname
        while 1:
            f, args = None, None
            try:
                f, args = self.work_queue.get()
                # print >>sys.stderr, "%s work starting %s%s" % (thname, f, args)
                f(*args)
                # print >>sys.stderr, "%s work done %s%s" % (thname, f, args)
            except Exception as ex:
                print >>sys.stderr, "work error in %s%s" % (f, args)
                traceback.print_exc()
            if self.__shutdown:
                break

    def execute(self, f, *args):
        self.work_queue.put((f, args))

    def __call__(self, f, *args):
        self.work_queue.put((f, args))
        
class Seen(object):
    _fp64 = FPGenerator(0xD74307D3FD3382DB, 64)
    S64 = 1<<63
    EXPIRE_NEVER = (1<<32)-1

    def __init__(self, jobname):
        self.coll = db.seen[jobname]

        # self.seen_pending = set()
        # self.seen_pending_lock = RLock()
        # self.seen_update_queue = Queue(1000)

    # def _update_seen(self):
    #     with self.seen_pending_lock:
            
    @staticmethod
    def urikey(uri):
        uhash = Seen._fp64.sfp(uri)
        # if uhash is 64bit negative number, make it so in Python, too
        #if uhash & Seen.S64: uhash = int((uhash & (Seen.S64 - 1)) - Seen.S64)
        return uhash

    @staticmethod
    def keyquery(key):
        return {'_id': key}

    @staticmethod
    def uriquery(uri):
        return Seen.keyquery(Seen.urikey(uri))

    def get(self, uri):
        q = Seen.uriquery(uri)
        u = self.coll.find_one(q)
        if not u:
            # sys.maxint is 64bit - use 32bit number
            u = {'_id': q['_id'], 'u': uri, 'e': self.EXPIRE_NEVER}
            self.coll.insert(u)
        return u

    def save_result(self, furi):
        if 'a' not in furi: return
        if 'id' in furi:
            key = int(furi['id'])
        else:
            key = Seen.urikey(furi['u'])
        self.coll.update({'_id': key},
                         {'$set':{'a': furi['a']}},
                         upsert=False, multi=False)

class WorkSet(object):
    WORKSET_DIR = '/1/crawling/hq/ws'
    def __init__(self, jobname, wsid):
        self.job = jobname
        self.wsid = wsid

        self.qdir = os.path.join(WorkSet.WORKSET_DIR,
                                 self.job, str(self.wsid))

        self.enq = FileEnqueue(self.qdir, maxage=0.0)
        self.deq = FileDequeue(self.qdir)

        self.running = True

        self.scheduledcount = 0
        self.checkedoutcount = 0
        self.finishedcount = 0
        self.activecount = 0

    def get_status(self):
        r = dict(id=self.wsid, running=self.running,
                 scheduled=self.scheduledcount,
                 checkedout=self.checkedoutcount,
                 finished=self.finishedcount
                 )
        return r

    def schedule(self, curi):
        self.enq.queue(curi)
        self.scheduledcount += 1

    def checkout(self, n):
        if not self.running:
            return []
        r = []
        while len(r) < n:
            curi = self.deq.get(timeout=0.01)
            if curi is None:
                self.enq.close()
                break
            r.append(curi)
        self.checkedoutcount += len(r)
        return r
    
    def deschedule(self, furi):
        self.finishedcount += 1
        
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
        worksets = []
        scheduledcount = 0
        activecount = 0
        for ws in self.worksets:
            worksets.append(ws.wsid)
            scheduledcount += ws.scheduledcount
            activecount += ws.activecount
        r['worksets'] = worksets
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
    NWORKSETS_BITS = 8
    #NWORKSETS = 256
    NWORKSETS = 1 << NWORKSETS_BITS
    
    def __init__(self, job):
        self.job = job
        self.clients = {}
        self.seen = Seen(self.job)
        self.get_jobconf()

        self.worksets = [WorkSet(job, wsid) for wsid in xrange(self.NWORKSETS)]
        self.load_workset_assignment()
        
    def shutdown(self):
        pass
    
    def get_status(self):
        r = dict(
            nworksets=self.NWORKSETS,
            clients={}
            )
        for i, client in self.clients.items():
            r['clients'][i] = client.get_status()
        return r

    def get_workset_status(self):
        r = [ws.get_status() for ws in self.worksets]
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

    def wsidforclient(self, client):
        '''return list of workset ids for node name of nodes-node cluster'''
        qids = [i for i in xrange(len(self.worksetclient))
                if self.worksetclient[i] == client[0]]
        return qids

    def get_clientqueue(self, client):
        clid = client[0]
        q = self.clients.get(clid)
        if q is None:
            worksets = [self.worksets[i] for i in self.wsidforclient(client)]
            q = ClientQueue(self.job, worksets)
            self.clients[clid] = q
            print >>sys.stderr, "new ClientQueue created for clid=%s" % clid
        return q

    def workset(self, curi):
        uc = urlsplit(curi['u'])
        h = uc.netloc
        p = h.find(':')
        if p > 0: h = h[:p]
        hosthash = int(_fp31.fp(h) >> (64 - self.NWORKSETS_BITS))
        return hosthash

    # def workset(self, hostfp):
    #     # don't use % (mod) - FP has much less even distribution in
    #     # lower bits.
    #     return hostfp >> (31 - self.NWORKSETS_BITS)

    # Scheduler - discovered event

    def schedule(self, curi):
        ws = self.workset(curi)
        return self.worksets[ws].schedule(curi)

    def schedule_unseen(self, incuri):
        '''schedule_unseen to be executed asynchronously'''
        #print >>sys.stderr, "_schedule_unseen(%s)" % incuri
        # uk = self.urlkey(incuri['u'])
        # q = self.keyquery(uk)
        # expire = incuri.get('e')

        # wsid = self.workset(q['fp'])
        # if self.worksets[wsid].is_active(incuri['u']):
        #     return False

        uri = incuri['u']
        #t0 = time.time()
        suri = self.seen.get(uri)
        #print >>sys.stderr, "seen %.3f" % (time.time() - t0,)
        if 'e' not in suri or suri['e'] < time.time():
            curi = dict(u=uri, id=suri['_id'],
                        w=dict(p=incuri.get('p'),
                               v=incuri.get('v'),
                               x=incuri.get('x'))
                        )
            #t0 = time.time()
            self.schedule(curi)
            #print >>sys.stderr, "schedule %.3f" % (time.time() - t0,)
            return True
        else:
            return False

        #print >>sys.stderr, "seen.find_one(%s)" % q
        curi = self.seen.find_one(q)
        #print >>sys.stderr, "seen.find_one:%.3f" % (time.time() - t0,)
        # if curi is None:
        #     curi = dict(u=uk, fp=q['fp'])
        #     if expire is not None:
        #         curi['e'] = expire
        #     curi['w'] = dict(p=incuri.get('p'), v=incuri.get('v'),
        #                      x=incuri.get('x'))
        #     self.schedule(curi)
        #     return True
        # else:
        #     if 'w' in curi: return False
        #     if expire is not None:
        #         curi['e'] = expire
        #     if curi.get('e', sys.maxint) < time.time():
        #         curi['w'] = dict(p=incuri.get('p'), v=incuri.get('v'),
        #                          x=incuri.get('x'))
        #         self.schedule(curi)
        #         return True
        #     return False

    def schedule_unseen_async(self, incuri):
        '''run schdule_unseen asynchronously'''
        executor.execute(self.schedule_unseen, incuri)
        return False

    # Scheduler - finished event

    def finished(self, furi, expire=None):
        '''furi: dict(u=url,
                      a=dict(s=status, d=digest, e=etag, m=last-modified)
                      )
        '''
        wsid = self.workset(furi)
        self.worksets[wsid].deschedule(furi)
        self.seen.save_result(furi)
        
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

class CrawlJob(object):
    def __init__(self, jobname):
        self.jobname = jobname
        self.scheduler = Scheduler(self.jobname)
        self.inq = IncomingQueue(self.jobname, self.scheduler)

    def shutdown(self):
        self.scheduler.shutdown()
        self.inq.close()

    def get_status(self):
        r = dict(job=self.jobname, hq=id(self))
        r['sch'] = self.scheduler and self.scheduler.get_status()
        r['inq'] = self.inq and self.inq.get_status()
        return r

    def get_workset_status(self, job):
        r = dict(job=self.jobname, hq=id(self))
        if self.scheduler:
            r['sch'] = id(self.scheduler)
            r['worksets'] = self.scheduler.get_workset_status()
        return r
        
    def discovered(self, curis):
        return self.inq.add(curis)

    def processinq(self, maxn):
        '''process incoming queue. maxn paramter adivces
        upper limit on number of URIs processed in this single call.
        actual number of URIs processed may exceed it if incoming queue
        stores URIs in chunks.'''
        #return self.inq.process(maxn)
        result = dict(processed=0, scheduled=0, td=0.0, ts=0.0)
        for count in xrange(maxn):
            result['processed'] += 1
            t0 = time.time()
            furi = self.inq.get(0.01)
            result['td'] += (time.time() - t0)
            t0 = time.time()
            if self.scheduler.schedule_unseen_async(furi):
                result['scheduled'] += 1
            result['ts'] += (time.time() - t0)
        return result

    def makecuri(self, o):
        return o

    def feed(self, client, n):
        curis = self.scheduler.feed(client, n)
        return [self.makecuri(u) for u in curis]

    def finished(self, curis):
        result = dict(processed=0)
        for curi in curis:
            self.scheduler.finished(curi)
            result['processed'] += 1
        return result

    def reset(self, client):
        return self.scheduler.reset(client)

    def flush(self):
        return self.scheduler.flush_clients()

class Headquarters(seen_ud):
    def __init__(self):
        self.jobs = {}

    def shutdown(self):
        for job in self.jobs.values():
            job.shutdown()

    def get_job(self, jobname):
        # TODO synchronize access to self.jobs
        job = self.jobs.get(jobname)
        if job is None:
            job = self.jobs[jobname] = CrawlJob(jobname)
        return job

        self.schedulers = {}
        self.incomingqueues = {}

    # def shutdown(self):
    #     for inq in self.incomingqueues.values():
    #         inq.shutdown()

    # def get_inq(self, job):
    #     inq = self.incomingqueues.get(job)
    #     if inq is None:
    #         jsch = self.get_scheduler(job)
    #         inq = IncomingQueue(job, jsch)
    #         self.incomingqueues[job] = inq
    #     #print >>sys.stderr, "inq=%d" % id(inq)
    #     return inq

    # def get_scheduler(self, job):
    #     jsch = self.schedulers.get(job)
    #     if jsch is None:
    #         jsch = Scheduler(job)
    #         self.schedulers[job] = jsch
    #     return jsch

    # # Headquarter - discovery

    # def schedule(self, job, curi):
    #     self.get_scheduler(job).schedule(curi)

    # def discovered(self, job, curis):
    #     return self.get_inq(job).add(curis)

    # def processinq(self, job, maxn):
    #     '''process incoming queue for job. maxn parameter advicse
    #     upper limit on numbe of URIs processed in this single call.
    #     number of actually processed URIs may exceed it if incoming
    #     queue is storing URIs in chunks.'''
    #     # process calls Schdule.schedule_unseen() on each URI
    #     return self.get_inq(job).process(maxn)

    # # Headquarter - feed

    # def makecuri(self, o):
    #     # note that newly discovered CURI would not have _id if they got
    #     # scheduled to the WorkSet directly
    #     curi = dict(u=self.keyurl(o['u']), p=o.get('p',''))
    #     if '_id' in o: curi['id'] = str(o['_id'])
    #     for k in ('v', 'x', 'a', 'fp'):
    #         if k in o: curi[k] = o[k]
    #     return curi
        
    # def feed(self, job, client, n):
    #     curis = self.get_scheduler(job).feed(client, n)
    #     return [self.makecuri(u) for u in curis]
        
    # def finished(self, job, curis):
    #     '''curis 'u' has original URL, not in db key format'''
    #     jsch = self.get_scheduler(job)
    #     result = dict(processed=0)
    #     for curi in curis:
    #         #curi['u'] = self.urlkey(curi['u'])
    #         jsch.deschedule(curi)
    #         result['processed'] += 1
    #     return result

    # def reset(self, job, client):
    #     return self.get_scheduler(job).reset(client)

    # def flush(self, job):
    #     return self.get_scheduler(job).flush_clients()

    # def seen(self, url):
    #     return db.seen.find_one(self.keyquery(self.urlkey(url)))

    # def rehash(self, job):
    #     result = dict(seen=0, jobs=0)
    #     it = db.seen.find()
    #     for u in it:
    #         u['fp'] = self.keyfp(u['u'])
    #         db.seen.save(u)
    #         result['seen'] += 1

    #     it = db.jobs[job].find()
    #     for u in it:
    #         uu = db.seen.find_one({'_id':u['_id']})
    #         if uu:
    #             u['fp'] = self.workset(uu['fp'])
    #             db.jobs[job].save(u)
    #             result['jobs'] += 1

    # def get_status(self, job):
    #     r = dict(job=job, hq=id(self))
    #     sch = self.schedulers.get(job)
    #     r['sch'] = sch and sch.get_status()
    #     inq = self.incomingqueues.get(job)
    #     r['inq'] = inq and inq.get_status()
    #     return r
    def get_workset_status(self, job):
        r = dict(job=job, hq=id(self))
        sch = self.schedulers.get(job)
        if sch:
            r['sch'] = id(sch)
            r['worksets'] = sch.get_workset_status()
        return r

executor = ThreadPoolExecutor(poolsize=4)
hq = Headquarters()
atexit.register(executor.shutdown)
atexit.register(hq.shutdown)

class ClientAPI:
    def __init__(self):
        #print >>sys.stderr, "new Headquarters instance created"
        pass
    def GET(self, job, action):
        if action in ('feed', 'ofeed', 'reset', 'processinq', 'setupindex',
                      'rehash', 'seen', 'rediscover', 'status', 'worksetstatus',
                      'flush'):
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
        result = hq.get_job(job).finished(curis)
        result['t'] = time.time() - start

        print >>sys.stderr, "mfinished ", result
        db.connection.end_request()
        return self.jsonres(result)

    def do_finished(self, job):
        p = web.input(a='{}', f=None, id=None)
        curi = dict(u=p.u, f=p.f, a=json.loads(p.a))
        if p.id: curi['id'] = p.id

        start = time.time()
        result = hq.get_job(job).finished([curi])
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
        hq.get_job(job).discovered([p])
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

        result.update(hq.get_job(job).discovered(curis))

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
        result.update(hq.get_job(job).processinq(maxn))
        
        result.update(t=(time.time() - start))
        db.connection.end_request()
        #print >>sys.stderr, "processinq %s" % result
        return self.jsonres(result)

    def do_feed(self, job):
        p = web.input(n=5, name=None, nodes=1)
        name = int(p.name)
        nodes = int(p.nodes)
        count = int(p.n)
        if count < 1: count = 5

        start = time.time()
        # return an JSON array of objects with properties:
        # uri, path, via, context and data
        r = hq.get_job(job).feed((name, nodes), count)
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
        r.update(hq.get_job(job).reset((name, nodes)))
        db.connection.end_request()
        print >>sys.stderr, "reset %s" % str(r)
        # TODO: return number of URIs reset
        return self.jsonres(r)

    def do_flush(self, job):
        '''flushes cached objects into database for safe shutdown'''
        hq.get_job(job).flush()
        db.connection.end_request()
        r = dict(ok=1)
        return self.jsonres(r)

    def do_seen(self, job):
        p = web.input()
        u = hq.get_job(job).seen(p.u)
        if u:
            del u['_id']
        result = dict(u=u)
        db.connection.end_request()
        return self.jsonres(result)

    def do_status(self, job):
        r = hq.get_job(job).get_status()
        if executor:
            r['workqueuesize'] = executor.work_queue.qsize()
        return self.jsonres(r)

    def do_worksetstatus(self, job):
        r = hq.get_job(job).get_workset_status()
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
