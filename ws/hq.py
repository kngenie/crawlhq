#!/usr/bin/python
#
# Headquarters server for crawling cloud control
#
# make sure to specify "lib" directory in python-path in WSGI config
#
import sys, os
import web
import pymongo
import json
import time
import re
import itertools
from cfpgenerator import FPGenerator
from urlparse import urlsplit, urlunsplit
import threading
import random
from Queue import Queue, Empty, Full, LifoQueue
import traceback
import atexit
from fileinq import IncomingQueue
from filequeue import FileEnqueue, FileDequeue
import sortdequeue
from scheduler import Scheduler
import leveldb
from executor import *
import logging
from mongodomaininfo import DomainInfo

logging.basicConfig(level=logging.WARN)

HQ_HOME = '/1/crawling/hq'

urls = (
    '/(.*)/(.*)', 'ClientAPI',
    )
app = web.application(urls, globals())

# if 'mongo' not in globals():
#     mongohosts = ['localhost',
#                   'crawl451.us.archive.org',
#                   'crawl401.us.archive.org',
#                   'crawl402.us.archive.org',
#                   'crawl403.us.archive.org']
#     mongo = pymongo.Connection(host=mongohosts, port=27017)
# db = mongo.crawl
# atexit.register(mongo.disconnect)

# _fp12 = FPGenerator(0xE758000000000000, 12)
_fp31 = FPGenerator(0xBA75BB4300000000, 31)
# _fp32 = FPGenerator(0x9B6C9A2F80000000, 32)
# _fp63 = FPGenerator(0xE1F8D6B3195D6D97, 63)
# _fp64 = FPGenerator(0xD74307D3FD3382DB, 64)

class MongoJobConfigs(object):
    def __init__(self):
        self.mongo = pymongo.Connection()
        self.db = self.mongo.crawl

    def get_jobconf(self, job, pname, default=None, nocreate=0):
        jobconf = self.db.jobconfs.find_one({'name':job}, {pname: 1})
        if jobconf is None and not nocreate:
            jobconf = {'name':self.job}
            self.db.jobconfs.save(jobconf)
        return jobconf.get(pname, default)

    def save_jobconf(self, job, pname, value, nocreate=0):
        if nocreate:
            self.db.jobconfs.update({'name': job}, {pname: value},
                                    multi=False, upsert=False)
        else:
            self.db.jobconfs.update({'name': job}, {pname: value},
                                    multi=False, upsert=True)

    def job_exists(self, job):
        o = self.db.jobconfs.find_one({'name':job}, {'name':1})
        return o is not None

class CrawlMapper(object):
    '''maps client queue id to set of WorkSets'''
    def __init__(self, jobconfigs, job, nworksets_bits):
        self.jobconfigs = jobconfigs
        self.job = job
        self.nworksets_bits = nworksets_bits
        self.nworksets = (1 << self.nworksets_bits)
        self.load_workset_assignment()

    def create_default_workset_assignment(self):
        num_nodes = self.jobconfigs.get_jobconf(self.job, 'nodes', 20)
        return list(itertools.islice(
                itertools.cycle(xrange(num_nodes)),
                0, self.nworksets))
        
    def load_workset_assignment(self):
        wscl = self.jobconfigs.get_jobconf(self.job, 'wscl')
        if wscl is None:
            wscl = self.create_default_workset_assignment()
            self.jobconfigs.save_jobconf(self.job, 'wscl', wscl)
        if len(wscl) > self.nworksets:
            wscl[self.nworksets:] = ()
        elif len(wscl) < self.nworksets:
            wscl.extend(itertools.repeat(None, self.nworksets-len(wscl)))
        self.worksetclient = wscl

    def wsidforclient(self, client):
        '''return list of workset ids for node name of nodes-node cluster'''
        qids = [i for i in xrange(len(self.worksetclient))
                if self.worksetclient[i] == client[0]]
        return qids

    def workset(self, curi):
        '''reutrns WorkSet id to which curi should be dispatched'''
        uc = urlsplit(curi['u'])
        h = uc.netloc
        p = h.find(':')
        if p > 0: h = h[:p]
        # Note: don't use % (mod) - FP hash much less even distribution in
        # lower bits.
        hosthash = int(_fp31.fp(h) >> (64 - self.nworksets_bits))
        return hosthash

class MongoCrawlInfo(object):
    '''database of re-crawl infomation. keeps fetch result from previous
       crawl and makes it available in next cycle. this version uses MongoDB.'''
    def __init__(self, jobname):
        self.jobname = jobname
        self.mongo = pymongo.Connection()
        self.db = self.mongo.crawl
        self.coll = self.db.seen[self.jobname]
        
    def shutdown(self):
        self.coll = None
        self.db = None
        self.mongo.disconnect()

    def save_result(self, furi):
        if 'a' not in furi: return
        if 'id' in furi:
            key = int(furi['id'])
        else:
            key = Seen.urikey(furi['u'])
        self.coll.update({'_id': key},
                         {'$set':{'a': furi['a'], 'u': furi['u']}},
                         upsert=True, multi=False)

    def update_crawlinfo(self, curis):
        '''updates curis with crawlinfo in bulk'''
        tasks = [(self._set_crawlinfo, (curi,))
                 for curi in curis if 'a' not in curi]
        if tasks:
            b = TaskBucket(tasks)
            b.execute_wait(executor, 4)
        
    def _set_crawlinfo(self, curi):
        key= curi['id']
        r = self.coll.find_one({'_id': key}, {'a': 1})
        if r and 'a' in r:
            curi['a'] = r['a']

    def get_crawlinfo(self, curi):
        if 'a' in curi:
            return curi['a']
        key = curi['id']
        r = self.coll.find_one({'_id': key}, {'a': 1})
        return r and r.get('a')
        
class Seen(object):
    _fp64 = FPGenerator(0xD74307D3FD3382DB, 64)
    S64 = 1<<63
    EXPIRE_NEVER = (1<<32)-1

    def __init__(self, dbdir):
        self.dbdir = dbdir
        self.ready = threading.Event()
        self._open()
        self.ready.set()
        self.putqueue = Queue(1000)
        self.drainlock = threading.RLock()

    def _open(self):
        logging.info("opening seen-db %s", self.dbdir)
        self.seendb = leveldb.IntHash(self.dbdir,
                                      block_cache_size=16*(1024**3),
                                      block_size=4096,
                                      max_open_files=256,
                                      write_buffer_size=128*(1024**2))
        logging.info("seen-db %s is ready", self.dbdir)

    def close(self):
        logging.info("flushing putqueue (%d)", self.putqueue.qsize())
        self.drain_putqueue()
        logging.info("closing leveldb...")
        self.seendb.close()
        logging.info("closing leveldb...done")
        self.seendb = None

    def _count(self):
        self.ready.wait()
        it = self.seendb.new_iterator()
        if not it: return 0
        it.seek_to_first()
        c = 0
        while it.valid():
            c += 1
            it.next()
        return c

    @staticmethod
    def urikey(uri):
        uhash = Seen._fp64.sfp(uri)
        return uhash

    @staticmethod
    def keyquery(key):
        return {'_id': key}

    @staticmethod
    def uriquery(uri):
        return Seen.keyquery(Seen.urikey(uri))

    def drain_putqueue(self):
        # prevent multiple threads from racing on draining - it just
        # makes performance worse. should not happen often
        with self.drainlock:
            try:
                while 1:
                    key = self.putqueue.get_nowait()
                    self.seendb.put(key, '1')
            except Empty:
                pass

    def already_seen(self, uri):
        self.ready.wait()
        key = Seen.urikey(uri)
        v = self.seendb.get(key)
        if not v:
            #self.seendb.put(key, '1')
            while 1:
                try:
                    self.putqueue.put_nowait(key)
                    break
                except Full:
                    self.drain_putqueue()
            return {'_id': key, 'e': 0}
        else:
            return {'_id': key, 'e': self.EXPIRE_NEVER}

    def repair(self):
        self.ready.clear()
        try:
            self.close()
            leveldb.IntHash.repair_db(self.dbdir)
            self._open()
        finally:
            self.ready.set()

    def clear(self):
        self.ready.clear()
        try:
            self.close()
            logging.info('deleting files in %s', self.dbdir)
            for f in os.listdir(self.dbdir):
                p = os.path.join(self.dbdir, f)
                try:
                    os.remove(p)
                except:
                    logging.warn('os.remove failed on %s', p, exc_info=1)
            logging.info('done deleting files, re-creating %s', self.dbdir)
            self._open()
        finally:
            self.ready.set()

class FPSortingQueueFileReader(sortdequeue.SortingQueueFileReader):
    def urikey(self, o):
        Seen.urikey(o['u'])

class PooledIncomingQueue(IncomingQueue):
    def init_queues(self, n=5, buffsize=0, maxsize=1000*1000*1000):
        maxsize = maxsize / n
        self.write_executor = ThreadPoolExecutor(poolsize=2, queuesize=100)
        self.rqfile = FileDequeue(self.qdir, reader=FPSortingQueueFileReader)
        self.qfiles = [FileEnqueue(self.qdir, suffix=str(i),
                                   maxsize=maxsize,
                                   buffer=buffsize,
                                   executor=self.write_executor)
                       for i in range(n)]
        self.avail = LifoQueue()
        for q in self.qfiles:
            self.avail.put(q)

    def add(self, curis):
        processed = 0
        t0 = time.time()
        enq = self.avail.get()
        t = time.time() - t0
        if t > 1e-3: logging.warn('self.avail.get() %.4f', t)
        try:
            enq.queue(curis)
            self.addedcount += len(curis)
            processed += len(curis)
            return dict(processed=processed)
        finally:
            t0 = time.time()
            self.avail.put(enq)
            t = time.time() - t0
            if t > 1e-4: logging.warn('slow self.avail.put() %.4f', t)

class OpenQueueQuota(object):
    def __init__(self, maxopens=256):
        self.maxopens = maxopens
        self.lock = threading.Condition()
        self.opens = set()

    def opening(self, fileq):
        logging.debug("opening %s", fileq)
        with self.lock:
            logging.debug("opening:locked")
            while len(self.opens) >= self.maxopens:
                for q in list(self.opens):
                    logging.debug("  try closing %s", q)
                    if q.detach():
                        break
                # if everybody's busy (unlikely), keep trying in busy loop
                # sounds bad...
            self.opens.add(fileq)
        logging.debug("opening:released")

    def closed(self, fileq):
        logging.debug("closed %s", fileq)
        with self.lock:
            logging.debug("closed:locked")
            self.opens.discard(fileq)
            #self.lock.notify()
        logging.debug("closed:released")
            
class HashSplitIncomingQueue(IncomingQueue):
    def init_queues(self, window_bits=54, buffsize=0, maxsize=200*1000*1000,
                    maxopens=256):
        self.opener = OpenQueueQuota(maxopens=maxopens)
        if window_bits < 1 or window_bits > 63:
            raise ValueError, 'window_bits must be 1..63'
        self.window_bits = window_bits

        self.nwindows = (1 << 64 - self.window_bits)
        self.win_mask = self.nwindows - 1
                               
        maxsize = maxsize / self.nwindows

        # dequeue side
        self.rqfile = FileDequeue(self.qdir)

        # queues for each sub-range
        self.write_executor = ThreadPoolExecutor(poolsize=2, queuesize=10)
        self.qfiles = [FileEnqueue(self.qdir, maxsize=maxsize,
                                   suffix=str(i), opener=opener,
                                   buffer=buffsize,
                                   executor=self.write_executor)
                       for i in range(self.nwindows)]

    def get_status(self):
        s = IncomingQueue.get_status()
        s.update(writequeue=self.write_executor.work_queue.qsize())
        return s

    @property
    def maxopens(self):
        return self.opener.maxopens
    @maxopens.setter
    def maxopens(self, v):
        self.opener.maxopens = v

    PARAMS = [('buffsize', int), ('maxopens', int)]

    def hash(self, curi):
        if 'id' in curi:
            return curi['id']
        else:
            h = Seen.urikey(curi['u'])
            curi['id'] = h
            return h

    def queue_dispatch(self, curi):
        h = self.hash(curi)
        win = (h >> self.window_bits) & self.win_mask
        return win

    def add(self, curis):
        processed = 0
        for curi in curis:
            win = self.queue_dispatch(curi)
            enq = self.qfiles[win]
            enq.queue(curi)
            self.addedcount += 1
            processed += 1
        return dict(processed=processed)

class CrawlJob(object):
    NWORKSETS_BITS = 8

    def __init__(self, jobconfigs, jobname, crawlinfo, domaininfo):
        self.jobconfigs = jobconfigs
        self.jobname = jobname
        self.mapper = CrawlMapper(self.jobconfigs, self.jobname,
                                  self.NWORKSETS_BITS)
        self.seen = Seen(dbdir=os.path.join(HQ_HOME, 'seen', self.jobname))
        #self.crawlinfodb = MongoCrawlInfo(self.jobname)
        self.crawlinfodb = crawlinfo
        self.domaininfo = domaininfo
        self.scheduler = Scheduler(self.jobname, self.mapper,
                                   self.crawlinfodb)
        # self.inq = HashSplitIncomingQueue(
        #     qdir=os.path.join(HQ_HOME, 'inq', self.jobname),
        #     buffsize=500)
        self.inq = PooledIncomingQueue(
            qdir=os.path.join(HQ_HOME, 'inq', self.jobname),
            buffsize=500)

        #self.discovered_executor = ThreadPoolExecutor(poolsize=1)

    def shutdown(self):
        logging.info("closing seen db")
        self.seen.close()
        logging.info("shutting down scheduler")
        self.scheduler.shutdown()
        logging.info("closing incoming queues")
        self.inq.flush()
        self.inq.close()
        logging.info("shutting down crawlinfo")
        self.crawlinfodb.shutdown()
        logging.info("done.")
        #self.discovered_executor.shutdown()

    def get_status(self):
        r = dict(job=self.jobname, hq=id(self))
        r['sch'] = self.scheduler and self.scheduler.get_status()
        r['inq'] = self.inq and self.inq.get_status()
        return r

    def get_workset_status(self):
        r = dict(job=self.jobname, crawljob=id(self))
        if self.scheduler:
            r['sch'] = id(self.scheduler)
            r['worksets'] = self.scheduler.get_workset_status()
        return r
        
    #def discovered_async(self, curis):
    #    return self.inq.add(curis)

    def get_domaininfo(self, url):
        uc = urlsplit(url)
        host = uc.netloc
        p = host.find(':')
        if p > 0: host = host[:p]
        di = self.domaininfo.get(host)
        return di
        
    def schedule(self, curis):
        '''schedule curis bypassing seen-check. typically used for starting
           new crawl cycle.'''
        scheduled = 0
        for curi in curis:
            self.scheduler.schedule(curi)
            scheduled += 1
        return dict(processed=scheduled, scheduled=scheduled)

    def discovered(self, curis):
        return self.inq.add(curis)
        
    def processinq(self, maxn):
        '''process incoming queue. maxn paramter adivces
        upper limit on number of URIs processed in this single call.
        actual number of URIs processed may exceed it if incoming queue
        stores URIs in chunks.'''
        result = dict(processed=0, scheduled=0, excluded=0, td=0.0, ts=0.0)
        for count in xrange(maxn):
            t0 = time.time()
            furi = self.inq.get(0.01)
            result['td'] += (time.time() - t0)
            if furi is None: break
            result['processed'] += 1
            di = self.get_domaininfo(furi['u'])
            if di and di['exclude']:
                result['excluded'] += 1
                continue
            t0 = time.time()
            u = furi['u']
            suri = self.seen.already_seen(u)
            if suri['e'] < int(time.time()):
                w = dict()
                for k in ('p','v','x'):
                    m = furi.get(k)
                    if m is not None:
                        w[k] = m
                curi = dict(u=u, id=suri['_id'], w=w)
                self.scheduler.schedule(curi)
                result['scheduled'] += 1
            result['ts'] += (time.time() - t0)
        # currently no access to MongoDB
        #self.mongo.end_request()
        return result

    def makecuri(self, o):
        return o

    def feed(self, client, n):
        logging.warn('feed %s begin', client)
        curis = self.scheduler.feed(client, n)
        r = [self.makecuri(u) for u in curis]
        #self.mongo.end_request()
        return r

    def finished(self, curis):
        result = dict(processed=0)
        for curi in curis:
            self.scheduler.finished(curi)
            result['processed'] += 1
        #self.mongo.end_request()
        return result

    def reset(self, client):
        return self.scheduler.reset(client)

    def flush(self):
        self.inq.flush()
        self.inq.close()
        return self.scheduler.flush_clients()

class Headquarters(object):
    '''now just a collection of CrawlJobs'''
    def __init__(self):
        self.jobs = {}
        self.jobslock = threading.RLock()
        # single shared CrawlInfo database
        # named 'wide' for historical reasons.
        self.crawlinfo = MongoCrawlInfo('wide')
        self.domaininfo = DomainInfo()
        self.jobconfigs = MongoJobConfigs()

    def shutdown(self):
        for job in self.jobs.values():
            job.shutdown()
        self.domaininfo.shutdown()

    def get_job(self, jobname, nocreate=False):
        with self.jobslock:
            job = self.jobs.get(jobname)
            if job is None:
                if nocreate and not self.jobconfigs.job_exists(jobname):
                    raise ValueError('unknown job %s' % jobname)
                job = self.jobs[jobname] = CrawlJob(
                    self.jobconfigs, jobname, self.crawlinfo, self.domaininfo)
            return job

        self.schedulers = {}
        self.incomingqueues = {}

    def get_workset_status(self, job):
        r = self.get_job(job).get_workset_status()
        r['hq'] = id(self)
        return r

    PARAMS = [('loglevel', int)]

    @property
    def loglevel(self):
        return logging.getLogger().getEffectiveLevel()
    @loglevel.setter
    def loglevel(self, level):
        logging.getLogger().setLevel(level)

#executor = ThreadPoolExecutor(poolsize=4)
hq = Headquarters()
#atexit.register(executor.shutdown)
atexit.register(hq.shutdown)

def parse_bool(s):
    s = s.lower()
    if s == 'true': return True
    if s == 'false': return False
    try:
        i = int(s)
        return bool(i)
    except ValueError:
        pass
    return bool(s)

class ClientAPI:
    def __init__(self):
        #print >>sys.stderr, "new Headquarters instance created"
        pass
    def GET(self, job, action):
        h = getattr(self, 'do_' + action, None)
        if h is None: raise web.notfound(action)
        response = h(job)
        if isinstance(response, dict):
            response = json.dumps(response, check_circular=False,
                                  separators=',:') + "\n"
            web.header('content-type', 'text/json')
        return response

    def POST(self, job, action):
        h = getattr(self, 'post_' + action, None)
        if h is None: raise web.notfound(action)
        response = h(job)
        if isinstance(response, dict):
            response = json.dumps(response, check_circular=False,
                                  separators=',:') + "\n"
            web.header('content-type', 'text/json')
        return response

    def jsonres(self, r):
        web.header('content-type', 'text/json')
        return json.dumps(r, check_circular=False, separators=',:') + "\n"
    
    def post_mfinished(self, job):
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

        logging.debug("mfinished %s", result)
        return result

    def post_finished(self, job):
        p = web.input(a='{}', f=None, id=None)
        curi = dict(u=p.u, f=p.f, a=json.loads(p.a))
        if p.id: curi['id'] = p.id

        start = time.time()
        result = hq.get_job(job).finished([curi])
        result['t'] = time.time() - start
        logging.debug("finished %s", result)
        return result
        
    def post_discovered(self, job):
        '''receives URLs found as 'outlinks' in just downloaded page.
        do_discovered runs already-seen test and then schedule a URL
        for crawling with last-modified and content-hash obtained
        from seen database (if previously crawled)'''
        
        p = web.input(force=0)
        if 'u' not in p:
            return {error:'u value missing'}

        furi = dict(u=p.u)
        for k in ('p', 'v', 'x'):
            if k in p and p[k] is not None: furi[k] = p[k]

        cj = hq.get_job(job)
        if p.force:
            return cj.schedule([furi])
        else:
            return cj.discovered([furi])

    def post_mdiscovered(self, job):
        '''receives submission of "discovered" events in batch.
        this version simply queues data submitted in incoming queue
        to minimize response time. entries in the incoming queue
        will be processed by separate processinq call.'''
        result = dict(processed=0)
        data = None
        try:
            data = web.data()
            curis = json.loads(data)
        except:
            web.debug("json.loads error:data=%s" % data)
            result['error'] = 'invalid data - json parse failed'
            return result
        if isinstance(curis, dict):
            force = curis.get('f')
            curis = curis['u']
        elif isinstance(curis, list):
            force = False
        else:
            result['error'] = 'invalid data - not an array'
            return result
        if len(curis) == 0:
            return result

        cj = hq.get_job(job)
        start = time.time()

        if force:
            result.update(cj.schedule(curis))
        else:
            result.update(cj.discovered(curis))

        t = time.time() - start
        result.update(t=t)
        if t / len(curis) > 1e-3:
            logging.warn("slow discovered: %.3fs for %d", t, len(curis))
        else:
            logging.debug("mdiscovered %s", result)
        return result
            
    def do_processinq(self, job):
        '''process incoming queue. max parameter advise upper limit on
        number of URIs processed. actually processed URIs may exceed that
        number if incoming queue is storing URIs in chunks'''
        p = web.input(max=5000)
        maxn = int(p.max)
        result = dict(job=job, inq=0, processed=0, scheduled=0, max=maxn,
                      td=0.0, ts=0.0)
        start = time.time()

        # transient instance for now
        result.update(hq.get_job(job).processinq(maxn))
        
        result.update(t=(time.time() - start))
        return result

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
        t = time.time() - start
        if t > 0.01:
            logging.warn("slow feed %s %s, %.4fs", name, len(r), t)
        else:
            logging.debug("feed %s %s, %.4fs", name, len(r), t)

        web.header('content-type', 'text/json')
        return self.jsonres(r)

    def do_reset(self, job):
        '''resets URIs' check-out state, make them schedulable to crawler'''
        p = web.input(name=None, nodes=1)
        name = int(p.name)
        nodes = int(p.nodes)
        r = dict(name=name, nodes=nodes)
        logging.info("received reset request: %s", str(r))
        if name is None or nodes is None:
            r.update(msg='name and nodes are required')
            return r
        r.update(hq.get_job(job).reset((name, nodes)))
        logging.info("reset %s", str(r))
        # TODO: return number of URIs reset
        return r

    def do_flush(self, job):
        '''flushes cached objects into database for safe shutdown'''
        hq.get_job(job).flush()
        r = dict(ok=1)
        return r

    def do_seen(self, job):
        p = web.input()
        u = hq.get_job(job).seen.already_seen(p.u)
        if u:
            u['id'] = u.pop('_id', None)
        result = dict(u=u)
        return result

    def do_status(self, job):
        r = hq.get_job(job).get_status()
        # if executor:
        #     r['workqueuesize'] = executor.work_queue.qsize()
        return r

    def do_worksetstatus(self, job):
        r = hq.get_job(job).get_workset_status()
        return r

    def do_seencount(self, job):
        '''can take pretty long time'''
        try:
            count = hq.get_job(job, nocreate=1).seen._count()
            return dict(success=1, seencount=count)
        except Exception as ex:
            return dict(success=0, err=str(ex))

    def do_clearseen(self, job):
        try:
            hq.get_job(job, nocreate=1).seen.clear()
            return dict(success=1)
        except Exception as ex:
            return dict(success=0, err=str(ex))

    def do_test(self, job):
        web.debug(web.data())
        return str(web.ctx.env)

    def do_rehash(self, job):
        return hq.rehash(job)

    def do_repairseen(self, job):
        logging.info('repairing seen-db')
        hq.get_job(job).seen.repair()
        logging.info('repairing seen-db done')
        return dict(ok=1)

    def do_param(self, job):
        p = web.input(value=None, name='')
        name = p.name
        nc = name.split('.')
        if len(nc) != 2:
            return dict(success=0, error='bad parameter name', name=name)
        on, pn = nc
        j = hq.get_job(job)
        o = dict(scheduler=j.scheduler, inq=j.inq, hq=hq).get(on)
        if o is None:
            return dict(success=0, error='no such object', name=name)
        params = o.PARAMS
        pe = None
        for d in params:
            if d[0] == pn:
                pe = d
                break
        if pe is None:
            return dict(success=0, error='no such parameter', name=name)
        r = dict(success=1, name=name, v=getattr(o, pe[0], None))
        if p.value is not None:
            r['value'] = p.value
            conv = pe[1]
            if conv == bool: conv = parse_bool
            try:
                r['nv'] = conv(p.value)
                setattr(o, pe[0], r['nv'])
            except ValueError:
                r.update(success=0, error='bad value')
            except AttributeError as ex:
                r.update(success=0, error=str(ex), o=str(o), a=pe[0])
        logging.info("param: %s", r)
        return r
             
if __name__ == "__main__":
    app.run()
else:
    application = app.wsgifunc()
