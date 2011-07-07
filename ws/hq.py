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
from Queue import Queue, Empty
import traceback
import atexit
from fileinq import IncomingQueue
from filequeue import FileEnqueue, FileDequeue
from scheduler import Scheduler
import leveldb

urls = (
    '/(.*)/(.*)', 'ClientAPI',
    )
app = web.application(urls, globals())

if 'mongo' not in globals():
    mongo = pymongo.Connection(host='localhost', port=27017)
db = mongo.crawl
atexit.register(mongo.disconnect)

# _fp12 = FPGenerator(0xE758000000000000, 12)
_fp31 = FPGenerator(0xBA75BB4300000000, 31)
# _fp32 = FPGenerator(0x9B6C9A2F80000000, 32)
# _fp63 = FPGenerator(0xE1F8D6B3195D6D97, 63)
# _fp64 = FPGenerator(0xD74307D3FD3382DB, 64)

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
        
class CrawlMapper(object):
    '''maps client queue id to set of WorkSets'''
    def __init__(self, db, job, nworksets_bits):
        self.db = db
        self.job = job
        self.nworksets_bits = nworksets_bits
        self.nworksets = (1 << self.nworksets_bits)
        self.get_jobconf()
        self.load_workset_assignment()

    def get_jobconf(self):
        self.jobconf = self.db.jobconfs.find_one({'name':self.job})
        if self.jobconf is None:
            self.jobconf = {'name':self.job}
            self.db.jobconfs.save(self.jobconf)

    def create_default_workset_assignment(self):
        num_nodes = self.jobconf.get('nodes', 20)
        return list(itertools.islice(
                itertools.cycle(xrange(num_nodes)),
                0, self.nworksets))
        
    def load_workset_assignment(self):
        r = self.db.jobconfs.find_one({'name':self.job}, {'wscl':1})
        wscl = self.jobconf.get('wscl')
        if wscl is None:
            wscl = self.create_default_workset_assignment()
            self.jobconf['wscl'] = wscl
            self.db.jobconfs.save(self.jobconf)
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

class CrawlInfo(object):
    '''database of re-crawl infomation. keeps fetch result from previous
       crawl and makes it available in next cycle. this version uses MongoDB.'''
    def __init__(self, coll):
        self.coll = coll
        
    def save_result(self, furi):
        if 'a' not in furi: return
        if 'id' in furi:
            key = int(furi['id'])
        else:
            key = Seen.urikey(furi['u'])
        self.coll.update({'_id': key},
                         {'$set':{'a': furi['a'], 'u': furi['u']}},
                         upsert=True, multi=False)

    def get_crawlinfo(self, curi):
        if 'a' in curi:
            return curi['a']
        key = curi['id']
        r = self.coll.find_one({'_id': key}, {'a': 1})
        return r.get('a')
        
class Seen(object):
    _fp64 = FPGenerator(0xD74307D3FD3382DB, 64)
    S64 = 1<<63
    EXPIRE_NEVER = (1<<32)-1

    def __init__(self, dbdir):
        self.dbdir = dbdir
        self.seendb = leveldb.IntHash(self.dbdir,
                                      block_cache_size=4*(1024**3),
                                      block_size=4096,
                                      max_open_files=512,
                                      write_buffer_size=128*(1024**2))

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

    def already_seen(self, uri):
        key = Seen.urikey(uri)
        v = self.seendb.get(key)
        if not v:
            self.seendb.put(key, '1')
            return {'_id': key, 'e': 0}
        else:
            return {'_id': key, 'e': self.EXPIRE_NEVER}

class CrawlJob(object):
    NWORKSETS_BITS = 8
    #NWORKSETS = 1 << NWORKSETS_BITS
    def __init__(self, jobname):
        self.jobname = jobname
        self.mapper = CrawlMapper(db, self.jobname, self.NWORKSETS_BITS)
        self.seen = Seen(dbdir='/1/crawling/hq/seen')
        self.crawlinfodb = CrawlInfo(db.seen[self.jobname])
        self.scheduler = Scheduler(self.jobname, self.mapper, self.seen,
                                   self.crawlinfodb)
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
            t0 = time.time()
            furi = self.inq.get(0.01)
            result['td'] += (time.time() - t0)
            if furi is None: break
            result['processed'] += 1
            t0 = time.time()
            if self.scheduler.schedule_unseen(furi):
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

class Headquarters(object):
    '''now just a collection of CrawlJobs'''
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
