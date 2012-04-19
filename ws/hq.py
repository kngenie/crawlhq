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
from gzip import GzipFile
from cStringIO import StringIO
from urlparse import urlsplit, urlunsplit
import threading
import random
from Queue import Queue, Empty, Full, LifoQueue
import atexit
import logging

import hqconfig
from fileinq import IncomingQueue
from filequeue import FileEnqueue, FileDequeue
from scheduler import Scheduler
from executor import *
from zkcoord import Coordinator
from mongodomaininfo import DomainInfo
from mongocrawlinfo import CrawlInfo
from mongojobconfigs import JobConfigs
import urihash
from dispatcher import WorksetMapper, Dispatcher, FPSortingQueueFileReader
from seen import Seen
from weblib import QueryApp
from handlers import *

class CrawlMapper(WorksetMapper):
    """maps client queue id to set of WorkSets
    """
    def __init__(self, crawljob, nworksets_bits):
        super(CrawlMapper, self).__init__(nworksets_bits)
        self.crawljob = crawljob
        self.jobconfigs = self.crawljob.jobconfigs
        self.job = self.crawljob.jobname
        #self.nworksets = (1 << nworksets_bits)
        self.load_workset_assignment()

        self.client_last_active = {}

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

    def wsidforclient(self, clid):
        '''return list of workset ids for node name of nodes-node cluster'''
        # XXX linear search
        qids = [i for i in xrange(len(self.worksetclient))
                if self.worksetclient[i] == clid]
        return qids

    def client_activating(self, clid):
        """called back by Scheduler when client gets a new feed request
        when it is inactive."""
        for wsid in self.wsidforclient(clid):
            self.crawljob.workset_activating(wsid)

class PooledIncomingQueue(IncomingQueue):
    def init_queues(self, n=5, buffsize=0, maxsize=1000*1000*1000):
        maxsize = maxsize / n
        self.write_executor = ThreadPoolExecutor(poolsize=1, queuesize=100)
        self.rqfile = FileDequeue(self.qdir, reader=FPSortingQueueFileReader)
        self.qfiles = [FileEnqueue(self.qdir, suffix=str(i),
                                   maxsize=maxsize,
                                   buffer=buffsize,
                                   executor=self.write_executor)
                       for i in range(n)]
        self.avail = LifoQueue()
        for q in self.qfiles:
            self.avail.put(q)

    def shutdown(self):
        super(PooledIncomingQueue, self).shutdown()
        self.write_executor.shutdown()

    def add(self, curis):
        processed = 0
        t0 = time.time()
        enq = self.avail.get()
        t = time.time() - t0
        if t > 0.1: logging.warn('self.avail.get() %.4f', t)
        try:
            enq.queue(curis)
            self.addedcount += len(curis)
            processed += len(curis)
            return dict(processed=processed)
        finally:
            t0 = time.time()
            self.avail.put(enq)
            t = time.time() - t0
            if t > 0.1: logging.warn('slow self.avail.put() %.4f', t)

class CrawlJob(object):

    def __init__(self, jobconfigs, jobname, crawlinfo, domaininfo):
        self.jobconfigs = jobconfigs
        self.jobname = jobname
        self.mapper = CrawlMapper(self, hqconfig.NWORKSETS_BITS)
        self.scheduler = Scheduler(hqconfig.worksetdir(self.jobname),
                                   self.mapper)

        self.domaininfo = domaininfo

        self.dispatcher = Dispatcher(self.domaininfo,
                                     self.jobname,
                                     mapper=self.mapper,
                                     scheduler=self.scheduler)
        self.workset_activating = self.dispatcher.workset_activating

        self.crawlinfodb = crawlinfo

        # self.inq = HashSplitIncomingQueue(
        #     qdir=hqconfig.inqdir(self.jobname),
        #     buffsize=500)
        self.inq = PooledIncomingQueue(
            qdir=hqconfig.inqdir(self.jobname),
            buffsize=1000)

        # currently disabled by default - too slow
        self.use_crawlinfo = False
        self.save_crawlinfo = False

        self.last_inq_count = 0

    PARAMS = [('use_crawlinfo', bool),
              ('save_crawlinfo', bool)]

    def shutdown(self):
        logging.info("shutting down scheduler")
        self.scheduler.shutdown()
        logging.info("closing incoming queues")
        self.inq.flush()
        self.inq.close()
        logging.info("shutting down dispatcher")
        self.dispatcher.shutdown()
        logging.info("shutting down crawlinfo")
        self.crawlinfodb.shutdown()
        logging.info("done.")

    def get_status(self):
        r = dict(job=self.jobname, oid=id(self))
        r['sch'] = self.scheduler and self.scheduler.get_status()
        r['inq'] = self.inq and self.inq.get_status()
        return r

    def get_workset_status(self):
        r = dict(job=self.jobname, crawljob=id(self))
        if self.scheduler:
            r['sch'] = id(self.scheduler)
            r['worksets'] = self.scheduler.get_workset_status()
        return r
        
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
        return self.dispatcher.processinq(maxn)

    def makecuri(self, o):
        if 'a' not in o:
            if 'w' in o:
                o['a'] = o.pop('w')
            else:
                a = dict(((k, o.pop(k)) for k in 'pxv' if k in o))
                if a: o['a'] = a
        return o

    def feed(self, client, n):
        logging.debug('feed "%s" begin', client)
        curis = self.scheduler.feed(client, n)
        # add recrawl info if enabled
        if self.use_crawlinfo and len(curis) > 0 and self.crawlinfodb:
            t0 = time.time()
            self.crawlinfodb.update_crawlinfo(curis)
            t = time.time() - t0
            if t / len(curis) > 0.5:
                logging.warn("SLOW update_crawlinfo: %s %.3fs/%d",
                             client, t, len(curis))
            self.crawlinfodb.mongo.end_request()
        r = [self.makecuri(u) for u in curis]
        return r

    def finished(self, curis):
        result = dict(processed=0)
        for curi in curis:
            self.scheduler.finished(curi)
            result['processed'] += 1
        if self.save_crawlinfo and self.crawlinfodb:
            for curi in curis:
                self.crawlinfodb.save_result(curi)
            # XXX - until I come up with better design
            self.crawlinfodb.mongo.end_request()
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
        self.crawlinfo = CrawlInfo('wide')
        mongoserver = hqconfig.get('mongo')
        logging.warn('using MongoDB: %s', mongoserver)
        self.mongo = pymongo.Connection(mongoserver)
        self.configdb = self.mongo.crawl
        self.domaininfo = DomainInfo(self.configdb)
        self.jobconfigs = JobConfigs(self.configdb)
        self.coordinator = Coordinator(hqconfig.get('zkhosts'))

    def shutdown(self):
        for job in self.jobs.values():
            job.shutdown()
        self.domaininfo.shutdown()
        self.configdb = None
        self.mongo.disconnect()

    def get_job(self, jobname, nocreate=False):
        with self.jobslock:
            job = self.jobs.get(jobname)
            if job is None:
                if nocreate and not self.jobconfigs.job_exists(jobname):
                    raise ValueError('unknown job %s' % jobname)
                job = self.jobs[jobname] = CrawlJob(
                    self.jobconfigs, jobname, self.crawlinfo, self.domaininfo)
                self.coordinator.publish_job(job)
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

    def reload_domaininfo(self):
        self.domaininfo.load()

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

class ClientAPI(QueryApp, DiscoveredHandler):

    def __init__(self):
        self.hq = hq

    # overriding QueryApp.{GET,POST} because argument order is different
    def GET(self, job, action):
        return self._dispatch('do_', action, job)
    def POST(self, job, action):
        return self._dispatch('post_', action, job)

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
        curis = json.loads(self.decode_content(payload))

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
        
    # def post_discovered(self, job):
    #     '''receives URLs found as 'outlinks' in just downloaded page.
    #     do_discovered runs already-seen test and then schedule a URL
    #     for crawling with last-modified and content-hash obtained
    #     from seen database (if previously crawled)'''
        
    #     p = web.input(force=0)
    #     if 'u' not in p:
    #         return {error:'u value missing'}

    #     furi = dict(u=p.u)
    #     for k in ('p', 'v', 'x'):
    #         if k in p and p[k] is not None: furi[k] = p[k]

    #     cj = hq.get_job(job)
    #     if p.force:
    #         return cj.schedule([furi])
    #     else:
    #         return cj.discovered([furi])

    # def post_mdiscovered(self, job):
    #     '''receives submission of "discovered" events in batch.
    #     this version simply queues data submitted in incoming queue
    #     to minimize response time. entries in the incoming queue
    #     will be processed by separate processinq call.'''
    #     result = dict(processed=0)
    #     data = None
    #     try:
    #         data = web.data()
    #         curis = json.loads(self.decode_content(data))
    #     except:
    #         web.debug("json.loads error:data=%s" % data)
    #         result['error'] = 'invalid data - json parse failed'
    #         return result
    #     if isinstance(curis, dict):
    #         force = curis.get('f')
    #         curis = curis['u']
    #     elif isinstance(curis, list):
    #         force = False
    #     else:
    #         result['error'] = 'invalid data - not an array'
    #         return result
    #     if len(curis) == 0:
    #         return result

    #     cj = hq.get_job(job)
    #     start = time.time()

    #     if force:
    #         result.update(cj.schedule(curis))
    #     else:
    #         result.update(cj.discovered(curis))

    #     t = time.time() - start
    #     result.update(t=t)
    #     if t / len(curis) > 1e-3:
    #         logging.warn("slow discovered: %.3fs for %d", t, len(curis))
    #     else:
    #         logging.debug("mdiscovered %s", result)
    #     return result
            
    def do_processinq(self, job):
        '''process incoming queue. max parameter advise upper limit on
        number of URIs processed. actually processed URIs may exceed that
        number if incoming queue is storing URIs in chunks'''
        p = web.input(max=5000)
        maxn = int(p.max)
        result = dict(job=job, inq=0, processed=0, scheduled=0, excluded=0,
                      max=maxn, td=0.0, ts=0.0)

        start = time.time()
        # transient instance for now
        try:
            result.update(hq.get_job(job).processinq(maxn))
        except Exception as ex:
            logging.exception('processinq failed')
            result.update(error=str(ex))
        result.update(job=job, t=(time.time() - start))

        return result

    def do_feed(self, job):
        p = web.input(n=5, name=None)
        # TODO: name will be a string
        name = web.intget(p.name, -1)
        if name < 0:
            return []
        count = max(web.intget(p.n, 0), 0)

        start = time.time()
        # return an JSON array of objects with properties:
        # uri, path, via, context and data
        r = hq.get_job(job).feed(name, count)
        t = time.time() - start
        if t > 1.0:
            logging.warn("slow feed %s:%s %s, %.4fs", job, name, len(r), t)
        else:
            logging.debug("feed %s %s:%s, %.4fs", job, name, len(r), t)

        return r

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

    # def do_flush(self, job):
    #     '''flushes cached objects into database for safe shutdown'''
    #     hq.get_job(job).flush()
    #     r = dict(ok=1)
    #     return r

    def do_seen(self, job):
        p = web.input()
        u = hq.get_job(job).seen.already_seen(dict(u=p.u))
        if u:
            u['id'] = u.pop('_id', None)
        result = dict(u=u)
        return result

    def do_status(self, job):
        try:
            r = hq.get_job(job, nocreate=1).get_status()
            return dict(success=1, r=r)
        except Exception as ex:
            logging.error('get_status failed', exc_info=1)
            return dict(success=0, err=str(ex))

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
        return "test\nweb.ctx.env="+str(web.ctx.env)+\
            "\nweb.ctx.path="+web.ctx.path

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
        o = dict(scheduler=j.scheduler, inq=j.inq, hq=hq, job=j).get(on)
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

    def do_threads(self, job):
        r = [str(th) for th in threading.enumerate()]
        return dict(success=1, r=r)

    def do_reloaddomaininfo(self, job):
        # job is irrelevant
        try:
            hq.reload_domaininfo()
            return dict(success=1)
        except Exception as ex:
            return dict(success=0, error=str(ex))
             
def setuplogging(level=logging.INFO, filename='hq.log'):
    logsdir = os.path.join(hqconfig.get('datadir'), 'logs')
    if not os.path.isdir(logsdir): os.makedirs(logsdir)
    logging.basicConfig(
        filename=os.path.join(logsdir, filename),
        level=level,
        format='%(asctime)s %(levelname)s %(name)s %(message)s',
        datefmt='%F %T')
    
urls = (
    '/(.*)/(.*)', 'ClientAPI',
    )
app = web.application(urls, globals())

if __name__ == "__main__":
    # FastCGI/stand-alone. Under FastCGI exception in app.run() does not
    # appear in server error log.
    try:
        setuplogging()
        logging.info('starting hq')
        app.run()
    except Exception as ex:
        logging.critical('app.run() terminated with error', exc_info=1)
else:
    # WSGI or testing
    setuplogging()
    application = app.wsgifunc()
