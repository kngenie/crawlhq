#!/usr/bin/python
#
# Headquarters server for crawling cloud control
#
# make sure to specify "lib" directory in python-path in WSGI config
#
import sys, os
import web
import json
import time
import re
import itertools
import threading
from Queue import Queue, Empty, Full, LifoQueue
import atexit
import logging

import hqconfig
from fileinq import IncomingQueue
from filequeue import FileEnqueue, FileDequeue, DummyFileDequeue
from pooledqueue import PooledEnqueue
from priorityqueue import PriorityEnqueue, PriorityDequeue
from scheduler import Scheduler
from executor import *
import urihash
from dispatcher import WorksetMapper, Dispatcher, FPSortingQueueFileReader
from weblib import QueryApp
import rcom
from handlers import *

class UnknownJobError(Exception):
    pass

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

    def clientforwsid(self, wsid):
        if wsid < 0 or wsid >= len(self.worksetclient):
            raise ValueError, 'wsid %d out range (0-%d)' % (
                wsid, len(self.worksetclient) - 1)
        return self.worksetclient[wsid]

    def client_activating(self, clid):
        """called back by Scheduler when client gets a new feed request
        when it is inactive."""
        for wsid in self.wsidforclient(clid):
            self.crawljob.workset_activating(wsid)

class CrawlJob(object):

    def __init__(self, hq, jobname):
        self.hq = hq
        self.jobconfigs = self.hq.jobconfigs
        self.jobname = jobname
        self.mapper = CrawlMapper(self, hqconfig.NWORKSETS_BITS)
        self.scheduler = Scheduler(hqconfig.worksetdir(self.jobname),
                                   self.mapper)

        try:
            readsorted = int(hqconfig.get(('inq', 'sort'), 1))
        except:
            readsorted = 1
        
        self.eninq = PriorityEnqueue(
            qdir=hqconfig.inqdir(self.jobname),
            buffer=1000)
        
        deinqargs = {}
        if readsorted:
            deinqargs['reader'] = FPSortingQueueFileReader
        self.deinq = PriorityDequeue(qdir=hqconfig.inqdir(self.jobname),
                                     **deinqargs)

        self._dispatcher_mode = hqconfig.get(
            ('jobs', self.jobname, 'dispatcher'), 'internal')
                                            
        self.dispatcher = None
        #self.init_dispatcher()

        # currently disabled by default - too slow
        self.use_crawlinfo = False
        self.save_crawlinfo = False

        self.last_inq_count = 0

        self.addedcount = 0
        self.processedcount = 0

    PARAMS = [('use_crawlinfo', bool),
              ('save_crawlinfo', bool),
              ('dispatcher_mode', str)]

    @property
    def dispatcher_mode(self):
        return self._dispatcher_mode
    @dispatcher_mode.setter
    def dispatcher_mode(self, value):
        self._dispatcher_mode = value
        if value == 'external':
            self.shutdown_dispatcher()

    def init_dispatcher(self):
        if self.dispatcher: return
        if self.dispatcher_mode == 'external':
            raise RuntimeError, 'dispatcher mode is %s' % self.dispatcher_mode
        self.dispatcher = Dispatcher(self.hq.get_domaininfo(),
                                     self.jobname,
                                     mapper=self.mapper,
                                     scheduler=self.scheduler,
                                     inq=self.deinq)
        
    def shutdown_dispatcher(self):
        if not self.dispatcher: return
        logging.info("shutting down dispatcher")
        self.dispatcher.shutdown()
        self.dispatcher = None

    def shutdown(self):
        logging.info("shutting down scheduler")
        self.scheduler.shutdown()
        logging.info("closing incoming queues")
        self.eninq._flush()
        self.eninq.close()
        self.shutdown_dispatcher()
        logging.info("done.")

    def get_status(self):
        r = dict(job=self.jobname, oid=id(self))
        r['sch'] = self.scheduler and self.scheduler.get_status()
        r['inq'] = dict(
            addedcount=self.addedcount,
            processedcount=self.processedcount,
            queuefilecount=self.deinq.qfile_count(),
            dequeue=self.deinq and self.deinq.get_status(),
            enqueue=self.eninq and self.eninq.get_status()
            )
        return r

    def get_workset_status(self):
        r = dict(job=self.jobname, crawljob=id(self))
        if self.scheduler:
            r['sch'] = id(self.scheduler)
            r['worksets'] = self.scheduler.get_workset_status()
        return r
        
    def workset_activating(self, *args):
        self.init_dispatcher()
        self.dispatcher.workset_activating(*args)

    def schedule(self, curis):
        '''schedule curis bypassing seen-check. typically used for starting
           new crawl cycle.'''
        scheduled = 0
        for curi in curis:
            self.scheduler.schedule(curi)
            scheduled += 1
        return dict(processed=scheduled, scheduled=scheduled)

    def discovered(self, curis):
        processed = 0
        self.eninq.queue(curis)
        self.addedcount += len(curis)
        processed += len(curis)
        return dict(processed=processed)

    def processinq(self, maxn):
        self.init_dispatcher()
        return self.dispatcher.processinq(maxn)

    def makecuri(self, o):
        # temporary rescue measure. delete after everything's got fixed.
        a = o.get('a')
        if isinstance(a, dict):
            for k in 'pvx':
                m = a.pop(k, None)
                if m is not None: o[k] = m
            if not o['a']:
                del o['a']
        return o

    def feed(self, client, n):
        logging.debug('feed "%s" begin', client)
        curis = self.scheduler.feed(client, n)
        # add recrawl info if enabled
        if self.use_crawlinfo and len(curis) > 0 and self.hq.crawlinfo:
            t0 = time.time()
            self.hq.crawlinfo.update_crawlinfo(curis)
            t = time.time() - t0
            if t / len(curis) > 1.0:
                logging.warn("SLOW update_crawlinfo: %s %.3fs/%d",
                             client, t, len(curis))
            self.hq.crawlinfo.mongo.end_request()
        r = [self.makecuri(u) for u in curis]
        # if client queue is empty, request incoming queue to flush
        if not r:
            # but do not flush too frequently.
            if self.addedcount > self.last_inq_count + 1000:
                self.eninq.close()
                self.last_inq_count = self.addedcount
        return r

    def finished(self, curis):
        result = dict(processed=0)
        for curi in curis:
            self.scheduler.finished(curi)
            result['processed'] += 1
        if self.save_crawlinfo and self.hq.crawlinfo:
            for curi in curis:
                self.hq.crawlinfo.save_result(curi)
            # XXX - until I come up with better design
            self.hq.crawlinfo.mongo.end_request()
        return result

    def reset(self, client):
        return self.scheduler.reset(client)

    def flush(self):
        self.eninq.close()
        return self.scheduler.flush_clients()

    def count_seen(self):
        """return number of items in seen db.
        can take pretty long to return.
        """
        self.init_dispatcher()
        if self.dispatcher.seen:
            return self.dispatcher.seen._count()
        else:
            # TODO:
            raise ValueError, 'seen db is not opened'

    def clear_seen(self):
        self.init_dispatcher()
        self.dispatcher.clear_seen()

class Headquarters(object):
    '''now just a collection of CrawlJobs'''
    def __init__(self):
        self.jobs = {}
        self.jobslock = threading.RLock()
        # single shared CrawlInfo database
        self.crawlinfo = None # disabled for performance reasons
        # lazy initialization (FIXME: there must be better abstraction)
        self.domaininfo = None
        self.jobconfigs = hqconfig.factory.jobconfigs()
        self.coordinator = hqconfig.factory.coordinator()

    def shutdown(self):
        for job in self.jobs.values():
            logging.info("shutting down job %s", job)
            job.shutdown()
        if self.domaininfo:
            logging.info("shutting down domaininfo")
            self.domaininfo.shutdown()
        if self.crawlinfo:
            logging.info("shutting down crawlinfo")
            self.crawlinfo.shutdown()

    def get_domaininfo(self):
        if self.domaininfo is None:
            self.domaininfo = hqconfig.factory.domaininfo()
        return self.domaininfo

    def get_job(self, jobname, nocreate=False):
        with self.jobslock:
            job = self.jobs.get(jobname)
            if job is None:
                if nocreate and not self.jobconfigs.job_exists(jobname):
                    raise UnknownJobError('unknown job %s' % jobname)
                job = self.jobs[jobname] = CrawlJob(self, jobname)
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
        if self.domaininfo:
            self.domaininfo.load()

#executor = ThreadPoolExecutor(poolsize=4)
hq = Headquarters()
#atexit.register(executor.shutdown)
atexit.register(hq.shutdown)

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
        except UnknownJobError as ex:
            result.update(error=str(ex))
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
        if t > 2.0 and len(r) > 0:
            logging.warn("slow feed %s:%s %s, %.4fs", job, name, len(r), t)
        else:
            logging.debug("feed %s %s:%s, %.4fs", job, name, len(r), t)

        return r

    def do_reset(self, job):
        '''resets URIs' check-out state, make them schedulable to crawler'''
        p = web.input(name=None)
        name = int(p.name)
        r = dict(name=name)
        logging.info("received reset request: %s", str(r))
        if name is None:
            r.update(msg='name is required')
            return r
        r.update(hq.get_job(job).reset(name))
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
        except UnknownJobError as ex:
            return dict(success=0, err=str(ex))
        except Exception as ex:
            logging.error('get_status failed', exc_info=1)
            #return dict(success=0, err=str(ex))
            return dict(success=0, err=traceback.format_exc(limit=2))

    def do_worksetstatus(self, job):
        r = hq.get_job(job).get_workset_status()
        return r

    def do_seencount(self, job):
        '''can take pretty long time'''
        try:
            count = hq.get_job(job, nocreate=1).count_seen()
            return dict(success=1, seencount=count)
        except Exception as ex:
            return dict(success=0, err=str(ex))

    def do_clearseen(self, job):
        try:
            hq.get_job(job, nocreate=1).clear_seen()
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
        m = rcom.Model(dict(
                scheduler=j.scheduler, inq=j.inq, hq=hq, job=j))
        # XXX we cannot set None to an attribute
        if p.value is None:
            try:
                return dict(success=1, name=name, v=m.get_property(p.name))
            except Exception, ex:
                return dict(success=0, name=name, error=str(ex))
        else:
            try:
                nv, ov = m.set_property(p.name, p.value)
                return dict(success=1, name=name, value=p.value,
                            v=ov, nv=nv)
            except Exception, ex:
                return dict(success=0, name=name, value=p.value,
                            error=str(ex))
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

    def do_which(self, job):
        p = web.input(u=None)
        u = p.u
        if not u:
            return dict(success=0, u=u)
        if not re.match(r'https?://', u):
            u = 'http://' + u
        job = hq.get_job(job)
        ws = job.mapper.workset(dict(u=u))
        clid = job.mapper.clientforwsid(ws)

        return dict(success=1, u=u, ws=ws, client=clid)
        
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
