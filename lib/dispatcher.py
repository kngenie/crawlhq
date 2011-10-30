# HQ dispatcher - seen check and scheduling to worksets
#

import os
import re
import threading
import logging
from urlparse import urlsplit
from Queue import Queue, Empty, Full
import time

import hqconfig
from seen import Seen
from filequeue import FileDequeue, FileEnqueue
import sortdequeue
from mongojobconfigs import JobConfigs
from mongodomaininfo import DomainInfo
from mongocrawlinfo import CrawlInfo
import urihash
from cfpgenerator import FPGenerator
from scheduler import Scheduler

import weblib

class WorksetMapper(object):
    """maps URLs to WorkSet"""
    def __init__(self, nworksets_bits):
        self.nworksets_bits = nworksets_bits
        self.nworksets = (1 << self.nworksets_bits)

        self._fp31 = FPGenerator(0xBA75BB4300000000, 31)

    def workset(self, curi):
        '''reutrns WorkSet id to which curi should be dispatched'''
        uc = urlsplit(curi['u'])
        h = uc.netloc
        p = h.find(':')
        if p > 0: h = h[:p]
        # Note: don't use % (mod) - FP hash much less even distribution in
        # lower bits.
        hosthash = int(self._fp31.fp(h) >> (64 - self.nworksets_bits))
        return hosthash

class WorksetWriter(object):
    """writing side of Workset."""
    def __init__(self, wsdir, wsid):
        self.wsid = wsid
        self.qdir = os.path.join(wsdir, str(self.wsid))
        FileEnqueue.recover(self.qdir)
        self.enq = FileEnqueue(self.qdir, buffer=500)

        self.scheduledcount = 0
        
    def flush(self):
        self.enq._flush()
        return self.enq.close()

    def shutdown(self):
        self.flush()

    def get_status(self):
        r = dict(id=self.wsid, running=True,
                 scheduled=self.scheduledcount)
        return r
    def schedule(self, curi):
        self.enq.queue(curi)
        self.scheduledcount += 1

def FPSortingQueueFileReader(qfile, **kwds):
    def urikey(o):
        return urihash.urikey(o['u'])
    return sortdequeue.SortingQueueFileReader(qfile, urikey)
    
class Scheduler(object):
    def __init__(self, job, mapper):
        self.jobname = job
        self.mapper = mapper
        wsdir = hqconfig.worksetdir(self.jobname)
        self.worksets = [WorksetWriter(wsdir, wsid)
                         for wsid in xrange(self.mapper.nworksets)]
    def flush(self):
        r = []
        for ws in self.worksets:
            if ws.flush(): r.append(ws.wsid)
        return r

    def shutdown(self):
        for ws in self.worksets:
            ws.shutdown()

    def get_status(self):
        r = dict(
            nworksets=self.mapper.nworksets
            )
        return r
    def schedule(self, curi):
        wsid = self.mapper.workset(curi)
        self.worksets[wsid].schedule(curi)

class CrawlJob(object):
    def __init__(self, jobconfigs, jobname, domaininfo):
        self.jobconfigs = jobconfigs
        self.jobname = jobname
        self.qdir = hqconfig.inqdir(self.jobname)

        self.inq = FileDequeue(self.qdir, reader=FPSortingQueueFileReader)

        self.mapper = WorksetMapper(hqconfig.NWORKSETS_BITS)
        self.seen = Seen(dbdir=hqconfig.seendir(self.jobname))
        self.domaininfo = domaininfo
        
        self.scheduler = Scheduler(self.jobname, self.mapper)

    def shutdown(self):
        logging.info("closing seen db")
        self.seen.close()
        logging.info("shutting down scheduler")
        self.scheduler.shutdown()
        logging.info("done.")

    def get_status(self):
        r = dict(job=self.jobname, oid=id(self))
        r['sch'] = self.scheduler and self.scheduler.get_status()
        r['inq'] = self.inq and self.inq.get_status()
        return r

    # def get_workset_status(self):
    #     r = dict(job=self.jobname, crawljob=id(self))
    #     if self.scheduler:
    #         r['sch'] = id(self.scheduler)
    #         r['worksets'] = self.scheduler.get_workset_status()
    #     return r
        
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
            #self.scheduler.schedule(curi)
            ws = self.mapper.workset(curi)
            self.worksets[ws].schedule(curi)
            scheduled += 1
        return dict(processed=scheduled, scheduled=scheduled)

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
            suri = self.seen.already_seen(furi)
            if suri['e'] < int(time.time()):
                if 'w' in furi:
                    w = furi['w']
                else:
                    w = dict()
                    for k in ('p','v','x'):
                        m = furi.get(k)
                        if m is not None:
                            w[k] = m
                curi = dict(u=furi['u'], id=suri['_id'], a=w)
                self.scheduler.schedule(curi)
                result['scheduled'] += 1
            result['ts'] += (time.time() - t0)
        # currently no access to MongoDB
        #self.mongo.end_request()
        return result

    def makecuri(self, o):
        return o

    def flush(self):
        self.seen.flush()
        return self.scheduler.flush()
    
class Dispatcher(object):
    def __init__(self):
        self.domaininfo = DomainInfo()
        self.jobconfigs = JobConfigs()

        self.jobs = {}
        self.jobslock = threading.RLock()

    def shutdown(self):
        for job in self.jobs.values():
            job.shutdown()
        self.jobconfigs.shutdown()
        self.domaininfo.shutdown()

    def get_job(self, jobname, nocreate=True):
        with self.jobslock:
            job = self.jobs.get(jobname)
            if job is None:
                if nocreate and not self.jobconfigs.job_exists(jobname):
                    raise ValueError('unknown job %s' % jobname)
                job = self.jobs[jobname] = CrawlJob(
                    self.jobconfigs, jobname, self.domaininfo)
                #self.coordinator.publish_job(job)
            return job

    def flush_job(self, jobname):
        """flushes URIs buffered in worksets"""
        return self.get_job(jobname, nocreate=True).flush()

    def processinq(self, job, maxn):
        t = time.time()
        r = self.get_job(job).processinq(maxn)
        r.update(job=job, t=(time.time() - t))
        return r

# TODO move this class to ws directory
class DispatcherAPI(weblib.QueryApp):
    def do_processinq(self, job):
        """process incoming queue. max parameter advise uppoer limit on
        number of URIs processed. actually processed URIs may exceed that
        number if incoming queue is storing URIs in chunks
        """
        p = web.input(max=500)
        maxn = int(p.max)
        result = dict(job=job, inq=0, processed=0, scheduled=0, max=maxn,
                      td=0.0, ts=0.0)
        start = time.time()

        result.update(self.get_job(job).processinq(maxn))
        
        result.update(t=(time.time() - start))
        return result
