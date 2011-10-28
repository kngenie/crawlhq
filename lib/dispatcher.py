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
import leveldb
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
    
class Seen(object):
    #_fp64 = FPGenerator(0xD74307D3FD3382DB, 64)
    EXPIRE_NEVER = (1<<32)-1

    def __init__(self, dbdir, block_cache_size=None):
        self.dbdir = dbdir
        self.block_cache_size = (block_cache_size or
            self.default_block_cache_size())
        self.ready = threading.Event()
        self._open()
        self.ready.set()
        self.putqueue = Queue(1000)
        self.drainlock = threading.RLock()

    def default_block_cache_size(self):
        try:
            f = open('/proc/meminfo')
            for l in f:
                if l.startswith('MemTotal:'):
                    mem = int(re.split('\s+', l)[1]) # in kB
                    # use 50% of total memory
                    return mem * 1000 / 2
        except:
            pass
        # conservative(!) 4GB, assuming 8GB machine.
        return 4*(1024**3)

    def _open(self):
        logging.info("opening seen-db %s", self.dbdir)
        self.seendb = leveldb.IntHash(self.dbdir,
                                      block_cache_size=self.block_cache_size,
                                      block_size=4096,
                                      max_open_files=256,
                                      write_buffer_size=128*(1024**2))
        logging.info("seen-db %s is ready", self.dbdir)

    def flush(self):
        logging.info("flushing putqueue (%d)", self.putqueue.qsize())
        self.drain_putqueue()

    def close(self):
        self.flush()
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

    # @staticmethod
    # def urikey(uri):
    #     uhash = Seen._fp64.sfp(uri)
    #     return uhash

    @staticmethod
    def keyquery(key):
        return {'_id': key}

    @staticmethod
    def uriquery(uri):
        return Seen.keyquery(urihash.urikey(uri))

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

    def already_seen(self, furi):
        self.ready.wait()
        key = furi.get('id') or urihash.urikey(furi['u'])
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
