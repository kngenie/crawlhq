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
    
import ctypes, ctypes.util
import select
import fcntl
import termios
import struct
import errno
import array

libc = ctypes.cdll.LoadLibrary(ctypes.util.find_library('libc'))

class InqueueWatcher(threading.Thread):
    def __init__(self):
        super(InqueueWatcher, self).__init__()
        self.daemon = True
        self.in_fd = libc.inotify_init()
        if self.in_fd < 0:
            logging.error('inotify_init failed')
        self.watches = {}

    def __del__(self):
        self.shutdown()
        super(InqueueWatcher, self).__del__()

    def shutdown(self):
        self.running = False

    def run(self):
        try:
            self._poll = select.poll()
            self._poll.register(self.in_fd, select.POLLIN)
            self.loop()
        except:
            logging.error('self.loop error', exc_info=1)
        finally:
            print >>sys.stderr, "self.loop exited"
            self._poll.unregister(self.in_fd)
            os.close(self.in_fd)
            for w in self.watches.values():
                with w[1]:
                    w[1].notify_all()

    def process_events(self):
        # structure of fixed length part of inotify_event struct
        fmt = 'iIII'
        ssize = struct.calcsize(fmt)
        # each event record must be read with one os.read() call.
        # so you cannot do "read the first 16 byte, then name_len
        # more bytes"
        #b = os.read(self.in_fd, struct.calcsize(fmt))
        #b = os.read(self.in_fd, 16)
        buf = array.array('i', [0])
        if fcntl.ioctl(self.in_fd, termios.FIONREAD, buf, 1) < 0:
            return
        rlen = buf[0]
        try:
            b = os.read(self.in_fd, rlen)
        except:
            logging.warn('read on in_fd failed', exc_info=1)
            return
        while b:
            wd, mask, cookie, name_len = struct.unpack(fmt, b[:ssize])
            name = b[ssize:ssize + name_len]
            b = b[ssize + name_len:]
            # notify Condition corresponding to wd
            if (mask & 0x80) == 0x80:
                logging.info('new queue file %s' % name)
                for w in self.watches.values():
                    if w[0] == wd:
                        with w[1]:
                            w[1].notify()

    def loop(self):
        self.running = True
        while self.running:
            # poll with timeout for checking self.running
            try:
                r = self._poll.poll(500)
            except select.error as ex:
                if ex[0] == errno.EINTR:
                    # nofity all conditions so that waiting clients
                    # wake up and (probably) exit
                    # (this is ineffective since only main thread
                    # receives INT signal)
                    for w in self.watches.values():
                        with w[1]:
                            w[1].notify_all()
                else:
                    logging.error('poll failed', exc_info=1)
                continue
            if r and r[0][1] & select.POLLIN:
                self.process_events()
    def addwatch(self, path):
        if path in self.watches:
            return self.watches[path][1]
        mask = 0x80 # IN_MOVED_TO
        wd = libc.inotify_add_watch(self.in_fd, path, mask)
        c = threading.Condition()
        self.watches[path] = [wd, c, False]
        return c

    def delwatch(self, path):
        e = self.watches[path]
        s = libc.inotity_rm_watch(self.in_fd, e[0])
        if s < 0:
            logging.warn('failed to remove watch on %s(wd=%d)', path, e[0])
        else:
            with e[1]:
                e[2] = True
                e[1].notify_all()
            del self.watches[path]
        
    def available(self, path, timeout=None):
        e = self.watches[path]
        with e[1]:
            e[2] = False
            e[1].wait(timeout)
            return e[2]

class Dispatcher(object):
    inqwatcher = None

    # TODO: absorb CrawlJob above.
    def __init__(self, jobconfigs, domaininfo, job):
        self.domaininfo = domaininfo
        self.jobconfigs = jobconfigs
        self.jobname = job
        if not self.jobconfigs.job_exists(self.jobname):
            raise ValueError('unknown job %s' % self.jobname)
        self.job = CrawlJob(self.jobconfigs, self.jobname, self.domaininfo)
        if Dispatcher.inqwatcher is None:
            iqw = Dispatcher.inqwatcher = InqueueWatcher()
            iqw.start()
        self.watch = iqw.addwatch(hqconfig.inqdir(self.jobname))

    def shutdown(self):
        if self.job: self.job.shutdown()

    def flush(self):
        """flushes URIs buffered in workset objects"""
        return self.job.flush()

    def processinq(self, maxn):
        t = time.time()
        r = self.job.processinq(maxn)
        r.update(job=self.jobname, t=(time.time() - t))
        return r

    def wait_available(self, timeout=None):
        with self.watch:
            self.__available = False
            self.watch.wait(timeout)
            return self.__available

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
