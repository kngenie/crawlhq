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
from scheduler import Scheduler, WorkSet
from diverter import Diverter

import tasprefix
import weblib

class WorksetMapper(object):
    """maps URLs to WorkSet"""
    def __init__(self, nworksets_bits):
        self.nworksets_bits = nworksets_bits
        self.nworksets = (1 << self.nworksets_bits)

        self._fp31 = FPGenerator(0xBA75BB4300000000, 31)

    def hosthash(self, curi):
        return int(self._fp31.fp(tasprefix.prefix(curi)))

    def workset(self, curi):
        '''reutrns WorkSet id to which curi should be dispatched'''
        hosthash = self.hosthash(curi['u'])
        # Note: don't use % (mod) - FP hash is much less evenly distributed
        # in lower bits.
        ws = hosthash >> (64 - self.nworksets_bits)
        return ws
    
import ctypes, ctypes.util
import select
import fcntl
import termios
import struct
import errno
import array

libc = ctypes.cdll.LoadLibrary(ctypes.util.find_library('libc'))

class Watch(object):
    def __init__(self, wd):
        self.wd = wd
        self.c = threading.Condition()
        self.available = False
    def wait(self, timeout=None):
        with self.c:
            self.available = False
            self.c.wait(timeout)
            return self.available

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
                with w.c:
                    w.c.notify_all()

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
                    if w.wd == wd:
                        with w.c:
                            w.available = True
                            w.c.notify()

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
                        with w.c:
                            w.c.notify_all()
                else:
                    logging.error('poll failed', exc_info=1)
                continue
            if r and r[0][1] & select.POLLIN:
                self.process_events()
    def addwatch(self, path):
        if path in self.watches:
            return self.watches[path]
        mask = 0x80 # IN_MOVED_TO
        wd = libc.inotify_add_watch(self.in_fd, path, mask)
        watch = self.watches[path] = Watch(wd)
        return watch

    def delwatch(self, path):
        e = self.watches[path]
        s = libc.inotity_rm_watch(self.in_fd, e.wd)
        if s < 0:
            logging.warn('failed to remove watch on %s(wd=%d)', path, e.wd)
        else:
            with e.c:
                e.available = True
                e.c.notify_all()
            del self.watches[path]
        
    def available(self, path, timeout=None):
        e = self.watches[path]
        return e.wait(timeout)

def FPSortingQueueFileReader(qfile, **kwds):
    def urikey(o):
        return urihash.urikey(o['u'])
    return sortdequeue.SortingQueueFileReader(qfile, urikey)
    
class ExcludedList(object):
    """URL list for storing excluded URLs. As URLs are checked for exclusion
    before seen check, there are (a lot of) duplicates.
    read-out is not supported because current HQ makes no use of these URLs.
    """
    # TODO: duplicated code with DivertQueue
    def __init__(self, jobname, bufsize=20):
        self.qdir = os.path.join(hqconfig.get('datadir'), jobname, 'ex')
        if not os.path.isdir(self.qdir):
            os.makedirs(self.qdir)
        FileEnqueue.recover(self.qdir)
        self.enq = FileEnqueue(self.qdir, buffer=bufsize, suffix='ex')
        self.queuedcount = 0

    def flush(self):
        self.enq._flush()
        return self.enq.close()

    def shutdown(self):
        self.flush()

    def get_status(self):
        r = dict(queued=self.queuedcount)
        return r

    def add(self, furi):
        self.enq.queue(furi)
        self.queuedcount += 1

class Dispatcher(object):
    inqwatcher = None

    # TODO: take JobConfig, instead of job
    def __init__(self, domaininfo, job, mapper,
                 scheduler):
        self.domaininfo = domaininfo
        self.jobname = job
        self.mapper = mapper
        self.scheduler = scheduler

        # TODO: inject these objects from outside
        qdir = hqconfig.inqdir(self.jobname)
        self.inq = FileDequeue(qdir, reader=FPSortingQueueFileReader)
        self.seen = Seen(dbdir=hqconfig.seendir(self.jobname))
        self.diverter = Diverter(self.jobname, self.mapper)
        self.excludedlist = ExcludedList(self.jobname)

        self.workset_state = [0 for i in range(self.mapper.nworksets)]

        # TODO: this could be combined with FileDequeue above in a single class
        if Dispatcher.inqwatcher is None:
            iqw = Dispatcher.inqwatcher = InqueueWatcher()
            iqw.start()
        self.watch = Dispatcher.inqwatcher.addwatch(hqconfig.inqdir(self.jobname))

    def shutdown(self):
        #if self.job: self.job.shutdown()
        logging.info("closing seen db")
        self.seen.close()
        # logging.info("shutting down scheduler")
        # self.scheduler.shutdown()
        logging.info("shutting down diverter")
        self.diverter.shutdown()
        logging.info("shutting down excludedlist")
        self.excludedlist.shutdown()
        logging.info("done.")

    def flush(self):
        """flushes URIs buffered in workset objects"""
        #return self.job.flush()
        
    def is_client_active(self, clid):
        """is client clid active?"""
        # TODO: update ZooKeeper when active status changes
        #t = self.client_last_active.get(str(clid))
        return self.scheduler.is_active(clid)

    def is_workset_active(self, wsid):
        """is workset wsid assigned to any active client?"""
        clid = self.mapper.worksetclient[wsid]
        return self.is_client_active(clid)

    def workset_activating(self, wsid):
        """activates working set wsid; start sending CURIs to Scheduler
        and enqueue diverted CURIs back into incoming queue so that
        processinq will process them (again). called by Scheduler,
        through CrawlMapper, when client starts feeding.
        note, unlike workset_deactivating, this method shall not be
        called from inside processinq method below, because processinq
        executes it only when at least one CURI is available for processing.
        if inq is empty, CURIs in divert queues would never be enqueued back.
        """
        # this could be executed asynchronously
        logging.info('workset %s activated', wsid)
        self.workset_state[wsid] = 1
        # is it better to move files back into inq directory?
        qfiles = self.diverter.listqfiles(wsid)
        logging.info('re-scheduling %s to inq', str(qfiles))
        self.inq.qfiles_available(qfiles)

    def workset_deactivating(self, wsid):
        """deactivates working set wsid; start sending CURIs into
        divert queues."""
        logging.info('workset %s deactivated', wsid)
        self.workset_state[wsid] = 0
        # flush Workset queues. we don't move qfiles to diverter yet.
        # it will be done when other HQ server becomes active on the
        # workset, and this HQ server starts forwarding CURIs.
        self.scheduler.flush_workset(wsid)

    def processinq(self, maxn):
        '''process incoming queue. maxn paramter adivces
        upper limit on number of URIs processed in this single call.
        actual number of URIs processed may exceed it if incoming queue
        stores URIs in chunks.'''

        # lazy initialization of seen db
        if not self.seen:
            try:
                cachesize = hqconfig.get('seencache')
                if cachesize: cachesize = int(cachesize)*(1024**2)
            except:
                cachesize = None
            self.seen = Seen(dbdir=hqconfig.seendir(self.jobname),
                             block_cache_size=cachesize)

        result = dict(processed=0, scheduled=0, excluded=0, saved=0,
                      td=0.0, ts=0.0)
        for count in xrange(maxn):
            t0 = time.time()
            furi = self.inq.get(0.01)
            
            result['td'] += (time.time() - t0)
            if furi is None: break
            result['processed'] += 1
            ws = self.mapper.workset(furi)
            if self.is_workset_active(ws):
                # no need to call self.workset_activating(). it's already
                # done by Scheduler
                di = self.domaininfo.get_byurl(furi['u'])
                if di and di['exclude']:
                    self.excludedlist.add(furi)
                    result['excluded'] += 1
                    continue
                t0 = time.time()
                suri = self.seen.already_seen(furi)
                if suri['e'] < int(time.time()):
                    if 'w' in furi:
                        a = furi['w']
                    else:
                        a = dict()
                        for k in ('p','v','x'):
                            m = furi.get(k)
                            if m is not None:
                                a[k] = m
                    curi = dict(u=furi['u'], id=suri['_id'], a=a)
                    self.scheduler.schedule(curi)
                    result['scheduled'] += 1
                result['ts'] += (time.time() - t0)
            else:
                if self.workset_state[ws]:
                    self.workset_deactivating(ws)
                # client is not active
                self.diverter.divert(str(ws), furi)
                result['saved'] += 1
        return result

    def wait_available(self, timeout=None):
        return self.watch.wait(timeout)

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
