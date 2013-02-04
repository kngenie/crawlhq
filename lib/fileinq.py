# file-based IncominqQueue

import sys, os
import re
from threading import Thread, RLock, Condition, Event
import time
from filequeue import FileEnqueue, FileDequeue
import logging

class IncomingQueue(object):
    # default maxsize 1GB - this would be too big for multi-queue
    # settings
    def __init__(self, qdir, noupdate=False, norecover=False,
                 enq=FileEnqueue,
                 deq=FileDequeue,
                 **kw):
        self.deq_factory = deq
        self.enq_factory = enq

        # ensure qdir directory exists
        self.qdir = qdir
        if not os.path.isdir(self.qdir):
            os.makedirs(self.qdir)

        self.addedcount = 0
        self.processedcount = 0

        self.rqfile = None
        self.qfiles = None

        if not norecover:
            FileEnqueue.recover(self.qdir)
        self.init_queues(**kw)

    def init_queues(self, buffsize=0, maxsize=1000*1000*1000):
        # dequeue side
        self.deq = self.deq_factory and self.deq_factory(self.qdir)
        # single queue file, no asynchronous writes
        self.enq = self.enq_factory and self.enq_factory(
            self.qdir, buffer=buffsize, maxsize=maxsize)

    @property
    def buffsize(self):
        return self.enq and self.enq.buffer_size
    @buffsize.setter
    def buffsize(self, v):
        if self.enq:
            self.enq.buffer_size = v

    def __del__(self):
        self.shutdown()

    def close(self, blocking=True):
        if self.enq:
            self.enq.close(blocking=blocking)
            
    def flush(self):
        if self.enq:
            self.enq._flush()

    def shutdown(self):
        if self.deq: self.deq.close()
        # _flush should be part of close, but not now.
        self.flush()
        self.close()

    def get_status(self):
        #buffered = sum([enq.buffered_count for enq in self.qfiles])
        r = dict(addedcount=self.addedcount,
                 processedcount=self.processedcount,
                 queuefilecount=self.deq and self.deq.qfile_count(),
                 dequeue=self.deq and self.deq.get_status(),
                 bufferedcount=self.enq and dself.enq.buffered_count
                 )
        if self.deq:
            r['queuefilecount'] = self.deq.qfile_count()
        return r

    def add(self, curis):
        if not self.enq: raise NotImplementedError, 'enq side is not set up'
        result = dict(processed=0)
        for curi in curis:
            self.enq.queue(curi)

            self.addedcount += 1
            result['processed'] += 1
        return result

    def get(self, timeout=0.0):
        if not self.deq: raise NotImplementedError, 'deq side is not set up'
        o = self.deq.get(timeout)
        # if queue exhausted, try closing current enq
        # leave busy queues
        if not o:
            self.close(blocking=False)
        if o: self.processedcount += 1
        return o

class SplitEnqueue(object):
    """FileEnqueue compatible object that distribute incoming URLs
    into multiple FlieEnqueues based on id range. This scheme
    has the same effiect of (partial) merge sort and makes seen
    check much faster by take advantage of block cache.
    as all queue files go into qdir, FileDequeue can read from them.
    """
    def __init__(self, qdir, splitter):
        self.splitter = splitter
        self.queue_writer = AsyncFlusher()
        self.enqs = [FileEnqueue(self.qdir, suffix=str(win))
                     for win in range(self.splitter.nqueues)]
    def queue(self, curi):
        # shall return 0..self.splitter.nqueues
        win = self.splitter.hash(curi)
        enq = self.enqs[win]
        enq.queue(curi)

class SplitIncomingQueue(IncomingQueue):
    '''IncomingQueue variant that stores incoming URLs into
       multiple queue files, grouping by id range. This scheme
       has the same effect with merge sort and makes seen check
       much faster.'''
    def __init__(self, job, qdir, splitter):
        super(SplitIncomingQueue, self).__init__(
            qdir, enq=SplitEnqueue, splitter=splitter)

    # def hash(self, curi):
    #     if 'id' in curi:
    #         return curi['id']
    #     else:
    #         h = Seen.urikey(curi['u'])
    #         curi['id'] = h
    #         return h

    # def add(self, curis):
    #     result = dict(processed=0)
    #     for curi in curis:
    #         h = self.hash(curi)
    #         win = (h >> self.window_bits) & self.win_mask
    #         enq = self.enqs[win]
    #         enq.queue(curi)

    #         self.addedcount += 1
    #         result['processed'] += 1
    #     return result

    # def get(self, timeout=0.0):
    #     o = self.rqfile.get(timeout)
    #     # TODO: if queue exhausted, try closing largest enq
    #     if o: self.processedcount += 1
    #     return o
