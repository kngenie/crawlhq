# file-based IncominqQueue

import sys, os
import re
from threading import Thread, RLock, Condition, Event
import time
import cjson
import mmap
from filequeue import FileEnqueue, FileDequeue
import logging
from executor import ThreadPoolExecutor

class IncomingQueue(object):
    # default maxsize 1GB - this would be too big for multi-queue
    # settings
    def __init__(self, qdir, noupdate=False,**kw):
        # ensure qdir directory exists
        self.qdir = qdir
        if not os.path.isdir(self.qdir):
            os.makedirs(self.qdir)

        self.addedcount = 0
        self.processedcount = 0

        self.init_queues(**kw)

    def init_queues(self, buffsize=0, maxsize=1000*1000*1000):
    
        # dequeue side
        self.rqfile = FileDequeue(self.qdir)
        # single queue file, no asynchronous writes
        self.qfiles = [FileEnqueue(self.qdir, buffer=buffsize,
                                   maxsize=maxsize)]

    @property
    def buffsize(self):
        self.qfiles[0].buffer_size
    @buffsize.setter
    def buffsize(self, v):
        for enq in self.qfiles:
            enq.buffer_size = v

    def __del__(self):
        self.close()

    def close(self, blocking=True):
        for q in self.qfiles:
            q.close(blocking=blocking)

    def flush(self):
        for q in self.qfiles:
            q._flush()

    def shutdown(self):
        self.rqfile.close()
        # _flush should be part of close, but not now.
        self.flush()
        self.write_executor.shutdown()
        self.close()

    def get_status(self):
        return dict(addedcount=self.addedcount,
                    processedcount=self.processedcount,
                    queuefilecount=self.rqfile.qfile_count(),
                    )

    def add(self, curis):
        result = dict(processed=0)
        for curi in curis:
            enq = self.qfiles[0]
            enq.queue(curi)

            self.addedcount += 1
            result['processed'] += 1
        return result

    def get(self, timeout=0.0):
        o = self.rqfile.get(timeout)
        # if queue exhausted, try closing current enq
        # leave busy queues
        if not o:
            self.close(blocking=False)
        if o: self.processedcount += 1
        return o

class SplitIncomingQueue(object):
    '''IncomingQueue variant that stores incoming URLs into
       multiple queue files, grouping by id range. This scheme
       has the same effect with merge sort and makes seen check
       much faster.'''
    def __init__(self, job, qdirbase, splitter):

        self.job = job
        self.splitter = splitter
        # ensure job directory exists
        self.qdir = os.path.join(qdirbase, job)
        if not os.path.isdir(self.qdir):
            os.makedirs(self.qdir)

        self.addedcount = 0
        self.processedcount = 0

        self.maxsize = 1000*1000*1000 # 1GB

        self.queue_writer = AsyncFlusher()
        self.enqs = [FileEnqueue(self.qdir, suffix=str(win))
                     for win in range(self.splitter.nqueues)]

        # dequeue side
        #self.lastqfile = None
        self.rqfile = FileDequeue(self.qdir)

        self.qfile_read = 0
        self.qfile_written = 0

    def __del__(self):
        self.close()

    def close(self):
        self.write_executor.shutdown()
        for enq in self.enqs:
            enq.close()

    def shutdown(self):
        self.close()

    def hash(self, curi):
        if 'id' in curi:
            return curi['id']
        else:
            h = Seen.urikey(curi['u'])
            curi['id'] = h
            return h

    def add(self, curis):
        result = dict(processed=0)
        for curi in curis:
            h = self.hash(curi)
            win = (h >> self.window_bits) & self.win_mask
            enq = self.enqs[win]
            enq.queue(curi)

            self.addedcount += 1
            result['processed'] += 1
        return result

    def get(self, timeout=0.0):
        o = self.rqfile.get(timeout)
        # TODO: if queue exhausted, try closing largest enq
        if o: self.processedcount += 1
        return o
