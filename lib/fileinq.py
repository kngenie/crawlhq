# file-based IncominqQueue

import sys, os
import re
from threading import Thread, RLock, Condition, Event
import time
import cjson
import mmap
from filequeue import FileEnqueue, FileDequeue

QUEUE_DIRECTORY = '/1/incoming/hq'

class IncomingQueue(object):
    # default maxsize 1GB - this would be too big for multi-queue
    # settings
    def __init__(self, job, qdirbase=QUEUE_DIRECTORY,
                 noupdate=False, opener=None, buffsize=0,
                 maxsize=1000*1000*1000):
        self.job = job
        # ensure job directory exists
        self.qdir = os.path.join(qdirbase, job)
        if not os.path.isdir(self.qdir):
            os.makedirs(self.qdir)

        self.addedcount = 0
        self.processedcount = 0

        self.maxage = 0.0 # no aging
        self.maxsize = maxsize

        # multiple queue files
        # TODO: current code does not take advantage of having multiple
        # files to write into. it would take asynchronous writing.
        self.qfiles = [FileEnqueue(self.qdir, maxage=self.maxage,
                                   suffix=str(i), opener=opener,
                                   buffer=buffsize)
                       for i in range(self.num_queues())]

        # dequeue side
        self.rqfile = FileDequeue(self.qdir)

    @property
    def buffsize(self):
        self.qfiles[0].buffer_size
    @buffsize.setter
    def set_buffsize(self, v):
        for enq in self.qfils:
            enq.buffer_size = v

    def __del__(self):
        self.close()

    def close(self):
        # with self.qfilelock:
        #     if self.qfile:
        #         qfile = self.qfile
        #         self.qfile = None
        #         qfile.close()
        for q in self.qfiles:
            q.close()

    def shutdown(self):
        self.rqfile.close()
        self.close()

    def get_status(self):
        return dict(addedcount=self.addedcount,
                    processedcount=self.processedcount)

    # override these two methods when writing into multiple queues
    def num_queues(self):
        return 1
    def queue_dispatch(self, curi):
        return 0

    def add(self, curis):
        result = dict(processed=0)
        for curi in curis:
            win = self.queue_dispatch(curi)
            enq = self.qfiles[win]
            enq.queue(curi)

            self.addedcount += 1
            result['processed'] += 1
        return result

    def get(self, timeout=0.0):
        o = self.rqfile.get(timeout)
        # if queue exhausted, try closing current enq
        # TODO: re-implement this for possibly multi-queue situation
        # if not o:
        #     self.qfile.close()
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

        self.maxage = 0.0 # no aging
        self.maxsize = 1000*1000*1000 # 1GB

        self.enqs = [FileEnqueue(self.qdir, maxage=self.maxage, suffix=str(win))
                     for win in range(self.splitter.nqueues)]

        # dequeue side
        #self.lastqfile = None
        self.rqfile = FileDequeue(self.qdir)

        self.qfile_read = 0
        self.qfile_written = 0

    def __del__(self):
        self.close()

    def close(self):
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
