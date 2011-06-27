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
    def __init__(self, job, scheduler, qdirbase=QUEUE_DIRECTORY,
                 noupdate=False):
        self.job = job
        self.scheduler = scheduler
        # ensure job directory exists
        self.qdir = os.path.join(qdirbase, job)
        if not os.path.isdir(self.qdir):
            os.makedirs(self.qdir)

        self.addedcount = 0
        self.processedcount = 0

        self.maxage = 0.0 # no aging
        self.maxsize = 1000*1000*1000 # 1GB

        # should write into multiple files?
        self.qfile = FileEnqueue(self.qdir, maxage=self.maxage)
        self.qfilelock = RLock()

        # dequeue side
        self.lastqfile = None
        #self.rqfile = None
        self.rqfile = FileDequeue(self.qdir)
        #self.rqfiles = []
        #self.noupdate = noupdate

        self.qfile_read = 0
        self.qfile_written = 0

    def __del__(self):
        self.close()

    def monitor(self):
        while 1:
            self.opened.wait()
            self.opened.clear()
            with self.qfilelock:
                qfile = self.qfile
            # just in case qfile was closed before qfile gets it
            # (then we don't have to wait)
            if qfile:
                qfile.closed.wait(self.maxage)
                if qfile == self.qfile:
                    self.close()
            
    def close(self):
        with self.qfilelock:
            if self.qfile:
                qfile = self.qfile
                self.qfile = None
                qfile.close()

    def shutdown(self):
        self.close()

    def get_status(self):
        return dict(addedcount=self.addedcount,
                    processedcount=self.processedcount)

    def add(self, curis):
        result = dict(processed=0)
        with self.qfilelock:
            # for curi in curis:
            #     self.qfile.queue(curi)
            #     self.addedcount += 1
            #     result['processed'] += 1
            self.qfile.queue(curis)
            self.addedcount += len(curis)
            result['processed'] = len(curis)
        return result

    def get(self, timeout=0.0):
        o = self.rqfile.get(timeout)
        # if queue exhausted, try closing current enq
        if not o:
            self.qfile.close()
        if o: self.processedcount += 1
        return o

