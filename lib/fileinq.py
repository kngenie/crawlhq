# file-based IncominqQueue

import sys, os
import re
from threading import Thread, RLock, Condition, Event
import time
import cjson

QUEUE_DIRECTORY = '/1/incoming/hq'

class FileEnqueue(object):
    def __init__(self, qdir):
        self.qdir = qdir
        self.open()
        self.closed = Event()

    def open(self):
        self.starttime = time.time()
        self.filename = str(int(self.starttime))
        self.openfilename = self.filename + '.open'
        self.file = open(os.path.join(self.qdir, self.openfilename), 'w+')

    def close(self):
        self.closed.set()
        self.file.close()
        print >>sys.stderr, "renaming %s to %s" % (self.openfilename, self.filename)
        os.rename(os.path.join(self.qdir, self.openfilename),
                  os.path.join(self.qdir, self.filename))

    def queue(self, curi):
        self.file.write(' ')
        self.file.write(cjson.encode(curi))
        self.file.write('\n')

    def age(self):
        return time.time() - self.starttime

    def size(self):
        return self.file.tell()

class IncomingQueue(object):
    def __init__(self, job, scheduler, qdirbase=QUEUE_DIRECTORY):
        self.job = job
        self.scheduler = scheduler
        # ensure job directory exists
        self.qdir = os.path.join(qdirbase, job)
        if not os.path.isdir(self.qdir):
            os.makedirs(self.qdir)

        self.addcount = 0
        self.processedcount = 0

        # should write into multiple files?
        self.qfile = None
        self.qfilelock = RLock()

        self.maxage = 30.0
        self.maxsize = 1000*1000*1000 # 1GB

        self.opened = Event()

        self.rollover_thread = Thread(target=self.monitor)
        self.rollover_thread.daemon = True
        self.rollover_thread.start()

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

    def get_status(self):
        return dict(addcount=self.addcount,
                    processedcount=self.processedcount)

    def add(self, curis):
        result = dict(processed=0)
        with self.qfilelock:
            if self.qfile is None:
                self.qfile = FileEnqueue(self.qdir)
                self.opened.set()
            for curi in curis:
                self.qfile.queue(curi)
                self.addcount += 1
            if self.qfile.size() > self.maxsize:
                self.close()

