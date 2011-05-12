# file-based IncominqQueue

import sys, os
import re
from threading import Thread, RLock, Condition, Event
import time
import cjson
import mmap

QUEUE_DIRECTORY = '/1/incoming/hq'
EMPTY_PAUSE = 15.0

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

class FileDequeue(object):
    def __init__(self, qfile, noupdate=False):
        self.qfile = qfile
        self.noupdate = noupdate
        self.open()
    def open(self):
        self.fd = os.open(self.qfile, os.O_RDWR)
        self.map = mmap.mmap(self.fd, 0, access=mmap.ACCESS_WRITE)
    def close(self):
        if self.map:
            self.map.close()
            self.map = None
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1
    def next(self):
        while 1:
            mark = self.map.read(1)
            if not mark:
                raise StopIteration
            if mark == ' ':
                markpos = self.map.tell() - 1
                l = self.map.readline()
                if not self.noupdate:
                    self.map[markpos] = '#'
                return cjson.decode(l)
            self.map.readline()

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

        # should write into multiple files?
        self.qfile = None
        self.qfilelock = RLock()

        self.maxage = 30.0
        self.maxsize = 1000*1000*1000 # 1GB

        self.opened = Event()

        self.rollover_thread = Thread(target=self.monitor)
        self.rollover_thread.daemon = True
        self.rollover_thread.start()

        # dequeue side
        self.lastqfile = None
        self.rqfile = None
        self.rqfiles = []
        self.noupdate = noupdate

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
            if self.qfile is None:
                self.qfile = FileEnqueue(self.qdir)
                self.qfile_written += 1
                self.opened.set()
            for curi in curis:
                self.qfile.queue(curi)
                self.addedcount += 1
                result['processed'] += 1
            if self.qfile.size() > self.maxsize:
                self.close()
        return result

    def scan(self):
        '''scan qdir for new file'''
        print >>sys.stderr, "scanning for new qfile"
        ls = os.listdir(self.qdir)
        mts = self.lastqfile
        new_rqfiles = []
        for f in ls:
            if not ('1' <= f[0] <= '9'): continue
            if f.endswith('.open'): continue
            try:
                ts = int(f)
                if self.lastqfile is None or self.lastqfile < ts:
                    print >>sys.stderr, "new qfile found: %s" % f
                    new_rqfiles.append(f)
                    if ts > mts: mts = ts
            except:
                pass
        if new_rqfiles:
            new_rqfiles.sort()
            self.rqfiles.extend(new_rqfiles)
        self.lastqfile = mts
        
    def next_rqfile(self, timeout=0.0):
        '''blocks until next qfile is available'''
        #print >>sys.stderr, "next_rqfile timeout=%.1f" % timeout
        remaining_timeout = timeout
        pause = 0.0
        while 1:
            if self.rqfiles:
                f = self.rqfiles.pop(0)
                print >>sys.stderr, "opening %s" % f
                self.rqfile = FileDequeue(
                    os.path.join(self.qdir, f),
                    noupdate = self.noupdate
                    )
                self.qfile_read += 1
                return self.rqfile
            else:
                if timeout > 0.0:
                    if remaining_timeout <= 0.0:
                        return None
                    if pause > remaining_timeout:
                        pause = remaining_timeout
                    remaining_timeout -= pause
                if pause > 0.0: time.sleep(pause)
                self.scan()
                pause = EMPTY_PAUSE
                continue
                
    def get(self, timeout=0.0):
        '''currently this method is not thread-safe'''
        while 1:
            if self.rqfile:
                try:
                    curi = self.rqfile.next()
                    return curi
                except StopIteration:
                    self.rqfile.close()
                    if not self.noupdate:
                        os.unlink(self.rqfile.qfile)
                    self.rqfile = None
                    if not self.next_rqfile(timeout):
                        return None
            else:
                if not self.next_rqfile(timeout):
                    return None

