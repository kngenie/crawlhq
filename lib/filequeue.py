import sys, os
import re
from threading import Thread, RLock, Condition, Event
import time
import cjson
import mmap

EMPTY_PAUSE = 15.0

class FileEnqueue(object):
    def __init__(self, qdir, maxage=30.0, suffix=None):
        self.qdir = qdir

        self.maxage = float(maxage)
        self.maxsize = 1000*1000*1000 # 1GB
        self.suffix = suffix

        self.closed = Event()
        self.opened = Event()
        self.lock = RLock()

        if self.maxage > 0:
            # XXX one monitor thread per FileEnqueue is too much
            # share monitor threads among multiple FileQueues
            self.rollover_thread = Thread(target=self.monitor)
            self.rollover_thread.daemon = True
            self.rollover_thread.start()

        self.file = None

    def open(self):
        '''open a new queue file and set it as current'''
        self.starttime = time.time()
        self.filename = str(int(self.starttime))
        if self.suffix: self.filename += ('_%s' % self.suffix)
        self.openfilename = self.filename + '.open'
        self.file = open(os.path.join(self.qdir, self.openfilename), 'w+')
        self.closed.clear()
        self.opened.set()

    def close(self):
        if self.file is None: return
        with self.lock:
            self.file.close()
            self.file = None
            self.closed.set()
            print >>sys.stderr, "renaming %s/%s to %s" % (self.qdir, self.openfilename, self.filename)
            os.rename(os.path.join(self.qdir, self.openfilename),
                      os.path.join(self.qdir, self.filename))
            self.filename = None
            self.openfilename = None
            # leave self.starttime - it is used by monitor()

    def queue(self, curis):
        if not isinstance(curis, list):
            curis = [curis]
        with self.lock:
            if self.file is None:
                self.open()
            for curi in curis:
                self.file.write(' ')
                self.file.write(cjson.encode(curi))
                self.file.write('\n')
            if self.size() > self.maxsize:
                self.close()

    def age(self):
        return time.time() - self.starttime

    def size(self):
        return self.file.tell()

    def monitor(self):
        while 1:
            self.opened.wait()
            self.opened.clear()
            ts = self.starttime
            self.closed.wait(self.maxage)
            # there's small chance of (though unlikely) race condition where
            # new queue file gets opened before self.closed.is_set().
            # self.starttime > ts detects this situation.
            with self.lock:
                #print >>sys.stderr, "closed=%s, starttime=%s, ts=%s" % (
                #    self.closed.is_set(), self.starttime, ts)
                if not self.closed.is_set():
                    if self.starttime <= ts:
                        self.close()

class QueueFileReader(object):
    '''reads (dequeues) from single queue file'''
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

class FileDequeue(object):
    '''multi-queue file reader'''
    def __init__(self, qdir, noupdate=False):
        self.qdir = qdir
        # timestamp of the last qfile
        self.rqfile = None
        self.rqfiles = []
        self.noupdate = noupdate

        self.qfile_read = 0

    def close(self):
        if self.rqfile:
            self.rqfile.close()
            self.rqfile = None

    def qfiles_available(self, qfiles):
        self.rqfiles.extend(qfiles)

    def scan(self):
        '''scan qdir for new file'''
        print >>sys.stderr, "scanning %s for new qfile" % self.qdir
        ls = os.listdir(self.qdir)
        curset = set(self.rqfiles)
        new_rqfiles = []
        for f in ls:
            if not ('1' <= f[0] <= '9'): continue
            if f.endswith('.open'): continue
            if f not in curset:
                new_rqfiles.append(f)
        if new_rqfiles:
            print >>sys.stderr, "found %d new queue file(s)" % len(new_rqfiles)
            new_rqfiles.sort()
            self.qfiles_available(new_rqfiles)
        else:
            print >>sys.stderr, "no new queue file was found"

    def next_rqfile(self, timeout=0.0):
        '''blocks until next qfile becomes available'''
        #print >>sys.stderr, "next_rqfile timeout=%.1f" % timeout
        remaining_timeout = timeout
        pause = 0.0
        while 1:
            if self.rqfiles:
                f = self.rqfiles.pop(0)
                qpath = os.path.join(self.qdir, f)
                print >>sys.stderr, "opening %s" % qpath
                try:
                    self.rqfile = QueueFileReader(
                        qpath,
                        noupdate = self.noupdate
                        )
                except mmap.error as ex:
                    if ex.errno == 22:
                        # empty file
                        os.remove(qpath)
                        continue
                    else:
                        raise
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