import sys, os
import re
from threading import Thread, RLock, Condition, Event
import time
import json
import mmap
import logging
from cStringIO import StringIO
from gzip import GzipFile
from collections import deque

EMPTY_PAUSE = 15.0

from executor import *

class timelog(object):
    def __init__(self, msg, warn=1.0):
        self.msg = msg
        self.warn = warn
    def __enter__(self):
        self.t0 = time.time()
    def __exit__(self, t, v, tb):
        t = time.time() - self.t0
        if t > self.warn:
            logging.warn('SLOW %s %.4fs', self.msg, t)
        else:
            logging.debug('%s %.4fs', self.msg, t)

class FileEnqueue(object):
    def __init__(self, qdir, suffix=None,
                 maxsize=1000*1000*1000, # 1GB
                 opener=None,
                 buffer=0, executor=None,
                 gzip=9):
        self.qdir = qdir
        self.opener = opener

        self.maxsize = maxsize
        self.suffix = suffix

        self.closed = Event()
        self.opened = Event()
        self.lock = RLock()

        self.buffer_size = buffer
        if self.buffer_size > 0:
            self.buffer = []
            self.bufflock = RLock()

        self.file = None
        self.filename = None
        self.__queuecount = 0

        self.executor = executor
        self.gzip = gzip
        self.__size = None

        self.starttime = None

    WRITE_BUFSIZE = 50000

    def get_status(self):
        r = dict(buffered=self.buffered_count,
                 pending=self.pending_count,
                 qfile=self.filename)
        return r

    @property
    def pending_count(self):
        """number of items written to current qfile, but not 'shipped' yet."""
        return self.__queuecount
    # for backward compatibility
    queue_count = pending_count

    @property
    def buffered_count(self):
        if self.buffer_size == 0:
            return 0
        else:
            return len(self.buffer)
            
    @staticmethod
    def recover(qdir):
        """fixes qfiles left open in qdir. this method is static
        as it should not be run automatically in __init__ if there
        are multiple FileEnqueue's for a directory."""
        logging.debug('recovering %s', qdir)
        try:
            ls = os.listdir(qdir)
        except Exception as ex:
            # no directory is fine
            if isinstance(ex, OSError) and ex.errno == 2:
                return
            logging.warn('listdir failed on %s', qdir, exc_info=1)
            return
        recovered = []
        for f in ls:
            if f.endswith('.open'):
                try:
                    os.rename(os.path.join(qdir, f),
                              os.path.join(qdir, f[:-5]))
                    recovered.append(f)
                except:
                    logging.warn('rename %s to %s failed', f, f[:-5],
                                 exc_info=1)
        logging.debug('recovering %s done (%d files fixed)',
                      qdir, len(recovered))

    def _open(self, fn):
        if self.opener: self.opener.opening(self)
        qf = os.path.join(self.qdir, fn)
        try:
            self.file = open(qf, 'a+', self.WRITE_BUFSIZE)
        except IOError as ex:
            if ex.errno == 2:
                os.makedirs(self.qdir)
                self.file = open(qf, 'a+', self.WRITE_BUFSIZE)
            else:
                raise
        #if self.opener: self.opener.opened(self)

    def _close(self):
        #if self.buffer_size > 0:
        #    self._flush()
        self.file.close()
        self.file = None
        if self.opener: self.opener.closed(self)

    def open(self):
        '''open a new queue file and set it as current'''
        if self.file: return
        if not self.filename:
            self.starttime = time.time()
            self.filename = str(int(self.starttime))
            if self.suffix: self.filename += ('_%s' % self.suffix)
            if self.gzip > 0: self.filename += ".gz"
            self.openfilename = self.filename + '.open'
            self._open(self.openfilename)
            self.closed.clear()
            self.opened.set()
            self.__size = 0
        else:
            # simply re-open the .open file
            self._open(self.openfilename)

    def rollover(self):
        self.close(rollover=True, blocking=True)

    def detach(self):
        '''try closing self.file so that others can use it.
           if file is busy or not open, return False,
           otherwise close and return True.'''
        return self.close(rollover=False, blocking=False)

    def close(self, rollover=True, blocking=True):
        '''set rollover to False to keep queue file .open'''
        if self.file is None and self.filename is None:
            return False
        logging.debug('%s close:acquiring lock blocking=%s', id(self), blocking)
        if self.lock.acquire(blocking):
            try:
                logging.debug('%s close:acquired file=%s', id(self), self.file)
                if self.file: self._close()
                if rollover:
                    self.closed.set()
                    logging.debug("renaming %s/%s to %s", self.qdir,
                                  self.openfilename, self.filename)
                    try:
                        os.rename(os.path.join(self.qdir, self.openfilename),
                                  os.path.join(self.qdir, self.filename))
                    except OSError as ex:
                        if ex.errno == 2:
                            logging.warn('failed to rename %s/%s to %s',
                                         self.qdir, self.openfilename,
                                         self.filename)
                        else:
                            raise
                    finally:
                        self.filename = None
                        self.openfilename = None
                        self.__size = None
                        self.__queuecount = 0
                return True
            finally:
                self.lock.release()
                logging.debug('%s close:released lock', id(self))
        else:
            return False

    def _writeout(self, data):
        logging.debug('%s _writerout before lock', id(self))
        with self.lock:
            logging.debug('%s _writeout inside lock', id(self))
            if self.file is None:
                self.open()
            b = StringIO()
            for s in data:
                b.write(' ')
                b.write(json.dumps(s, separators=',:'))
                b.write('\n')
            t0 = time.time(); s0 = self.size()
            data = b.getvalue()
            if self.gzip > 0:
                z = GzipFile(fileobj=self.file, mode='wb')
                z.write(data)
                z.close()
                self.file.flush()
                self.__size = (self.__size or 0) + len(data)
            else:
                self.file.write(data)
                self.__size = self.file.tell()
            t = time.time() - t0
            if t > 0.0 and max(len(data), 1024)/t < 1000000: # 1MB/s
                logging.warn('SLOW write: %dB, %.4fs', len(data), t)
            if self.size() > self.maxsize:
                self.rollover()
        logging.debug('%s _writeout done', id(self))
        
    def _flush(self):
        '''assuming lock is in place'''
        if self.buffer_size > 0:
            flushthis = None
            with self.bufflock:
                if len(self.buffer) > 0:
                    if self.executor:
                        self.executor.execute(self._writeout, self.buffer)
                    else:
                        flushthis = self.buffer
                    self.buffer = []
            if flushthis:
                self._writeout(flushthis)

    def queue(self, curis):
        if not isinstance(curis, (list, tuple)):
            curis = (curis,)
        if self.buffer_size > 0:
            flushthis = None
            it = iter(curis)
            curi = next(it, None)
            while curi:
                with self.bufflock:
                    while curi:
                        self.buffer.append(curi)
                        self.__queuecount += 1
                        curi = next(it, None)
                        if len(self.buffer) > self.buffer_size:
                            if self.executor:
                                self.executor.execute(self._writeout,
                                                      self.buffer)
                            else:
                                flushthis = self.buffer
                            self.buffer = []
                            break
                # we should writeout without locking buffer
                if flushthis:
                    self._writeout(flushthis)
                    flushthis = None
        else:
            if self.executor:
                self.executor.execute(self._writeout, curis)
            else:
                self._writeout(curis)
            self.__queuecount += len(curis)

    def age(self):
        """time elapsed since when current qfile was opened, float
        in seconds. if no qfile is open at the moment, returns -1.0"""
        if self.starttime is None: return -1.0
        return time.time() - self.starttime

    def size(self):
        #return self.file.tell()
        return self.__size

class QueueFileWriter(object):
    """writes objects in queuefile format.
    """
    def __init__(self, qfile, gzip=9, mode='wb'):
        self.fn = qfile
        self.mode = mode
        if 'r' in self.mode:
            raise ValueError, "invalid mode %r ('r' is not allowed)"
        self.gzip = gzip
        if self.gzip is not None and not self.fn.endswith('.gz'):
            self.fn += '.gz'
        self.f = None
        self.__size = 0
        self.open()
    def open(self):
        self.f = open(self.fn, self.mode)
    def close(self, rename=True):
        if self.f:
            self.f.close()
            self.f = None
        if rename:
            try:
                os.rename(self.fn+'.open', self.fn)
            except OSError as ex:
                if ex.errno == 2:
                    logging.warn('failed to rename %s to %s: %s',
                                 self.fn+'.open', self.fn, ex)
                else:
                    raise
    def size(self):
        """queue file size (uncompressed if gzip != None)
        """
        return self.__size
    def write(self, objs):
        def write_json_lines(f, objs):
            for s in objs:
                f.write(' ')
                f.write(json.dumps(s, separators=',:'))
                f.write('\n')

        if self.f is None:
            self.open()
        b = StringIO()
        if isinstance(self.gzip, int):
            z = GzipFile(fileobj=b, mode='wb', compresslevel=self.gzip)
            write_json_lines(z, objs)
            sz = z.tell()
            z.close()
            self.f.write(b.getvalue())
            # for less chance of losing data b/c of crash
            self.f.flush()
            self.__size += sz
        else:
            write_json_lines(b, objs)
            self.f.write(b.getvalue())
            self.__size = self.f.tell()

class QueueFileReader(object):
    '''reads (dequeues) from single queue file'''
    def __init__(self, qfile, noupdate=False):
        self.fn = qfile
        self.noupdate = noupdate
        self.map = None
        self.open()
    def open(self):
        fd = os.open(self.fn, os.O_RDWR)
        self.pos = 0
        sig = os.read(fd, 2)
        # check for gzip signature
        if sig == '\x1f\x8b':
            # there's no added benefit to mmap gzip file, I guess
            os.lseek(fd, 0, 0)
            self.z = GzipFile(fileobj=os.fdopen(fd), mode='rb')
            self.__next = self.__next_gzip
        else:
            self.z = None
            self.map = mmap.mmap(fd, 0, access=mmap.ACCESS_WRITE)
            # mmap dups fd, fd need not be kept open.
            os.close(fd)
            self.__next = self.__next_mmap
    def close(self):
        if self.z:
            self.z.close()
            self.z = None
        if self.map:
            self.map.close()
            self.map = None
    def __next_mmap(self):
        if self.map is None:
            logging.warn("QueueFileReader:next called on closed file:%s",
                         self.fn)
            raise StopIteration
        while self.pos < self.map.size():
            el = self.map.find('\n', self.pos + 1)
            if el < 0: el = self.map.size()
            s = self.pos
            self.pos = el + 1
            if self.map[s] == ' ':
                l = self.map[s + 1:el]
                if not self.noupdate:
                    self.map[s] = '#'
                try:
                    return json.loads(l)
                except Exception as ex:
                    logging.warn('malformed line in %s at %d: %s (%s)',
                                 self.fn, s, l, ex)
                    continue
        raise StopIteration

    def __next_gzip(self):
        while 1:
            try:
                l = self.z.readline()
            except Exception as ex:
                # probably CRC error due to truncated file. discard the rest.
                # should we keep the file for later diagnosis? we can get
                # IOError from gzip, as well as zlib.error for lower level
                # problems.
                logging.error('error in %s: %s', self.fn, str(ex))
                raise StopIteration
            if l == '': break
            if l[0] != ' ': continue
            try:
                return json.loads(l[1:])
            except Exception as ex:
                logging.warn('malformed line in %s: %s', self.fn, l)
                continue
        raise StopIteration
                             
    def next(self):
        return self.__next()

class FileDequeue(object):
    '''multi-queue file reader'''
    def __init__(self, qdir, noupdate=False,
                 reader=QueueFileReader, enq=None):
        '''reader should be a factory function for QueueFileReader-compatible
           object.'''
        self.qdir = qdir
        # timestamp of the last qfile
        self.rqfile = None
        self.rqfiles = deque()
        self.noupdate = noupdate
        self.reader = reader

        self.qfile_read = 0
        self.dequeuecount = 0

        # for status reporting
        self.qfile = None
        self.qfilestep = None

        self.enq = enq

    def get_status(self):
        r = dict(reader=(self.rqfile and hasattr(self.rqfile, 'get_status')
                         and self.rqfile.get_status()),
                 queuefilecount=self.qfile_count(),
                 queuefilereadcount=self.qfile_read,
                 dequeuecount=self.dequeuecount,
                 qfile=self.qfile,
                 qfilestep=self.qfilestep
                 )
        return r

    def close(self):
        if self.rqfile:
            self.rqfile.close()
            self.rqfile = None

    def qfile_count(self):
        # now includes a qfile being read
        return len(self.rqfiles)+(1 if self.rqfile is not None else 0)

    def qfiles_available(self, qfiles):
        self.rqfiles.extend(qfiles)

    def scan(self, refreshall=False):
        '''scan qdir for new file'''
        logging.debug("scanning %s for new qfile", self.qdir)
        try:
            ls = os.listdir(self.qdir)
        except Exception as ex:
            if isinstance(ex, OSError) and ex.errno == 2:
                logging.debug("qdir %s does not exist yet", self.qdir)
            else:
                logging.error("listdir failed on %s", self.qdir, exc_info=1)
            return
        if refreshall: self.rqfiles.clear()
        curset = set(self.rqfiles)
        new_rqfiles = []
        for f in ls:
            if not ('1' <= f[0] <= '9'): continue
            if f.endswith('.open'): continue
            if f not in curset:
                new_rqfiles.append(f)
        if new_rqfiles:
            logging.debug("found %d new queue file(s)", len(new_rqfiles))
            self.qfiles_available(sorted(new_rqfiles))
        else:
            logging.debug("no new queue file was found")

    def next_rqfile(self, timeout=0.0):
        '''blocks until next qfile becomes available'''
        #print >>sys.stderr, "next_rqfile timeout=%.3f" % timeout
        self.qfile = None
        self.qfilestep = "next"
        remaining_timeout = timeout
        pause = 0.0
        while 1:
            if self.rqfiles:
                f = self.rqfiles.popleft()
                if f.startswith('/'):
                    self.qfile = f
                else:
                    self.qfile = os.path.join(self.qdir, f)
                logging.debug("opening %s", self.qfile)
                with timelog('open %s' % self.qfile, warn=2.0):
                    try:
                        self.qfilestep = "initializing"
                        self.rqfile = self.reader(
                            self.qfile,
                            noupdate=self.noupdate
                            )
                        self.qfilestep = "dispensing"
                    except mmap.error as ex:
                        if ex.errno == 22:
                            # error for empty file on Python -2.6
                            os.remove(self.qfile)
                            self.qfilestep = "error_removed"
                            continue
                        else:
                            raise
                    except ValueError as ex:
                        # "mmap offset is greater than file size"
                        # error for empty file on Python 2.7-
                        logging.error('failed to open %s: %s',
                                      self.qfile, str(ex))
                        setasidefn = os.path.join(
                            os.path.dirname(self.qfile),
                            '#'+os.path.basename(self.qfile)
                            )
                        try:
                            os.rename(self.qfile, setasidefn)
                            logging.info('set aside %s as %s',
                                         self.qfile, setasidefn)
                            self.qfilestep = "error set aside"
                            continue
                        except OSError, ex:
                            logging.warn('rename %s #%s failed',
                                         self.qfile, setasidefn)
                            continue
                        
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
                with timelog('scan'):
                    self.scan()
                pause = EMPTY_PAUSE
                continue
                
    def get(self, timeout=0.0):
        '''currently this method is not thread-safe'''
        while 1:
            if self.rqfile:
                curi = next(self.rqfile, None)
                if curi:
                    self.dequeuecount += 1
                    return curi

                self.rqfile.close()
                if not self.noupdate:
                    with timelog('unlink %s' % self.rqfile.fn, warn=0.1):
                        try:
                            os.unlink(self.rqfile.fn)
                        except:
                            logging.warn("unlink failed on %s",
                                         self.rqfile.fn, exc_info=1)
                self.rqfile = None
                if not self.next_rqfile(timeout):
                    return None
            else:
                if not self.next_rqfile(timeout):
                    return None

    def pull(self):
        """checks if there are qfiles ready for reading,
        and closes enq if not. if enq is not set, this is
        no-op.
        """
        if self.enq is None:
            logging.warn('FileDequeue(%r):enq is None', self.qdir)
            return

        qfc = self.qfile_count()
        if qfc == 0:
            logging.debug('%s starved, flushing enq', self.qdir)
            self.enq.close(blocking=False)
        else:
            logging.debug('%s qfile_count()=%r', self.qdir, qfc)

class DummyFileEnqueue(FileEnqueue):
    """class compatible with FileEnqueue, but has no actual 'enqueue'
    functionality (raises error when performed). only implements
    those methods used by Scheduler."""
    def __init__(self, qdir, **kwds):
        super(DummyFileDequeue, self).__init__(qdir)
    def _flush(self):
        pass
    def close(self):
        pass
    def queue(self, curi):
        raise NotImplementedError, 'dummy - curi lost: %s' % curi
    
class DummyFileDequeue(FileDequeue):
    """class compatible with FileDequeue, but has no actual 'dequeue'
    functionality. It's used for providing statistics.
    """
    def __init__(self, qdir, enq=None, **kwds):
        super(DummyFileDequeue, self).__init__(qdir)
        self.lastscan = 0
        self.enq = enq
        
    def qfile_count(self):
        self.__update_rqfiles()
        return super(DummyFileDequeue, self).qfile_count()

    def scan(self):
        """extended so that those files removed on the filesystem
        are dropped from self.rqfiles as well"""
        super(DummyFileDequeue, self).scan(refreshall=True)
        self.lastscan = time.time()

    def __update_rqfiles(self):
        try:
            mtime = os.stat(self.qdir).st_mtime
            if mtime > self.lastscan:
                self.scan()
        except Exception as ex:
            logging.warn('error:%s', ex)

    def get(self, *kwds):
        raise NotImplementedError
