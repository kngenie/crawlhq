import sys, os
import mmap
import cjson
import logging
import time
from gzip import GzipFile

class SortedQueue(object):
    def __init__(self, qfile, urikey, cacheobj=True):
        self.fn = qfile
        self.urikey = urikey
        self.cacheobj = cacheobj
        fd = os.open(self.fn, os.O_RDONLY)
        self.map = mmap.mmap(fd, 0, access=mmap.ACCESS_READ)
        os.close(fd)
        logging.debug('scanning %s', self.fn)
        if self.map[0:2] == '\x1f\x8b':
            self.cacheobj = True
            self.build_index_gzip()
        else:
            self.build_index_regular()
        self.sort_index()
        self.itemcount = len(self.index)
        self.dupcount = 0

    def build_index_regular(self):
        self.index = []
        p = 0
        while p < self.map.size():
            el = self.map.find('\n', p)
            if el < 0: el = self.map.size()
            if self.map[p] == ' ':
                try:
                    o = cjson.decode(self.map[p + 1:el])
                except cjson.DecodeError:
                    logging.warn("skipping malformed JSON at %s:%d: %s",
                                 self.fn, p, self.map[p + 1:el])
                    p = el + 1
                    continue
                key = o.get('id')
                if key is None:
                    key = self.urikey(o)
                    if key is None:
                        raise ValueError('urikey->None for %s' % str(o))
                # saving o in index to avoid decoding again makes no big
                # difference in performance (just several secs per file,
                # within normal variance)
                if self.cacheobj:
                    self.index.append((key, o))
                else:
                    self.index.append((key, p))
            p = el + 1

    def sort_index(self):
        logging.debug('sorting %d entries in %s', len(self.index), self.fn)
        self.index.sort(lambda x, y: cmp(x[0], y[0]), reverse=True)
        logging.debug('sorting done')

    def build_index_gzip(self):
        """creates sorted index from gzip-compressed queue.
        caches object regardless of caccheobj flag.
        """
        self.index = []
        zf = GzipFile(fileobj=self.map, mode='rb')
        while 1:
            p = zf.tell() # just for diagnosis use
            try:
                l = zf.readline()
            except IOError as ex:
                # probably CRC error due to truncated file. discard the rest.
                logging.error('error in %s at %d: %s', self.fn, p, str(ex))
                break
            if not l: break
            if l[0] != ' ': continue
            try:
                o = cjson.decode(l[1:])
            except Exception as ex:
                logging.warn("skipping malformed JSON at %s:%d: %s",
                             self.fn, p, l[1:])
                continue
            key = o.get('id')
            if key is None:
                try:
                    key = self.urikey(o)
                except UnicodeEncodeError:
                    pass
                if key is None:
                    logging.error('urikey->None for %s', str(o))
                    continue
            self.index.append((key, o))
        zf.close()
            
    def close(self):
        if self.map:
            self.map.close()
            self.map = None
        del self.index
    def closed(self):
        return self.map is None

    def peek(self, skip=None):
        while self.map and len(self.index) > 0:
            # be sure to change "<=" to ">=" if you change sort order
            if skip and self.index[-1][0] <= skip:
                # if not self.update:
                #     self.map[a[1]] = '#'
                self.index.pop()
                self.dupcount += 1
                continue
            logging.debug('peek=%s', self.index[-1][0])
            return self.index[-1][0]
        logging.debug('peek=None')
        return None
    def getnext(self):
        '''not thread safe'''
        if self.map is None:
            logging.warn("SortedQueueFileReader:next called on closed file:%s",
                         self.fn)
        t0 = time.time()
        while 1:
            if len(self.index) == 0:
                return None
            a = self.index.pop()
            # if not self.noupdate:
            #     self.map[a[1]] = '#'
            if self.cacheobj:
                o = a[1]
            else:
                el = self.map.find('\n', a[1] + 1)
                if el < 0: el = self.map.size()
                l = self.map[a[1] + 1:el]
                o = cjson.decode(l)
            if 'id' not in o:
                o['id'] = a[0]
            break
        t = time.time() - t0
        if t > 0.1:
            logging.warn('SLOW SortedQueue.next %.4f', t)
        return o

class SortingQueueFileReader(object):
    '''variation of filequeue.QueueFileReader that pre-sorts qfile lines
       by urikey. this brings 10x or more speed gain to leveldb based seen
       check. this class is abstract - urikey should be overridden with
       actual function that computes "id" value for the URL.'''
    def __init__(self, qfile, urikey, noupdate=False, cacheobj=True):
        self.qfile = SortedQueue(qfile, urikey, cacheobj)
        self.noupdate = noupdate

        # for de-duping sequence of the same URL.
        # it would be effective if crawler does no local seen check.
        self.prevkey = None
        self.dupcount = 0
        self.dispensecount = 0

    @property
    def fn(self):
        return self.qfile.fn

    def get_status(self):
        r = dict(qfile=self.qfile.fn,
                 itemcount=self.qfile.itemcount,
                 dispensecount=self.dispensecount,
                 dupcount=self.qfile.dupcount)
        return r
                 
    def close(self):
        self.qfile.close()
    def __iter__(self):
        return self
    def next(self):
        '''not thread safe'''
        while 1:
            k = self.qfile.peek(self.prevkey)
            if k is None:
                raise StopIteration
            try:
                o = self.qfile.getnext()
            except cjson.DecodeError as ex:
                # this should not happen unless qfile is modified
                logging.warn('malformed line in %s', self.qfile.fn)
                continue
            self.prevkey = o['id']
            self.dispensecount += 1
            return o

class MergeSortingQueueFileReader(object):
    def __init__(self, qfiles, urikey, noupdate=True):
        if isinstance(qfiles, basestring): qfiles = [qfiles]
        self.qfiles = [SortedQueue(qfile, urikey, cacheobj=False)
                            for qfile in qfiles]
        self.prevkey = None
        self.dispensecount = 0
    def close(self):
        for qfile in self.qfiles:
            qfile.close()
    def get_status(self):
        itemcount = sum(qfile.itemcount for qfile in self.qfiles)
        dupcount = sum(qfile.dupcount for qfile in self.qfiles)
        r = dict(qfile=[qfile.fn for qfile in self.qfiles],
                 itemcount=itemcount,
                 dispensecount=self.dispensecount,
                 dupcount=dupcount)
        return r
    def __iter__(self):
        return self
    def next(self):
        k = None
        q = None
        # find queue with max key but less than self.prevkey
        for qfile in self.qfiles:
            pk = qfile.peek(self.prevkey)
            if pk is not None:
                if k is None or k < pk:
                    k = pk
                    q = qfile
        if q is None:
            raise StopIteration
        o = q.getnext()
        self.dispensecount += 1
        self.prevkey = o['id']
        return o
        
        
        
