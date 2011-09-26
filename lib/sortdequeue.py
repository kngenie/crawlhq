import sys, os
import mmap
import cjson
import logging
import time
import weakref

# for use with weak refs
class curi(dict):
    pass
class SortingQueueFileReader(object):
    '''variation of filequeue.QueueFileReader that pre-sorts qfile lines
       by urikey. this brings 10x or more speed gain to leveldb based seen
       check. this class is abstract - urikey should be overridden with
       actual function that computes "id" value for the URL.'''
    def __init__(self, qfile, noupdate=False):
        self.qfile = qfile
        self.noupdate = noupdate
        self.open()
    def urikey(self, u):
        '''override this with actual key function'''
        raise NotImplementedError('SortingQueueFileReader.urikey')
    def open(self):
        fd = os.open(self.qfile, os.O_RDONLY)
        #fd = os.open(self.qfile, os.O_RDWR)
        #self.map = mmap.mmap(fd, 0, access=mmap.ACCESS_WRITE)
        self.map = mmap.mmap(fd, 0, access=mmap.ACCESS_READ)
        #self.map = mmap.mmap(fd, 0, flags=mmap.MAP_PRIVATE|0x8000, prot=mmap.PROT_READ)
        os.close(fd)

        logging.debug('scanning %s', self.qfile)
        self.index = []
        p = 0
        while p < self.map.size():
            el = self.map.find('\n', p)
            if el < 0: el = self.map.size()
            if self.map[p] == ' ':
                try:
                    o = cjson.decode(self.map[p + 1:el])
                except cjson.DecodeError:
                    logging.warn("skipping malformed JSON at %d", p)
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
                self.index.append((key, p, o))
                #self.index.append((key, p))
            p = el + 1
        logging.debug('sorting %d entries in %s', len(self.index), self.qfile)
        self.index.sort(lambda x, y: cmp(x[0], y[0]))
        logging.debug('sorting done')
        # for de-duping sequence of the same URL.
        # it would be effective if crawler does no local seen check.
        self.prevkey = None
        self.itemcount = len(self.index)
        self.dupcount = 0
        self.rereadcount = 0
        self.dispensecount = 0

    def get_status(self):
        r = dict(qfile=self.qfile,
                 itemcount=self.itemcount,
                 dispensecount=self.dispensecount,
                 dupcount=self.dupcount,
                 rereadcount=self.rereadcount)
        return r
                 
    def close(self):
        if self.map:
            self.map.close()
            self.map = None
        del self.index
    def __iter__(self):
        return self
    def next(self):
        '''not thread safe'''
        if self.map is None:
            logging.warn("SortedQueueFileReader:next called on closed file::%s",
                         self.qfile)
        t0 = time.time()
        while 1:
            if len(self.index) == 0:
                raise StopIteration
            a = self.index.pop()
            # if not self.noupdate:
            #     self.map[a[1]] = '#'
            if self.prevkey == a[0]:
                self.dupcount += 1
                logging.debug('skipping dup %s==%s at %s',
                              self.prevkey, a[0], a[1])
                continue
            o = a[2]
            if o is None:
                el = self.map.find('\n', a[1] + 1)
                if el < 0: el = self.map.size()
                self.rereadcount += 1
                try:
                    l = self.map[a[1] + 1:el]
                    o = cjson.decode(l)
                except Exception as ex:
                    # this should not happen unless qfile is modified.
                    logging.warn('malformed line in %s at %d: %s',
                                 self.qfile, a[1], l)
                    continue
            self.prevkey = a[0]
            if 'id' not in o:
                o['id'] = a[0]
            break
        t = time.time() - t0
        if t > 0.1:
            logging.warn('SLOW SortingQueueFileReader.next %.4f', t)
        self.dispensecount += 1
        return o
