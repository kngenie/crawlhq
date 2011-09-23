import sys, os
import mmap
import cjson
import logging

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
        fd = os.open(self.qfile, os.O_RDWR)
        self.map = mmap.mmap(fd, 0, access=mmap.ACCESS_WRITE)
        os.close(fd)

        self.index = []
        p = 0
        while p < self.map.size():
            el = self.map.find('\n', p)
            if el < 0: el = self.map.size()
            if self.map[p] == ' ':
                o = cjson.decode(self.map[p + 1:el])
                key = o.get('id')
                if key is None: key = self.urikey(o)
                self.index.append((key, p))
            p = el + 1
        self.index.sort(lambda x, y: cmp(x[0], y[0]))
        # for de-duping sequence of the same URL.
        # it would be effective if crawler does no local seen check.
        self.prevkey = None

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
        while 1:
            if len(self.index) == 0:
                raise StopIteration
            a = self.index.pop(0)
            if not self.noupdate:
                self.map[a[1]] = '#'
            if self.prevkey == a[0]:
                continue
            el = self.map.find('\n', a[1] + 1)
            if el < 0: el = self.map.size()
            try:
                l = self.map[a[1] + 1:el]
                o = cjson.decode(l)
            except Exception as ex:
                logging.warn('malformed line in %s at %d: %s',
                             self.qfile, a[1], l)
                continue
            self.prevkey = a[0]
            if 'id' not in o:
                o['id'] = a[0]
            return o
