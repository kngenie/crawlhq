import os
import re
import threading
from Queue import Queue, Empty, Full
import logging

import urihash
import leveldb
import hqconfig

"""
Seen database implementation on top of LevelDB.
"""

class SeenFactory(object):
    def __init__(self):
        self.job_seen = dict()

    def __call__(self, job):
        # seen cache parameter is in MB
        seen = self.job_seen.get(job)
        if not seen:
            cachesize = hqconfig.get('seencache')
            if cachesize: cachesize = int(cachesize)*(1024**2)
            seen = Seen(dbdir=hqconfig.seendir(self.jobname))
            self.job_seen[job] = seen
        return seen

class Seen(object):
    #_fp64 = FPGenerator(0xD74307D3FD3382DB, 64)
    EXPIRE_NEVER = (1<<32)-1

    def __init__(self, dbdir, block_cache_size=None):
        self.dbdir = dbdir
        self.block_cache_size = (block_cache_size or
            self.default_block_cache_size())
        self.ready = threading.Event()
        self._open()
        self.ready.set()
        self.putqueue = Queue(1000)
        self.memhash = {}
        self.drainlock = threading.RLock()

        self.addedcount = 0

    def default_block_cache_size(self):
        try:
            f = open('/proc/meminfo')
            for l in f:
                if l.startswith('MemTotal:'):
                    mem = int(re.split('\s+', l)[1]) # in kB
                    # use 50% of total memory
                    return mem * 1000 / 2
        except:
            pass
        # conservative(!) 4GB, assuming 8GB machine.
        return 4*(1024**3)

    def _open(self):
        logging.info("opening seen-db %s", self.dbdir)
        if not os.path.isdir(self.dbdir):
            os.makedirs(self.dbdir)
        self.seendb = leveldb.IntHash(self.dbdir,
                                      block_cache_size=self.block_cache_size,
                                      block_size=4096,
                                      max_open_files=256,
                                      write_buffer_size=128*(1024**2))
        logging.info("seen-db %s is ready", self.dbdir)

    def flush(self):
        logging.info("flushing putqueue (%d)", self.putqueue.qsize())
        self.drain_putqueue()

    def close(self):
        self.flush()
        logging.info("closing leveldb...")
        self.seendb.close()
        logging.info("closing leveldb...done")
        self.seendb = None

    def get_status(self):
        s = dict(
            ready=self.ready.is_set(),
            blockcachesize=self.block_cache_size,
            putqueuesize=self.putqueue.qsize(),
            addedcount=self.addedcount
            )
        return s

    def _count(self):
        self.ready.wait()
        it = self.seendb.new_iterator()
        if not it: return 0
        it.seek_to_first()
        c = 0
        while it.valid():
            c += 1
            it.next()
        return c

    # @staticmethod
    # def urikey(uri):
    #     uhash = Seen._fp64.sfp(uri)
    #     return uhash

    @staticmethod
    def keyquery(key):
        return {'_id': key}

    @staticmethod
    def uriquery(uri):
        return Seen.keyquery(urihash.urikey(uri))

    def drain_putqueue(self):
        # prevent multiple threads from racing on draining - it just
        # makes performance worse. should not happen often
        with self.drainlock:
            try:
                while 1:
                    key = self.putqueue.get_nowait()
                    self.seendb.put(key, '1')
                    if key in self.memhash:
                        del self.memhash[key]
            except Empty:
                pass

    def _mark_seen(self, key):
        while 1:
            try:
                self.putqueue.put_nowait(key)
                self.memhash[key] = self.EXPIRE_NEVER
                self.addedcount += 1
                return
            except Full:
                self.drain_putqueue()

    def mark_seen(self, furi):
        self.ready.wait()
        key = furi.get('id')
        if key is None:
            key = furi['id'] = urihash.urikey(furi['u'])
        self._mark_seen(key)

    def already_seen(self, furi):
        self.ready.wait()
        key = furi.get('id') or urihash.urikey(furi['u'])
        if key in self.memhash:
            return {'_id': key, 'e': self.memhash[key]}
        v = self.seendb.get(key)
        if not v:
            self._mark_seen(key)
            return {'_id': key, 'e': 0}
        else:
            return {'_id': key, 'e': self.EXPIRE_NEVER}

    def repair(self):
        self.ready.clear()
        try:
            self.close()
            leveldb.IntHash.repair_db(self.dbdir)
            self._open()
        finally:
            self.ready.set()

    def clear(self):
        self.ready.clear()
        try:
            self.close()
            logging.info('deleting files in %s', self.dbdir)
            for f in os.listdir(self.dbdir):
                p = os.path.join(self.dbdir, f)
                try:
                    os.remove(p)
                except:
                    logging.warn('os.remove failed on %s', p, exc_info=1)
            logging.info('done deleting files, re-creating %s', self.dbdir)
            self._open()
        finally:
            self.ready.set()
