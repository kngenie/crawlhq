import sys, os
sys.path[0:0] = ('/opt/hq/lib',)
import web
import json
import time
import re
import itertools
from cfpgenerator import FPGenerator
from urlparse import urlsplit, urlunsplit
import threading
import random
from Queue import Queue, Empty
import traceback
from fileinq import IncomingQueue
from filequeue import FileEnqueue, FileDequeue
from scheduler import Scheduler
import leveldb
from executor import *
import logging

class Seen(object):
    _fp64 = FPGenerator(0xD74307D3FD3382DB, 64)
    S64 = 1<<63
    EXPIRE_NEVER = (1<<32)-1

    def __init__(self, dbdir):
        self.dbdir = dbdir
        self.ready = threading.Event()
        self._open()
        self.ready.set()

    def _open(self):
        logging.info("opening seen-db %s", self.dbdir)
        self.seendb = leveldb.IntHash(self.dbdir,
                                      block_cache_size=16*(1024**3),
                                      block_size=4096,
                                      max_open_files=256,
                                      write_buffer_size=128*(1024**2))
        logging.info("seen-db %s is ready", self.dbdir)

    def close(self):
        logging.info("closing leveldb...")
        self.seendb.close()
        logging.info("closing leveldb...done")
        self.seendb = None

    @staticmethod
    def urikey(uri):
        uhash = Seen._fp64.sfp(uri)
        return uhash

    @staticmethod
    def keyquery(key):
        return {'_id': key}

    @staticmethod
    def uriquery(uri):
        return Seen.keyquery(Seen.urikey(uri))

    def already_seen(self, uri):
        self.ready.wait()
        key = Seen.urikey(uri)
        v = self.seendb.get(key)
        if not v:
            self.seendb.put(key, '1')
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

class SeenChecker(object):
    def __init__(self):
        self.seen = self.Seen()
        self.deq = FileDequeue()


    def run(self):
        while 1:
            self.split()
            
        
