#!/usr/bin/python
#
# dump active work set into file-based queue
import sys, os
sys.path[0:0] = (os.path.join(sys.path[0], '../lib'),)
from fileinq import FileEnqueue
from threading import RLock
import pymongo

class seen_ud(object):
    # split long URL, use fp for the tail (0.02-0.04s/80URIs)
    @staticmethod
    def longkeyhash32(s):
        return ("#%x" % (_fp32.fp(s) >> 32))
    @staticmethod
    def hosthash(h):
        # mongodb can only handle upto 64bit signed int
        #return (_fp63.fp(h) >> 1)
        return int(_fp31.fp(h) >> 33)

    @staticmethod
    def urlkey(url):
        k = {}
        # 790 < 800 - (32bit/4bit + 1)
        if len(url) > 790:
            u1, u2 = url[:790], url[790:]
            k.update(u1=u1, u2=u2, h=self.longkeyhash32(u2))
        else:
            k.update(u1=url, h='')
        return k
    @staticmethod
    def keyurl(k):
        return k['u1']+k['u2'] if 'u2' in k else k['u1']
    @staticmethod
    def keyfp(k):
        url = k['u1']
        p1 = url.find('://')
        if p1 > 0:
            p2 = url.find('/', p1+3)
            host = url[p1+3:p2] if p2 >= 0 else url[p1+3:]
        else:
            host = ''
        return self.hosthash(host)
    @staticmethod
    def keyhost(k):
        return k['H']
    # name is incorrect
    @staticmethod
    def keyquery(k):
        # in sharded environment, it is important to have shard key in
        # a query. also it is necessary for non-multi update to work.
        return {'fp':self.keyfp(k), 'u.u1':k['u1'], 'u.h':k['h']}
    # old and incorrect name
    uriquery = keyquery

QUEUE_DIRECTORY = '/1/crawling/ws'

NWORKSETS = 256

class WorksetQueue(FileEnqueue):
    MAXSIZE = 500 * 1000 * 1000 # 500M

    def __init__(self, qdir):
        FileEnqueue.__init__(self, qdir)
        self.qfilelock = RLock()

    def queue(self, curi):
        with self.qfilelock:
            if not self.file:
                self.open()
            FileQueue.queue(self, curi)
            if self.size > self.MAXSIZE:
                self.close()

class Worksets(object):
    MAXSIZE = 500 * 1000 * 1000 # 500M
    MAXAGE = 30.0

    def __init__(self, job, qdirbase):
        self.job = job
        self.qdir = os.path.join(qdirbase, job)
        if not os.path.isdir(self.qdir):
            os.makedirs(self.qdir)
        # create subdirectories for each workset
        for ws in xrange(NWORKSETS):
            wsd = self.wsqdir(ws)
            if not os.path.isdir(wsd):
                os.mkdir(wsd)

        self.workset_queues = [None] * NWORKSETS

    def wsqdir(self, ws):
        return os.path.join(self.qdir, str(ws))

    def get_wsq(self, ws):
        wsq = self.workset_queues[ws]
        if wsq is None:
            wsq = self.workset_queues[ws] = WorksetQueue(self.wsqdir(ws))
        return wsq

    def suri_workset(self, suri):
        if 'ws' not in suri:
            raise Exception, 'ws is missing in curi'
        return suri['ws']

    def enqueue(self, suri):
        ws = self.suri_workset(suri)
        wsq = self.get_wsq(ws)

        curi = dict(u=seen_ud.keyurl(suri['u']), id=suri['_id'])
        w = suri.get('w')
        if w:
            curi['p'] = w.get('p')
            if 'v' in w and w['v']:
                curi['v'] = w['v']
            if 'x' in w and w['x']:
                curi['x'] = w['x']
        if 'a' in suri:
            curi['a'] = suri['a']

        wsq.queue(curi)
            
conn = pymongo.Connection()
db = conn.crawl
seen = db.seen

worksets = Worksets('wide', '/1/crawling/hq/ws')

fpb = 0
FPMAX = ((1<<32)-1)
i = 0
while fpb <= FPMAX:
    cur = seen.find({'fp':fpb, 'co':{'$gt':0}})
    for u in cur:
        i += 1
        print >>sys.stderr, "%d %d %s" % (fpb, i, u)
        worksets.queue(u)
    fpb += 1
