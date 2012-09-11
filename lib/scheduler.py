import sys, os
import pymongo
import time
import re
import itertools
from urlparse import urlsplit, urlunsplit
import threading
import random
from Queue import Queue, Empty
import traceback
from filequeue import FileEnqueue, FileDequeue, DummyFileEnqueue
import logging

class WorkSet(object):

    def __init__(self, wsdir, wsid, writing=False, reading=True):
        self.wsid = wsid

        self.qdir = os.path.join(wsdir, str(self.wsid))

        if writing:
            FileEnqueue.recover(self.qdir)
            self.enq = FileEnqueue(self.qdir, buffer=200)
        else:
            self.enq = DummyFileEnqueue(self.qdir)
        if reading:
            self.deq = FileDequeue(self.qdir)
        else:
            # dummy?
            self.deq = None

        self.running = True

        self.scheduledcount = 0
        self.checkedoutcount = 0
        self.finishedcount = 0
        self.activecount = 0

    def flush(self):
        # _flush() should be part of close(), but not now
        self.enq._flush()
        self.enq.close()
        
    def shutdown(self):
        self.flush()
        if self.deq:
            self.deq.close()

    def get_status(self):
        r = dict(id=self.wsid, running=self.running,
                 scheduled=self.scheduledcount,
                 checkedout=self.checkedoutcount,
                 finished=self.finishedcount
                 )
        if self.enq: r['enq'] = self.enq.get_status()
        if self.deq: r['deq'] = self.deq.get_status()
        return r

    def schedule(self, curi):
        self.enq.queue(curi)
        self.scheduledcount += 1

    def checkout(self, n):
        if not self.running:
            return []
        r = []
        while len(r) < n:
            curi = self.deq.get(timeout=0.001)
            if curi is None:
                # avoid flushing queue too frequently
                if self.enq.queue_count > 10000:
                    self.enq.close()
                break
            r.append(curi)
        self.checkedoutcount += len(r)
        return r
    
    def deschedule(self, furi):
        self.finishedcount += 1

class ClientQueue(object):
    CHECKEDOUT_SPILL_SIZE = 10000

    def __init__(self, worksets, active_timeout=10*3600):
        self.worksets = worksets
        if len(self.worksets) == 0:
            logging.warn("%s: no worksets", self)
        # persistent index into worksets
        self.next = 0
        self.feedcount = 0
        self.lastfeed = None
        self.lastfeedtime = None

        self.active_timeout = active_timeout

        ## checkedout caches CURIs scheduled for this client until they are
        ## flushed to the database (they are likely be removed before getting
        ## flushed). it is used for in-memory seen check and flushing
        #self.checkedout = {}
        ## mutex for manipulating self.checkedout
        #self.colock = threading.RLock()
        #self.spilling = False
        ## mutex for self.spilling flag
        #self.spilllock = threading.RLock()

    def shutdown(self):
        pass

    def is_active(self):
        return self.lastfeedtime > time.time() - self.active_timeout

    #def enough_room(self):
    #    return len(self.checkedout) < (self.CHECKEDOUT_SPILL_SIZE - 500)

    def get_status(self):
        r = dict(feedcount=self.feedcount,
                 lastfeedcount=self.lastfeed,
                 lastfeedtime=self.lastfeedtime,
                 next=self.next,
                 worksetcount=len(self.worksets))
        worksets = []
        scheduledcount = 0
        activecount = 0
        finishedcount = 0
        nqfiles = 0
        for ws in self.worksets:
            worksets.append(ws.wsid)
            scheduledcount += ws.scheduledcount
            activecount += ws.activecount
            finishedcount += ws.finishedcount
            nqfiles += ws.deq.qfile_count()
        r['worksets'] = worksets
        r['scheduledcount'] = scheduledcount
        r['activecount'] = activecount
        r['finishedcount'] = finishedcount
        r['qfilecount'] = nqfiles
        return r
        
    def reset(self):
        # TODO: client decommission recovery NOT IMPLEMENTED YET
        # store URLs shipped in log, and send back those unfinished
        # back into workset.
        # TODO: return something more useful for diagnosing failures
        result = dict(ws=[], ok=True)
        # for ws in self.worksets:
        #     ws.reset()
        #     result['ws'].append(ws.id)
        return result

    def flush_scheduled(self):
        for ws in self.worksets:
            ws.unload()

    # unused - merge into WorkSet.unload()
    def spill_checkedout(self, exh=False):
        with self.spilllock:
            if self.spilling: return
            self.spilling = True
        try:
            with self.colock:
                while self.checkedout:
                    (k, o) = self.checkedout.popitem()
                    o['co'] = 0
                    # TODO: should push back to workset's scheduled queue,
                    # rather than into database.
                    db.seen.save(o)
                    if not exh and self.enough_room():
                        break
        finally:
            db.connection.end_request()
            with self.spilllock:
                self.spilling = False

    #def clear_checkedout(self, uri):
    #    with self.colock:
    #        cocuri = self.checkedout.pop(uri, None)
    #    return cocuri

    # ClientQueue.feed
    def feed(self, n):
        self.lastfeedtime = time.time()
        # n = 0 is used for keep-alive (client notifying it's active).
        if n == 0: return []

        checkout_per_ws = n // 4 + 1
        r = []
        for a in (1, 2, 3):
            excount = len(self.worksets)
            while len(r) < n and excount > 0:
                if self.next >= len(self.worksets): self.next = 0
                t0 = time.time()
                curis = self.worksets[self.next].checkout(checkout_per_ws)
                t = time.time() - t0
                if t > 0.1:
                    logging.warn('SLOW WorkSet.checkout #%s %d, %.4fs',
                                 self.worksets[self.next].wsid, len(curis), t)
                self.next += 1
                if curis:
                    excount = len(self.worksets)
                    r.extend(curis)
                else:
                    excount -= 1
            if len(r) > 0: break
            # wait a while - if we don't, client may call again too soon,
            # keeping HQ too busy responding to feed request to fill the
            # queue.
            logging.debug('feed sleeping 0.5 [%s]', a)
            time.sleep(0.5)
            
        #with self.colock:
        #    for curi in curis:
        #        self.checkedout[self.keyurl(curi['u'])] = curi
        #if len(self.checkedout) > self.CHECKEDOUT_SPILL_SIZE:
        #    executor.execute(self.spill_checkedout)
        self.lastfeed = len(r)
        self.feedcount += self.lastfeed
        return r

class Scheduler(object):
    '''per job scheduler. manages assignment of worksets to each client.'''
    
    def __init__(self, wsdir, mapper, client_active_timeout=10*3600,
                 writing=True, reading=True):
        """client_active_timeout: max time client remains active without
        feeding (in seconds)
        """
        self.wsdir = wsdir
        self.mapper = mapper
        self.client_active_timeout = client_active_timeout

        self.NWORKSETS = self.mapper.nworksets
        self.clients = {}

        self.worksets = [WorkSet(wsdir, wsid, writing=writing, reading=reading)
                         for wsid in xrange(self.NWORKSETS)]

    def flush(self):
        r = []
        for ws in self.worksets:
            if ws.flush(): r.append(ws.wsid)
        return r

    def shutdown(self):
        for ws in self.worksets:
            ws.shutdown()
        for clq in self.clients.values():
            clq.shutdown()
    
    def get_status(self):
        r = dict(
            nworksets=self.NWORKSETS,
            )
        if self.clients:
            r['clients'] = dict((i, client.get_status())
                                for i, client in self.clients.iteritems())
        return r

    def get_workset_status(self):
        r = [ws.get_status() for ws in self.worksets]
        return r

    def get_clientqueue(self, clid):
        q = self.clients.get(clid)
        if q is None:
            worksets = [self.worksets[i]
                        for i in self.mapper.wsidforclient(clid)]
            q = ClientQueue(worksets)
            self.clients[clid] = q
            logging.debug("new ClientQueue created for clid=%s", clid)
        return q

    # Scheduler - discovered event

    def schedule(self, curi, ws=None):
        if ws is None:
            ws = self.mapper.workset(curi)
        return self.worksets[ws].schedule(curi)

    # Scheduler - finished event

    def finished(self, furi, expire=None):
        '''furi: dict(u=url,
                      a=dict(s=status, d=digest, e=etag, m=last-modified)
                      )
        '''
        wsid = self.mapper.workset(furi)
        self.worksets[wsid].deschedule(furi)
        
    # Scheduler - feed request

    def feed(self, client, n):
        clq = self.get_clientqueue(client)
        if not clq.is_active():
            self.mapper.client_activating(client)
        curis = clq.feed(n)
        return curis

    def lastfeedtime(self, clid):
        cl = self.clients.get(clid)
        return cl and cl.lastfeedtime
    def is_active(self, clid):
        cl = self.clients.get(clid)
        return cl and cl.is_active()

    # Scheduler - reset request

    def reset(self, client):
        return self.get_clientqueue(client).reset()

    def flush_clients(self):
        # actually this can be done without going through ClientQueues
        #for cl in self.clients.values():
        #    cl.flush_scheduled()
        for ws in self.worksets:
            ws.flush()

    def flush_workset(self, wsid):
        if 0 <= wsid < len(self.worksets):
            self.worksets[wsid].flush()
        else:
            logging.debug('wsid out of range: %d', wsid)
                 
