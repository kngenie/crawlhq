import sys, os
import pymongo
import time
import re
import itertools
#from cfpgenerator import FPGenerator
from urlparse import urlsplit, urlunsplit
import threading
import random
from Queue import Queue, Empty
import traceback
from filequeue import FileEnqueue, FileDequeue

#_fp31 = FPGenerator(0xBA75BB4300000000, 31)

# TODO: move Seen and CrawlInfo to HQ main file
class WorkSet(object):
    WORKSET_DIR = '/1/crawling/hq/ws'
    def __init__(self, jobname, wsid):
        self.job = jobname
        self.wsid = wsid

        self.qdir = os.path.join(WorkSet.WORKSET_DIR,
                                 self.job, str(self.wsid))

        self.enq = FileEnqueue(self.qdir, maxage=0.0)
        self.deq = FileDequeue(self.qdir)

        self.running = True

        self.scheduledcount = 0
        self.checkedoutcount = 0
        self.finishedcount = 0
        self.activecount = 0

    def shutdown(self):
        self.enq.close()
        self.deq.close()

    def get_status(self):
        r = dict(id=self.wsid, running=self.running,
                 scheduled=self.scheduledcount,
                 checkedout=self.checkedoutcount,
                 finished=self.finishedcount
                 )
        return r

    def schedule(self, curi):
        self.enq.queue(curi)
        self.scheduledcount += 1

    def checkout(self, n):
        if not self.running:
            return []
        r = []
        while len(r) < n:
            curi = self.deq.get(timeout=0.01)
            if curi is None:
                self.enq.close()
                break
            r.append(curi)
        self.checkedoutcount += len(r)
        return r
    
    def deschedule(self, furi):
        self.finishedcount += 1

class ClientQueue(object):
    CHECKEDOUT_SPILL_SIZE = 10000

    def __init__(self, job, worksets):
        #self.job = job
        self.worksets = worksets
        if len(self.worksets) == 0:
            print >>sys.stderr, "%s: no worksets" % self
        # persistent index into worksets
        self.next = 0
        self.feedcount = 0

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

    #def enough_room(self):
    #    return len(self.checkedout) < (self.CHECKEDOUT_SPILL_SIZE - 500)

    def get_status(self):
        r = dict(feedcount=self.feedcount,
                 next=self.next,
                 worksetcount=len(self.worksets))
        worksets = []
        scheduledcount = 0
        activecount = 0
        for ws in self.worksets:
            worksets.append(ws.wsid)
            scheduledcount += ws.scheduledcount
            activecount += ws.activecount
        r['worksets'] = worksets
        r['scheduledcount'] = scheduledcount
        r['activecount'] = activecount
        return r
        
    #def is_active(self, url):
    #    return url in self.checkedout
            
    def reset(self):
        # TODO: return something more useful for diagnosing failures
        result = dict(ws=[], ok=True)
        for ws in self.worksets:
            ws.reset()
            result['ws'].append(ws.id)
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

    def feed(self, n):
        checkout_per_ws = n // 4 + 1
        r = []
        for a in (1, 2, 3):
            excount = len(self.worksets)
            while len(r) < n and excount > 0:
                if self.next >= len(self.worksets): self.next = 0
                curis = self.worksets[self.next].checkout(checkout_per_ws)
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
            time.sleep(0.5)
            
        #with self.colock:
        #    for curi in curis:
        #        self.checkedout[self.keyurl(curi['u'])] = curi
        #if len(self.checkedout) > self.CHECKEDOUT_SPILL_SIZE:
        #    executor.execute(self.spill_checkedout)
        self.feedcount += len(r)
        return r

class Scheduler(object):
    '''per job scheduler. manages assignment of worksets to each client.'''
    #NWORKSETS = 4096
    #NWORKSETS_BITS = 8
    #NWORKSETS = 256
    #NWORKSETS = 1 << NWORKSETS_BITS
    
    def __init__(self, job, mapper, seen, crawlinfodb=None):
        self.job = job
        #self.db = crawlinfodb
        self.mapper = mapper
        self.NWORKSETS = self.mapper.nworksets
        self.clients = {}
        self.crawlinfo = crawlinfodb
        #self.crawlinfo = CrawlInfo(self.db.seen[self.job])
        self.seen = seen
        #self.seen = Seen(dbdir='/1/crawling/hq/seen')
        #self.get_jobconf()

        self.worksets = [WorkSet(job, wsid) for wsid in xrange(self.NWORKSETS)]
        #self.load_workset_assignment()
        
    def shutdown(self):
        for clq in self.clients.values():
            clq.shutdown()
        for ws in self.worksets:
            ws.shutdown()
    
    def get_status(self):
        r = dict(
            nworksets=self.NWORKSETS,
            clients={}
            )
        for i, client in self.clients.items():
            r['clients'][i] = client.get_status()
        return r

    def get_workset_status(self):
        r = [ws.get_status() for ws in self.worksets]
        return r

    # def get_jobconf(self):
    #     self.jobconf = self.db.jobconfs.find_one({'name':self.job})
    #     if self.jobconf is None:
    #         self.jobconf = {'name':self.job}
    #         self.db.jobconfs.save(self.jobconf)

    # def create_default_workset_assignment(self):
    #     num_nodes = self.jobconf.get('nodes', 20)
    #     return list(itertools.islice(
    #             itertools.cycle(xrange(num_nodes)),
    #             0, len(self.worksets)))
        
    # def load_workset_assignment(self):
    #     r = self.db.jobconfs.find_one({'name':self.job}, {'wscl':1})
    #     wscl = self.jobconf.get('wscl')
    #     if wscl is None:
    #         wscl = self.create_default_workset_assignment()
    #         self.jobconf['wscl'] = wscl
    #         self.db.jobconfs.save(self.jobconf)
    #     if len(wscl) > len(self.worksets):
    #         wscl[len(self.worksets):] = ()
    #     elif len(wscl) < len(self.worksets):
    #         wscl.extend(itertools.repeat(None, len(self.worksets)-len(wscl)))
    #     self.worksetclient = wscl

    # def wsidforclient(self, client):
    #     '''return list of workset ids for node name of nodes-node cluster'''
    #     qids = [i for i in xrange(len(self.worksetclient))
    #             if self.worksetclient[i] == client[0]]
    #     return qids

    def get_clientqueue(self, client):
        clid = client[0]
        q = self.clients.get(clid)
        if q is None:
            worksets = [self.worksets[i] for i in self.wsidforclient(client)]
            q = ClientQueue(self.job, worksets)
            self.clients[clid] = q
            print >>sys.stderr, "new ClientQueue created for clid=%s" % clid
        return q

    # def workset(self, curi):
    #     uc = urlsplit(curi['u'])
    #     h = uc.netloc
    #     p = h.find(':')
    #     if p > 0: h = h[:p]
    #     hosthash = int(_fp31.fp(h) >> (64 - self.NWORKSETS_BITS))
    #     return hosthash

    # def workset(self, hostfp):
    #     # don't use % (mod) - FP has much less even distribution in
    #     # lower bits.
    #     return hostfp >> (31 - self.NWORKSETS_BITS)

    # Scheduler - discovered event

    def schedule(self, curi):
        #ws = self.workset(curi)
        ws = self.mapper.workset(curi)
        return self.worksets[ws].schedule(curi)

    def schedule_unseen(self, incuri):
        '''schedule_unseen to be executed asynchronously'''
        #print >>sys.stderr, "_schedule_unseen(%s)" % incuri
        # uk = self.urlkey(incuri['u'])
        # q = self.keyquery(uk)
        # expire = incuri.get('e')

        # wsid = self.workset(q['fp'])
        # if self.worksets[wsid].is_active(incuri['u']):
        #     return False

        uri = incuri['u']
        #t0 = time.time()
        suri = self.seen.already_seen(uri)
        #print >>sys.stderr, "seen %.3f" % (time.time() - t0,)
        if suri['e'] < int(time.time()):
            curi = dict(u=uri, id=suri['_id'],
                        w=dict(p=incuri.get('p'),
                               v=incuri.get('v'),
                               x=incuri.get('x'))
                        )
            #t0 = time.time()
            self.schedule(curi)
            #print >>sys.stderr, "schedule %.3f" % (time.time() - t0,)
            return True
        else:
            return False

        #print >>sys.stderr, "seen.find_one(%s)" % q
        #curi = self.seen.find_one(q)
        #print >>sys.stderr, "seen.find_one:%.3f" % (time.time() - t0,)
        # if curi is None:
        #     curi = dict(u=uk, fp=q['fp'])
        #     if expire is not None:
        #         curi['e'] = expire
        #     curi['w'] = dict(p=incuri.get('p'), v=incuri.get('v'),
        #                      x=incuri.get('x'))
        #     self.schedule(curi)
        #     return True
        # else:
        #     if 'w' in curi: return False
        #     if expire is not None:
        #         curi['e'] = expire
        #     if curi.get('e', sys.maxint) < time.time():
        #         curi['w'] = dict(p=incuri.get('p'), v=incuri.get('v'),
        #                          x=incuri.get('x'))
        #         self.schedule(curi)
        #         return True
        #     return False

    def schedule_unseen_async(self, incuri):
        '''run schdule_unseen asynchronously'''
        executor.execute(self.schedule_unseen, incuri)
        return False

    # Scheduler - finished event

    def finished(self, furi, expire=None):
        '''furi: dict(u=url,
                      a=dict(s=status, d=digest, e=etag, m=last-modified)
                      )
        '''
        wsid = self.mapper.workset(furi)
        self.worksets[wsid].deschedule(furi)
        self.crawlinfo.save_result(furi)
        
    # Scheduler - feed request

    def feed(self, client, n):
        curis = self.get_clientqueue(client).feed(n)
        if self.crawlinfo:
            for curi in curis:
                crawlinfo = self.crawlinfo.get_crawlinfo(curi)
                if crawlinfo:
                    curi['a'] = crawlinfo
        return curis

    # Scheduler - reset request

    def reset(self, client):
        return self.get_clientqueue(client).reset()

    def flush_clients(self):
        # actually this can be done without going through ClientQueues
        #for cl in self.clients.values():
        #    cl.flush_scheduled()
        for ws in self.worksets:
            ws.unload()
