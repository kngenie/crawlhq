"""
HQ dispatcher implemented on top of LevelDB.
"""
import os
import time
import logging
import hqconfig
from dispatcher import Dispatcher

class LevelDispatcher(Dispatcher):
    def __init__(self, domaininfo, job, mapper, scheduler, inq=None):
        super(LevelDispatcher, self).__init__(domaininfo, job, mapper,
                                              scheduler, inq)

        # seen database is initialized lazily
        self.seenfactory = hqconfig.factory.seenfactory()
        self.seen = None

    def shutdown(self):
        if self.seen:
            logging.info("closing seen db")
            self.seen.close()
            self.seen = None
        super(LevelDispatcher, self).shutdown()

    def init_seen(self):
        if not self.seen:
            self.seen = self.seenfactory(self.jobname)

    def clear_seen(self):
        self.init_seen()
        self.seen.clear()

    def count_seen(self):
        self.init_seen()
        self.seen._count()

    def processinq(self, maxn):
        """performs seen check with LevelDB. this implementation honors
        maxn, but actual number of cURLs processed may exceed it if incoming
        queue stores cURIs in chunks.
        """
        # lazy initialization of seen db
        self.init_seen()

        result = dict(processed=0, scheduled=0, excluded=0, saved=0,
                      td=0.0, ts=0.0)
        for count in xrange(maxn):
            t0 = time.time()
            furi = self.inq.get(0.01)
            
            result['td'] += (time.time() - t0)
            if furi is None: break
            self.processedcount += 1
            result['processed'] += 1
            ws = self.mapper.workset(furi)
            if self.is_workset_active(ws):
                # no need to call self.workset_activating(). it's already
                # done by Scheduler
                di = self.domaininfo.get_byurl(furi['u'])
                if di and di['exclude']:
                    self.excludedlist.add(furi)
                    result['excluded'] += 1
                    continue
                t0 = time.time()
                if furi.get('f', 0):
                    curi = dict(u=furi['u'])
                    a = furi.get('w')
                    if not isinstance(a, dict): a = furi
                    for k in 'pvx':
                        m = a.get(k)
                        if m is not None: curi[k] = m
                    self.scheduler.schedule(curi)
                    result['scheduled'] += 1
                else:
                    suri = self.seen.already_seen(furi)
                    if suri['e'] < int(time.time()):
                        curi = dict(u=furi['u'], id=suri['_id'])
                        a = furi.get('w')
                        if not isinstance(a, dict): a = furi
                        for k in 'pvx':
                            m = a.get(k)
                            if m is not None: curi[k] = m
                        self.scheduler.schedule(curi)
                        result['scheduled'] += 1
                result['ts'] += (time.time() - t0)
            else:
                if self.workset_state[ws]:
                    self.workset_deactivating(ws)
                # client is not active
                self.diverter.divert(str(ws), furi)
                result['saved'] += 1
        return result
