class WorkSet(seen_ud):
    SCHEDULE_SPILL_SIZE = 30000
    SCHEDULE_MAX_SIZE = 50000
    SCHEDULE_LOW = 500
    loadgroup = []
    loadgrouplock = threading.RLock()
    def __init__(self, job, wsid):
        self.job = job
        self.id = wsid
        self.seen = db.seen
        # scheduled CURIs
        self.scheduled = Queue(maxsize=self.SCHEDULE_MAX_SIZE)
        self.loadlock = threading.RLock()
        self.loading = False
        self.activelock = threading.RLock()
        self.active = {}
        self.running = False
        # set to True when this WorkSet is being dropped,
        # because it's been re-assigned to other worker.
        self.dropping = False

    def get_status(self):
        def is_locked(lock):
            acquired = lock.acquire(blocking=False)
            if acquired: lock.release()
            return not acquired
        r = dict(id=self.id, scheduledcount=self.scheduled.qsize(),
                 loading=self.loading,
                 activecount=len(self.active),
                 running=self.running,
                 loadlock=is_locked(self.loadlock),
                 activelock=is_locked(self.activelock)
                 )
        return r
                 
    def is_active(self, url):
        with self.activelock:
            return url in self.active
    def add_active(self, curi):
        with self.activelock:
            self.active[self.keyurl(curi['u'])] = curi
    def remove_active(self, curi):
        with self.activelock:
            self.active.pop(self.keyurl(curi['u']), None)
    def remove_active_uri(self, url):
        '''same as remove_active, but by raw URI'''
        with self.activelock:
            self.active.pop(url, None)

    def scheduledcount(self):
        return self.scheduled.qsize()
    def activecount(self):
        return len(self.active)

    def reset(self):
        self.running = False
        with self.activelock:
            for o in self.active.values():
                o['co'] = 0
        self.unload()
        # reset those checkedout CURIs in the database
        e = self.seen.update(
            {'co':{'$gt': 0}, 'ws':self.id},
            {'$set':{'co':0}},
            multi=True, safe=True)
        return e
            
    def checkout(self, n):
        self.running = True
        r = []
        while len(r) < n:
            try:
                o = self.scheduled.get(block=False)
                o['co'] = int(time.time())
                r.append(o)
            except Empty:
                break
        self.load_if_low()
        return r

    def load_if_low(self):
        with self.loadlock:
            if self.loading: return
            if self.scheduled.qsize() < self.SCHEDULE_LOW:
                self.loading = True
                if True:
                    executor.execute(self._load)
                else:
                    with WorkSet.loadgrouplock:
                        WorkSet.loadgroup.append(self)
                        # if this is the first WorkSet, schedule group
                        # loading task.
                        if len(WorkSet.loadgroup) == 1:
                            executor.execute(self._load_group)

    def _load_group(self):
        time.sleep(0.05) # wait 50ms
        with self.loadgrouplock:
            group = WorkSet.loadgroup
            WorkSet.loadgroup = []
        if len(group) == 0: return
        if len(group) == 1:
            group[0]._load()
            return
        t0 = time.time()
        l = []
        limit = self.SCHEDULE_LOW * len(group)
        wsa = []; wsmap = {}
        for ws in group:
            wsa.append(ws.id)
            wsmap[ws.id] = ws
        try:
            qfind = dict(co=0, ws={'$in': wsa})
            cur = self.seen.find(qfind, limit=limit)
            for o in cur:
                ws = wsmap[o['ws']]
                ws.scheduled.put(o)
                ws.add_active(o)
                l.append(o)
            for o in l:
                qup = dict(_id=o['_id'], fp=o['fp'])
                self.seen.update(qup, {'$set':dict(co=1)},
                                 upsert=False, multi=False)
                o['co'] = 1
        finally:
            for ws in group:
                with ws.loadlock:
                    ws.loading = False
        print >>sys.stderr, "WorkSet(%s,%s)._load_group: %d in %.2fs" % \
            (self.job, wsa, len(l), (time.time()-t0))

    def _load(self):
        t0 = time.time()
        l = []
        try:
            qfind = dict(co=0, ws=self.id)
            cur = self.seen.find(qfind, limit=self.SCHEDULE_LOW)
            for o in cur:
                self.scheduled.put(o)
                self.add_active(o)
                l.append(o)
            for o in l:
                qup = dict(_id=o['_id'], fp=o['fp'])
                self.seen.update(qup, {'$set':{'co':1}},
                                 upsert=False, multi=False)
                o['co'] = 1
        finally:
            with self.loadlock: self.loading = False
            # release connection
            db.connection.end_request()
        print >>sys.stderr, "WorkSet(%s,%d)._load: %d in %.2fs" % \
            (self.job, self.id, len(l), (time.time()-t0))

    def unload(self):
        '''discards all CURIs in scheduled queue'''
        self.running = False
        # TODO: support partial unload (spilling), do proper synchronization
        qsize = self.scheduled.qsize()
        print >>sys.stderr, "WS(%d).unload: flushing %d URIs" % \
            (self.id, qsize)
        while qsize > 0:
            try:
                o = self.scheduled.get(block=False)
                qsize -= 1
            except Empty:
                break
        with self.activelock:
            while self.active:
                url, curi = self.active.popitem()
                db.seen.save(curi)

    def schedule(self, curi):
        #print >>sys.stderr, "WorkSet.schedule(%s)" % curi
        curi['ws'] = self.id
        curi['co'] = 0
        if not self.running or self.scheduled.full():
            #print >>sys.stderr, "WS(%d) scheduled is full, saving into database" % (self.id,)
            self.seen.save(curi)
            return False
        else:
            curi['co'] = 1
            self.scheduled.put(curi)
            with self.activelock:
                self.active[self.keyurl(curi['u'])] = curi
            #self.seen.save(curi)
            return True
    
    def _update_seen(self, curi, expire):
        '''updates seen database from curi from finished event.'''
        # curi['u'] is a raw URI, and curi['id'] may or may not exist
        uk = self.urlkey(curi['u'])
        # there's no real need for unsetting 'ws'
        updates = {
            '$set':{
                    'a': curi['a'], 'f': curi['f'], 'e': expire,
                    'u': uk,
                    'co': -1
                    },
            '$unset':{'w':1, 'ws': 1}
            }
        if 'id' in curi:
            flt = {'_id': bson.objectid.ObjectId(curi['id']),
                   'fp': uk['fp']}
        else:
            flt = self.keyquery(uk)
        #print >>sys.stderr, "flt=%s update=%s" % (flt, update)
        self.seen.update(flt,
                         updates,
                         multi=False, upsert=True)
        #print >>sys.stderr, '_update_seen:%s' % e

    def deschedule(self, furi, expire=None):
        '''note: furi['u'] is original raw URI.'''
        finished = furi.get('f')
        if finished is None:
            finished = furi['f'] = int(time.time())
        if expire is None:
            # seen status expires after 2 months
            # TODO: allow custom expiration per job/domain/URI
            expire =  finished + 60 * 24 * 3600
        t0 = time.time()
        # update database with crawl result
        self._update_seen(furi, expire)
        #print >>sys.stderr, "update_seen %f" % (time.time() - t0)
        self.remove_active_uri(furi['u'])
