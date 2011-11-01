class OpenQueueQuota(object):
    def __init__(self, maxopens=256):
        self.maxopens = maxopens
        self.lock = threading.Condition()
        self.opens = set()

    def opening(self, fileq):
        logging.debug("opening %s", fileq)
        with self.lock:
            logging.debug("opening:locked")
            while len(self.opens) >= self.maxopens:
                for q in list(self.opens):
                    logging.debug("  try closing %s", q)
                    if q.detach():
                        break
                # if everybody's busy (unlikely), keep trying in busy loop
                # sounds bad...
            self.opens.add(fileq)
        logging.debug("opening:released")

    def closed(self, fileq):
        logging.debug("closed %s", fileq)
        with self.lock:
            logging.debug("closed:locked")
            self.opens.discard(fileq)
            #self.lock.notify()
        logging.debug("closed:released")
            
class HashSplitIncomingQueue(IncomingQueue):
    def init_queues(self, window_bits=54, buffsize=0, maxsize=200*1000*1000,
                    maxopens=256):
        self.opener = OpenQueueQuota(maxopens=maxopens)
        if window_bits < 1 or window_bits > 63:
            raise ValueError, 'window_bits must be 1..63'
        self.window_bits = window_bits

        self.nwindows = (1 << 64 - self.window_bits)
        self.win_mask = self.nwindows - 1
                               
        maxsize = maxsize / self.nwindows

        # dequeue side
        self.rqfile = FileDequeue(self.qdir)

        # queues for each sub-range
        self.write_executor = ThreadPoolExecutor(poolsize=2, queuesize=10)
        self.qfiles = [FileEnqueue(self.qdir, maxsize=maxsize,
                                   suffix=str(i), opener=opener,
                                   buffer=buffsize,
                                   executor=self.write_executor)
                       for i in range(self.nwindows)]

    def get_status(self):
        s = IncomingQueue.get_status()
        s.update(writequeue=self.write_executor.work_queue.qsize())
        return s

    @property
    def maxopens(self):
        return self.opener.maxopens
    @maxopens.setter
    def maxopens(self, v):
        self.opener.maxopens = v

    PARAMS = [('buffsize', int), ('maxopens', int)]

    def hash(self, curi):
        if 'id' in curi:
            return curi['id']
        else:
            h = urlhash.urikey(curi['u'])
            curi['id'] = h
            return h

    def queue_dispatch(self, curi):
        h = self.hash(curi)
        win = (h >> self.window_bits) & self.win_mask
        return win

    def add(self, curis):
        processed = 0
        for curi in curis:
            win = self.queue_dispatch(curi)
            enq = self.qfiles[win]
            enq.queue(curi)
            self.addedcount += 1
            processed += 1
        return dict(processed=processed)
