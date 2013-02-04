#
"""
queue that writes with multiple FileEnqueues.
there's no Dequeue because queue can be read by ordinary FileDequeue.
"""

import sys
import os
from Queue import LifoQueue
import time

from filequeue import FileEnqueue
from executor import *

class PooledEnqueue(object):
    def __init__(self, qdir, n=5, maxsize=1000*1000*1000, **qargs):
        maxsize = maxsize / n
        self.qdir = qdir
        self.write_executor = ThreadPoolExecutor(poolsize=1, queuesize=100)
        self.queues = [FileEnqueue(self.qdir, suffix=str(i),
                                   maxsize=maxsize,
                                   executor=self.write_executor,
                                   **qargs)
                       for i in range(n)]
        self.avail = LifoQueue()
        for q in self.queues:
            self.avail.put(q)
        self.addedcount = 0

    def get_status(self):
        qq = [q.get_status() for q in self.queues]
        r = dict(
            buffered=sum(s['buffered'] for s in qq),
            pending=sum(s['pending'] for s in qq),
            queues=qq)
        return r

    def _flush(self):
        for q in self.queues:
            q._flush()

    def close(self):
        for q in self.queues:
            q.close()
        self.write_executor.shutdown()

    def queue(self, curis):
        t0 = time.time()
        enq = self.avail.get()
        t = time.time() - t0
        if t > 0.1:
            logging.warn('self.avail.get() %.4f', t)
        try:
            enq.queue(curis)
            self.addedcount += len(curis)
        finally:
            t0 = time.time()
            self.avail.put(enq)
            t = time.time() - t0
            if t > 0.1:
                logging.warn('slow self.avail.put() %.4f', t)
