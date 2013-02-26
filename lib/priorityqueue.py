import os
import sys
import re
import threading
import errno
from filequeue import FileEnqueue, FileDequeue
from collections import defaultdict

# TODO: priority method is HQ-specific.

class PriorityEnqueue(object):
    """FileEnqueue compatible object that enqueue URIs into multiple
    queues based on its priority.
    priority(curi) method shall return non-negative integer, 0 for highest
    priority.
    """
    def __init__(self, qdir, suffix=None, maxsize=4*1000*1000,
                 buffer=1000, executor=None, gzip=9):
        self.qdir = qdir
        self.__maxqueuesize = maxsize
        self.queues = {}
        self.__queueslock = threading.RLock()
        self.__queueargs = dict(
            suffix=suffix, maxsize=maxsize, buffer=buffer,
            executor=executor, gzip=gzip)
        
    def get_status(self):
        r = dict(
            queues=[(n, q.get_status()) for n, q in self.queues.items()]
            )
        return r

    def _new_queue(self, n):
        qdir = os.path.join(self.qdir, str(n))
        if not os.path.isdir(qdir):
            os.makedirs(qdir)
        return FileEnqueue(qdir=qdir, **self.__queueargs)

    def _get_queue(self, n):
        with self.__queueslock:
            if n not in self.queues:
                self.queues[n] = self._new_queue(n)
            q = self.queues[n]
            return q
    
    def queue(self, curis):
        if not isinstance(curis, (list, tuple)):
            curis = [curis]
        prio = defaultdict(list)
        for curi in curis:
            n = self.priority(curi)
            prio[n].append(curi)
        for n, l in prio.iteritems():
            q = self._get_queue(n)
            q.queue(l)

    def _flush(self):
        """flushes all queues."""
        for q in self.queues.values():
            q._flush()

    def close(self):
        for q in self.queues.values():
            q._flush()
            q.close()
    
    def priority(self, curi):
        path = curi.get('p')
        if not path:
            return 0
        last = path[-1]
        if last == 'R':
            return 1
        if last == 'I':
            return 2
        if last == 'E':
            return 2
        return 3

class PriorityDequeue(object):
    """FileDequeue compatible object for reading queues created by
    PriorityEnqueue.
    """
    def __init__(self, qdir, **kwargs):
        self.queues = {}
        self.__queueslock = threading.RLock()
        self.qdir = qdir
        # qdir must be passed in addition to these args
        self.__queueargs = dict(kwargs)
        self.__curqueue = None # queue priority number
        self.__curdispensed = 0
        self.__dequeuecount = 0

    def _new_queue(self, n):
        qdir = os.path.join(self.qdir, str(n))
        # mkdirs not necessary - assumes qdir always exists
        return FileDequeue(qdir=qdir, **self.__queueargs)

    def _update_queues(self):
        """check qdir and update the list of queues.
        return True is self.queues was modififed.
        """
        modified = False
        with self.__queueslock:
            try:
                fns = os.listdir(self.qdir)
            except OSError, ex:
                if ex.errno != errno.ENOENT:
                    logging.warn('listdir failed on %s: %s', self.qdir, ex)
                return modified
            for fn in fns:
                if re.match(r'\d+$', fn) and \
                        int(fn) not in self.queues and \
                        os.path.isdir(os.path.join(self.qdir, fn)):
                    self.queues[int(fn)] = self._new_queue(fn)
                    modified = True
        return modified

    def get_status(self):
        queues = []
        for n, q in self.queues.items():
            s = q.get_status()
            s['name'] = str(n)
            queues.append(s)
        queuefilecount = sum(q.get('queuefilecount', 0) for q in queues)
        r = dict(queues=queues,
                 queuefilecount=queuefilecount,
                 dequeuecount=self.__dequeuecount,
                 curdispensed=self.__curdispensed,
                 curqueue=self.__curqueue)
        return r

    def close(self):
        with self.__queueslock:
            for q in self.queues.values():
                q.close()

    def qfile_count(self):
        return sum(q.qfile_count() for q in self.queues.values())

    def qfiles_available(self, qfiles):
        # TODO - priority 0 (the highest) would be appropriate for
        # "taken-back" queue files
        with self.__queueslock:
            if 0 not in self.queues:
                self.queues[0] = self._new_queue(0)
        q = self.queues[0]
        q.qfiles_available(qfiles)

    # this is for preventing lower priority queues from blocking higher
    # priority ones indefinitely. higher priority queues still take
    # complete precedence over lower ones.
    MAX_DISPENSE = 10000

    def _get_newqueue(self, timeout=0.0, scan=True):
        if scan:
            self._update_queues()
        with self.__queueslock:
            prios = self.queues.keys()
        if prios:
            prios.sort()
            for p in prios:
                q = self.queues[p]
                u = q.get(timeout=timeout)
                if u:
                    self.__curqueue = p
                    self.__curdispensed = 1
                    self.__dequeuecount += 1
                    return u
        return None
        
    def get(self, timeout=0.0):
        """read out URI in accordance with their priority.
        current algorithm sticks to the queue picked as long as
        it has URIs, up to set maximum count.
        """
        if self.__curqueue is None:
            return self._get_newqueue(timeout=timeout)
        else:
            if self.__curdispensed < self.MAX_DISPENSE:
                q = self.queues[self.__curqueue]
                u = q.get(timeout=timeout)
                if u:
                    self.__curdispensed += 1
                    self.__dequeuecount += 1
                    return u
            self.__curqueue = None
            return self._get_newqueue(timeout=timeout)
