import sys
import os
import re

import hqconfig
from dispatcher import WorksetWriter
from filequeue import FileEnqueue, FileDequeue

class DivertQueue(object):
    """Workset compatible class for storing URIs for delivery to external
    services (including other HQ)."""
    def __init__(self, basedir, name, bufsize=500):
        self.name = name
        self.qdir = os.path.join(basedir, str(name))
        FileEnqueue.recover(self.qdir)
        self.enq = FileEnqueue(self.qdir, buffer=bufsize, suffix='d')

        self.queuedcount = 0

    def flush(self):
        self.enq._flush()
        return self.enq.close()

    def shutdown(self):
        self.flush()

    def get_status(self):
        r = dict(name=self.name, queued=self.queuedcount)
        return r

    def schedule(self, curi):
        self.enq.queue(curi)
        self.queuedcount += 1

    def listqfiles(self):
        try:
            fns = os.listdir(self.qdir)
            qfiles = []
            for fn in fns:
                if '0' <= fn[0] <= '9' and not fn.endswith('.open'):
                    qfiles.append(os.path.abspath(os.path.join(self.qdir, fn)))
            return qfiles
        except:
            return []

class Diverter(object):
    def __init__(self, job, mapper):
        self.jobname = job
        self.mapper = map
        self.basedir = os.path.join(hqconfig.HQ_HOME, 'div', self.jobname)
        if not os.path.isdir(self.basedir):
            os.makedirs(self.basedir)

        self.queues = {}
        for fn in os.listdir(self.basedir):
            self.queues[fn] = DivertQueue(self.basedir, fn)

    def hasdata(self, qname):
        q = self.queues.get(qname)
        return q and q.hasdata()

    def flush(self):
        r = []
        for name, q in self.queues.items():
            r.append((name, q.flush()))
        return r
    def shutdown(self):
        for q in self.queues.values():
            q.shutdown()

    def get_status(self):
        r = dict(queues=[(name, q.get_status())
                         for name, q in self.queues.items()])
        return r

    def getqueue(self, name):
        if name not in self.queues:
            self.queues[name] = DivertQueue(self.basedir, name)
        return self.queues[name]

    def divert(self, names, curi):
        if isinstance(names, (list, tuple)):
            for name in names:
                q = self.getqueue(name)
                if q: q.schedule(curi)
        else:
            q = self.getqueue(names)
            if q: q.schedule(curi)

    def listqfiles(self, name):
        """returns a list of absolute path for each qfile in divert queue
        `name'. note this method first flushes the current qfile (if any)."""
        q = self.getqueue(name)
        if q:
            q.flush()
            return q.listqfiles()
        else:
            return []


            
            
    
