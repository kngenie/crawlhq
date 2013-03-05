#!/usr/bin/python
"""web app process dedicated to receiving discovered data
discovered is the highest traffic end point. putting this endpoint
in separate processes, would avoid dragging seen check processing
with GIL.
"""
import sys
import os
import json
import time
import re
import web
import logging
import atexit
import threading

#import pymongo

from executor import *
import hqconfig
#from mongojobconfigs import JobConfigs
#from filequeue import FileEnqueue
from priorityqueue import *
from filequeue import DummyFileDequeue
from weblib import QueryApp
from handlers import DiscoveredHandler

class CrawlJob(object):
    def __init__(self, jobconfig, maxqueuesize=4*1000*1000):
        self.jobconfig = jobconfig
        qdir = hqconfig.inqdir(self.jobconfig.name)
        self.enq = PriorityEnqueue(qdir=qdir,
                               suffix=os.getpid(),
                               maxsize=maxqueuesize,
                               buffer=1000,
                               executor=None,
                               gzip=9)
        self.deq = PriorityDequeue(qdir=qdir, enq=self.enq,
                                   deqfactory=DummyFileDequeue)

    @property
    def name(self):
        return self.jobconfig.name

    def get_status(self):
        r = dict(
            enq=self.enq.get_status(),
            deq=self.deq.get_status()
            )
        return r

    def discovered(self, curis):
        self.enq.queue(curis)
        return dict(processed=len(curis))

    def shutdown(self):
        self.flush()

    def flush(self):
        self.enq._flush()
        self.enq.close()

    def flush_starved(self):
        logging.debug('job %r flush_starved()', self.name)
        self.deq.pull()

class Headquarters(object):
    """mini Headquarters object with just one incoming queue"""
    def __init__(self):
        self.jobs = {}
        self.jobslock = threading.RLock()
        self.jobconfigs = hqconfig.factory.jobconfigs() #JobConfigs(self.configdb)
        #self.coordinator = Coordinator(hqconfig.get('zkhosts'))
        self.maxinqueuesize = hqconfig.getint('inq.maxqueuesize', 50)
        self.pullinterval = hqconfig.getfloat('inq.pullinterval', 5.0)

        self.__start_pull_thread()

    def shutdown(self):
        for job in self.jobs.values():
            job.shutdown()
        self.jobconfigs = None

    def get_job(self, jobname):
        with self.jobslock:
            job = self.jobs.get(jobname)
            if job is None:
                # too small maxqueuesize would be a problem, so config
                # parameter is in MB.
                job = self.jobs[jobname] = CrawlJob(
                    self.jobconfigs.get_job(jobname),
                    maxqueuesize=int(max(1, self.maxinqueuesize)*1000*1000))
                #self.coordinator.publish_job(job)
            return job

    def __start_pull_thread(self):
        th = threading.Thread(name='inq-puller', target=self.__pull)
        th.daemon = True
        th.start()

    def __pull(self):
        try:
            while 1:
                time.sleep(self.pullinterval)
                jobs = self.jobs.values()
                for job in jobs:
                    job.flush_starved()
        except:
            logging.warn('inq-puller thread existing with error', exc_info=1)

hq = Headquarters()
atexit.register(hq.shutdown)

class ClientAPI(QueryApp, DiscoveredHandler):

    def __init__(self):
        self.hq = hq

    # overriding QueryApp.{GET,POST} because argument order is different
    def GET(self, job, action):
        return self._dispatch('do_', action, job)
    def POST(self, job, action):
        return self._dispatch('post_', action, job)

    def do_testinq(self, job):
        """test method for checking if URL mapping is configured correctly."""
        env = web.ctx.env
        r = dict(ok=1, job=job, pid=os.getpid())
        r['env'] = dict((k, web.ctx.env.get(k)) for k in (
                'SCRIPT_NAME', 'PATH_TRANSLATED', 'SCRIPT_FILENAME',
                'SCRIPT_URI', 'REQUEST_URI'))
        return r

    def do_status(self, job):
        try:
            cjob = self.hq.get_job(job)
            st = cjob.get_status()
            return dict(ok=1, job=job, pid=os.getpid(), r=st)
        except Exception, ex:
            logging.warn('job %s: get_status failed', exc_info=1)
            return dict(ok=0, job=job, pid=os.getpid(), error=str(ex))

logging.basicConfig(level=logging.WARN)
urls = (
    '/(.*)/(.*)', 'ClientAPI',
    )
app = web.application(urls, globals())

if __name__ == "__main__":
    try:
        app.run()
    except Exception as ex:
        logging.critical('app.run() terminated by error', exc_info=1)
else:
    application = app.wsgifunc()
