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
from priorityqueue import PriorityEnqueue
from weblib import QueryApp
from handlers import DiscoveredHandler

class CrawlJob(object):
    def __init__(self, jobconfig, maxqueuesize=4*1000*1000):
        self.jobconfig = jobconfig
        self.enq = PriorityEnqueue(qdir=hqconfig.inqdir(self.jobconfig.name),
                               suffix=os.getpid(),
                               maxsize=maxqueuesize,
                               buffer=1000,
                               executor=None,
                               gzip=9)

    def discovered(self, curis):
        self.enq.queue(curis)
        return dict(processed=len(curis))

    def shutdown(self):
        self.flush()

    def flush(self):
        self.enq._flush()
        self.enq.close()

class Headquarters(object):
    """mini Headquarters object with just one incomng queue"""
    def __init__(self):
        self.jobs = {}
        self.jobslock = threading.RLock()
        #self.mongo = pymongo.Connection(hqconfig.get('mongo'))
        #self.configdb = self.mongo.crawl
        self.jobconfigs = hqconfig.factory.jobconfigs() #JobConfigs(self.configdb)
        #self.coordinator = Coordinator(hqconfig.get('zkhosts'))
        self.maxinqueuesize = hqconfig.get(('inq', 'maxqueuesize'), 4)

    def shutdown(self):
        for job in self.jobs.values():
            job.shutdown()
        self.jobconfigs = None
        #self.configdb = None
        #self.mongo.disconnect()

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
