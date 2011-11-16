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

import pymongo

from executor import *
import hqconfig
from mongojobconfigs import JobConfigs
from fileinq import IncomingQueue
from filequeue import FileEnqueue

class CrawlJob(object):
    def __init__(self, jobconfig):
        self.jobconfig = jobconfig
        self.enq = FileEnqueue(qdir=hqconfig.inqdir(self.jobconfig.name),
                               suffix=os.getpid(),
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
        self.mongo = pymongo.Connection(hqconfig.get('mongo'))
        self.configdb = self.mongo.crawl
        self.jobconfigs = JobConfigs(self.configdb)
        #self.coordinator = Coordinator(hqconfig.get('zkhosts'))

    def shutdown(self):
        for job in self.jobs.values():
            job.shutdown()
        self.configdb = None
        self.mongo.disconnect()

    def get_job(self, jobname):
        with self.jobslock:
            job = self.jobs.get(jobname)
            if job is None:
                job = self.jobs[jobname] = CrawlJob(
                    self.jobconfigs.get_job(jobname))
                #self.coordinator.publish_job(job)
            return job

hq = Headquarters()
atexit.register(hq.shutdown)

class DiscoveredHandler(object):
    def GET(self, job, action):
        h = getattr(self, 'do_' + action, None)
        if h is None: raise web.notfound(action)
        response = h(job)
        if isinstance(response, dict):
            response = json.dumps(response, check_circular=False,
                                  separators=',:') + "\n"
            web.header('content-type', 'text/json')
        return response

    def POST(self, job, action):
        h = getattr(self, 'post_' + action, None)
        if h is None: raise web.notfound(action)
        response = h(job)
        if isinstance(response, dict):
            response = json.dumps(response, check_circular=False,
                                  separators=',:') + "\n"
            web.header('content-type', 'text/json')
        return response

    def jsonres(self, r):
        web.header('content-type', 'text/json')
        return json.dumps(r, check_circular=False, separators=',:') + "\n"
    
    def decode_content(self, data):
        if web.ctx.env.get('HTTP_CONTENT_ENCODING') == 'gzip':
            ib = StringIO(data)
            zf = GzipFile(fileobj=ib)
            return zf.read()
        else:
            return data

    def post_discovered(self, job):
        '''receives URLs found as 'outlinks' in just downloaded page.
        do_discovered runs already-seen test and then schedule a URL
        for crawling with last-modified and content-hash obtained
        from seen database (if previously crawled)'''
        
        p = web.input(force=0)
        if 'u' not in p:
            return {error:'u value missing'}

        furi = dict(u=p.u)
        for k in ('p', 'v', 'x'):
            if k in p and p[k] is not None: furi[k] = p[k]

        try:
            cj = hq.get_job(job)
        except Exception as ex:
            # TODO: return 404?
            return dict(err=str(ex))
        if p.force:
            return cj.schedule([furi])
        else:
            return cj.discovered([furi])

    def post_mdiscovered(self, job):
        '''receives submission of "discovered" events in batch.
        this version simply queues data submitted in incoming queue
        to minimize response time. entries in the incoming queue
        will be processed by separate processinq call.'''
        result = dict(processed=0)
        data = None
        try:
            data = web.data()
            curis = json.loads(self.decode_content(data))
        except:
            web.debug("json.loads error:data=%s" % data)
            result['error'] = 'invalid data - json parse failed'
            return result
        if isinstance(curis, dict):
            force = curis.get('f')
            curis = curis['u']
        elif isinstance(curis, list):
            force = False
        else:
            result['error'] = 'invalid data - not an array'
            return result
        if len(curis) == 0:
            return result


        try:
            cj = hq.get_job(job)
        except Exception as ex:
            result['error'] = str(ex)
            return result

        start = time.time()
        if force:
            result.update(cj.schedule(curis))
        else:
            result.update(cj.discovered(curis))
        t = time.time() - start
        result.update(t=t)
        if t / len(curis) > 1e-3:
            logging.warn("slow discovered: %.3fs for %d", t, len(curis))
        else:
            logging.debug("mdiscovered %s", result)
        return result

    def do_flush(self, job):
        '''flushes cached objects into database for safe shutdown'''
        hq.get_job(job).flush()
        r = dict(ok=1)
        return r

    def do_testinq(self, job):
        """test method for checking if URL mapping is configured correctly."""
        r = dict(ok=1, job=job, pid=os.getpid())
        return r
            
logging.basicConfig(level=logging.WARN)
urls = (
    '/(.*)/(.*)', 'DiscoveredHandler',
    )
app = web.application(urls, globals())

if __name__ == "__main__":
    try:
        app.run()
    except Exception as ex:
        logging.critical('app.run() terminated by error', exc_info=1)
else:
    application = app.wsgifunc()
