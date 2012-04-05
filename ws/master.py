"""Headquarter master console appliation.
"""
import sys
import os
import web
import pymongo
import json
import time
import re
import atexit
import logging
from threading import RLock, Condition, Thread
import httplib
import collections

import hqconfig
#from mongocrawlinfo import CrawlInfo
from zkcoord import Coordinator
from configobj import ConfigObj
from weblib import BaseApp, QueryApp

from mongojobconfigs import JobConfigs
from dispatcher import WorksetMapper

class ServerChannel(object):
    QUEUE_SIZE = 500
    def __init__(self, host, job):
        self.host = host
        # mmm, can we share ServerChannel among multiple jobs?
        self.job = job
        self.discovered_buffer = []
        self.discovered_lock = RLock()
        self.discovered_canget = Condition(self.discovered_lock)
        self.discovered_canput = Condition(self.discovered_lock)

        self.sleeper = Condition()

        self.http = httplib.HTTPConnection(self.host)

        self.running = True
        self.flushing = False
        self.flusher = Thread(target=self._flusher_main)
        self.flusher.start()

    def shutdown(self):
        self.running = False
        self.sleeper.notify()

    def get_status(self):
        r = dict(host=self.host, job=self.job,
                 bufferedcount=len(self.discovered_buffer),
                 running=self.running,
                 flushing=self.flushing)
                                   
    def _flusher_main(self):
        while self.running:
            with self.discovered_lock:
                if len(self.discovered_buffer) < self.QUEUE_SIZE:
                    self.discovered_canget.wait(1.0)
                if len(self.discovered_buffer) == 0:
                    continue
                b = self.discovered_buffer
                self.discovered_buffer = []
            try:
                self.flushing = True
                self._flush(b)
            finally:
                self.flushing = False
        with self.discovered_lock:
            if len(self.discovered_buffer) > 0:
                logging.error('flushter terminating with URIs in the buffer')
                logging.error(json.dumps(self.discovered_buffer))
            
    def _flush(self, b):
        body = json.dumps(b)
        while self.running:
            self.http.request('POST', '/hq/jobs/%s/mdiscovered' % self.job,
                              body)
            try:
                res = self.http.getresponse()
                if res.status == 200: break
                logging.warn('%s:mdiscovered failed with %s %s',
                             self.host, res.status, res.reason)
            except:
                logging.warn('%s:mdiscovered failed', self.host, exc_info=1)
            # put URIs back into the buffer
            with self.discovered_lock:
                self.discovered_buffer.extend(b)
            logging.warn('retrying after 1 minute')
            self.sleeper.wait(60.0)
            
    def discovered(self, curi):
        with self.discovered_lock:
            if len(self.discovered_buffer) >= self.QUEUE_SIZE:
                self.discovered_canput.wait()
            self.discovered_buffer.append(curi)
            if len(self.discovered_buffer) >= self.QUEUE_SIZE:
                self.discovered_canget.notify()
        
class Distributor(object):
    def __init__(self, jobconfig, worksetmapper):
        self.jobconfig = jobconfig
        self.wscl = self.jobconfig['wscl']
        self.mapper = worksetmapper

        # static config
        server_clients = {
            'crawl451': [0, 1, 2, 3, 4],
            'crawl452': [5, 6, 7, 8, 9],
            'crawl453': [10, 11, 12, 13, 14],
            'crawl454': [15, 16, 17, 18, 19],
            'crawl455': [20, 21, 22, 23, 24]
            }
        self.client_server = {}
        self.channels = []
        for s, cs in server_clients.items():
            ch = ServerChannel(s, self.jobconfig.name)
            self.channels.append(ch)
            for c in cs:
                self.client_server[str(c)] = ch
            
    def get_server_channel(self, curi):
        wsid = self.mapper.workset(curi)
        client = self.wscl[wsid]
        channel = self.client_server[str(client)]
        return channel
        
    def discovered(self, curis):
        for curi in curis:
            ch = self.get_server_channel(curi)
            ch.discovered(curi)

class QuarterMaster(object):
    def __init__(self):
        zkhosts = hqconfig.get('zkhosts', None)
        logging.warn('zkhosts=%s', zkhosts)
        self.coord = Coordinator(zkhosts, alivenode='master') if zkhosts else None
        self.mongo = pymongo.Connection(host=hqconfig.get('mongo'))
        self.jobconfigs = JobConfigs(self.mongo.crawl)

        # crawlinfo is historically named 'wide' but not really wide crawl
        # specific.
        #self.crawlinfo = CrawlInfo('wide')
        
        self.worksetmapper = WorksetMapper(hqconfig.NWORKSETS_BITS)
        # distributor for each job
        self.distributors = {}

    def shutdown(self):
        self.coord.shutdown()
        #self.crawlinfo.shutdown()

    @property
    def servers(self):
        return self.coord and self.coord.get_servers()
    @property
    def servers_status(self):
        return self.coord and self.coord.get_servers_status()

    def get_distributor(self, job):
        if job not in self.distributors:
            self.distributors[job] = Distributor(self.jobconfigs.get_job(job),
                                                 self.worksetmapper)
        return self.distributors[job]
        
master = QuarterMaster()
atexit.register(master.shutdown)

class QuarterMasterApp(BaseApp):
    """web application for monitoring everything in the cluster.
    """
    def GET(self):
        return self.render('master', master)
        
class Query(QueryApp):
    def do_serverstatus(self):
        return dict(r=master.servers_status)
    def do_jobstatus(self):
        status = master.servers_status
        # flip structure - we may swap serverstatus and jobstatus in near
        # future since job-centric view is more intuitive for users.
        jobs = collections.defaultdict(list)
        for s in status:
            server_jobs = s.pop('jobs', [])
            for j in server_jobs:
                jobs[j['name']].append(s)
        return dict(r=[dict(name=k, servers=v) for k, v in jobs.items()])

class JobActions(QueryApp):
    def GET(self, job, action):
        return self._dispatch('do_'+action, job)
    def POST(self, job, action):
        return self._dispatch('post_'+action, job)

    def post_mdiscovered(self, job):
        data = None
        result = dict(processed=0)
        try:
            data = web.data()
            curis = json.loads(self.decode_content(data))
        except:
            web.debug("JSON decode error:data=%s" % self.decode_content(data))
            result.update(error='invalid data - json parse failed')
            return result
        if isinstance(curis, dict):
            force = curis.get('f')
            curis = curis['u']
        elif isinstance(curis, list):
            force = False
        else:
            result.update(error='invalid data - neither an array nor object')
            return result

        if len(curis) == 0:
            return result

        dist = master.get_distributor(job)
        dist.discovered(curis)
        result.update(processed=len(curis))

        return result

urls = (
    '/', 'QuarterMasterApp',
    '/q/(.*)', 'Query',
    '/jobs/(.+?)/(.+)', 'JobActions'
    )
web.config.debug = True
app = web.application(urls, globals())
application = app.wsgifunc()
