"""
classes for implementing HQ clients.
"""
import sys
import os
import httplib
import json

class HeadquarterClient(object):
    def __init__(self, server):
        self.server = server

class DiscoveredClient(HeadquarterClient):
    def __init__(self, server, jobname):
        super(DiscoveredClient, self).__init__(server)
        self.jobname = jobname
        self.http = None
    def url_discovered(self):
        return 'http://%s/hq/jobs/%s/discovered' % (self.server, self.jobname)
    def url_mdiscovered(self):
        return 'http://%s/hq/jobs/%s/mdiscovered' % (self.server, self.jobname)

    def _init_http(self):
        if self.http is None:
            self.http = httplib.HTTPConnection(self.server)
        return self.http
    def submit_discovered(self, curls):
        reqpath = self.url_mdiscovered()
        headers = {
            'Content-Type': 'text/json'
            }
        self._init_http()
        self.http.request('POST', reqpath,
                          json.dumps(curls, separators=',:'),
                          headers)
        r = self.http.getresponse()
        data = r.read()
        r.close()

        if r.status != 200:
            raise IOError, '%s %s (%r)' % (r.status, r.reason, data)
        return data

    def batch_submit_discovered(self, iterable, batchsize=500):
        batch = []
        for curl in iterable:
            batch.append(curl)
            if len(batch) >= batchsize:
                self.submit_discovered(batch)
                batch = []
        if len(batch) > 0:
            self.submit_discovered(batch)
            
    def flush(self):
        """flushes incoming queue.
        """
        pass

    
