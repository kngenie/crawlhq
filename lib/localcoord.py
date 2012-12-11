import sys
import os
import logging
import time

"""provides no-op Coordinator for development use.
"""

__all__ = ['Coordinator']

class Coordinator(object):
    def __init__(self, **kwargs):
        self.jobs = {}
    def publish_alive(self):
        pass
    def publish_job(self, job):
        self.jobs[job] = dict(name=job, ts=time.time())
    def publish_client(self, job, client):
        pass
    def get_status_of(self, server=None):
        me = os.uname()[1]
        server = server or me
        if server == me:
            jobs = self.jobs.values()
        else:
            jobs = None
        status = dict(name=server, jobs=jobs)
    def get_servers_status(self):
        server = os.uname()[1]
        return [self.get_status_of(server)]
    def shutdown(self):
        pass
