import sys
import os
import hqconfig
import threading
import json
import logging
import itertools

"""
implementation agnostic job configuration classes.
"""

__all__ = ['JobConfig', 'JobConfigs']

class JobConfig(object):
    """collection of property for the given job"""
    def __init__(self, jobconfigs, jobdic):
        self.db = jobconfigs
        self.job = jobdic
    @property
    def name(self):
        return self.job['name']
    def __getitem__(self, k):
        return self.job[k]
    def __setitem__(self, k, v):
        self.job[k] = v
    def get(self, k, v):
        return self.job.get(k, v)
    def _updates(self):
        """return dic for updating backend. will return
        those fields modified since last save. for use by JobConfigs."""
        return dict(self.job)
    def ws_client_map(self):
        return self.job.get('wscl')

class JobConfigs(object):
    def __init__(self, coordinator=None):
        self.coordinator = coordinator
        if self.coordinator:
            self.coordinator.add_listener('jobschanged', self.__jobschanged)
    def __jobschanged(self):
        self.reload()

    def shutdown(self):
        pass
    def get_job(self, job):
        pass
    def get_jobconf(self, job, pname, default=None, nocreate=0):
        pass
    def job_exists(self, job):
        pass
    def create_job(self, name, nworksets=None, ncrawlers=None):
        pass
    def delete_job(self, name):
        pass
    def reaload(self):
        """reload job configs from persistent storage if implementation
        does some sort of caching.
        """
        pass
    def add_job_server(self, job, server):
        if not self.coordinator:
            raise RuntimeError, 'coordinator is not available'
        self.coordinator.add_job_server(job, server)

    def delete_job_server(self, job, server):
        if not self.coordinator:
            raise RuntimeError, 'coordinator is not available'
        j = self.coordinator.get_server_job(server, job)
        if j['ts'] > time.time() - 24*3600:
            # TODO define an exception type
            raise RuntimeError, ('server %r is still active in job %r' % (
                server, job))
        self.coordinator.delete_job_server(job, server)

