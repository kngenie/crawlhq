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

class JobConfigs(object):
    def shutdown(self):
        pass
    def get_job(self, job):
        pass
    def get_jobconf(self, job, pname, default=None, nocreate=0):
        pass
    def job_exists(self, job):
        pass

