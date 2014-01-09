import hqconfig
# because of this import, this module cannot be named "fixture.jobconfigs".
import jobconfigs
import logging

class TestJobConfigs(object):
    JOBS = ('wide',)
    def shutdown(self):
        pass
    def get_job(self, job):
        if job not in self.JOBS:
            raise Exception('{}: no such job'.format(job))
        logging.debug('jobconfigs=%s', jobconfigs)
        return jobconfigs.JobConfig(self, dict(name=job))
    def get_jobconf(self, job, pname, default=None, nocreate=0):
        return default
    def save_jobconf(self, jobname, pname, value, nocreate=0):
        pass
    def job_exists(self, job):
        return True
    def reload(self):
        pass

hqconfig.factory.jobconfigs = TestJobConfigs
