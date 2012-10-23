import sys
import os
import hqconfig
import threading
import json
import logging
import itertools

from jobconfigs import JobConfig

__all__ = ['JobConfigs']

# TODO: move this to engine-independent module
class JobConfigJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, JobConfig):
            return o.job
        else:
            return json.JSONEncoder.default(self, o)
#
class JobConfigs(object):
    def __init__(self, db):
        self.db = db
        self.coll = self.db.jobconfs
        self.cachedir = hqconfig.cachedir()
        self.jobs = None
        self.jobslock = threading.RLock()
        self.cachefn = os.path.join(self.cachedir, 'jobconfigs.json')

    def shutdown(self):
        self.db = None

    def get_status(self):
        s = dict()
        return s

    def _load(self):
        logging.debug('_load');
        if self.db is None:
            raise IOError, 'MongoDB is offline'
        jobs = dict()
        # TODO implement retry on AutoReconnect
        for jobdic in self.coll.find():
            logging.debug('loading job: %s', jobdic)
            if '_id' in jobdic: del jobdic['_id']
            name = jobdic.get('name')
            # discard entry with no name, empty name (and bad name?)...
            if name:
                jobs[name] = JobConfig(self, dict(jobdic))
        self.jobs = jobs

    def _savejob(self, jobname):
        if jobname not in self.jobs:
            raise ValueError, '%s: non-existent job' % jobname
        job = self.jobs[jobname].job
        for i in itertools.count():
            try:
                logging.debug('updating MongoDB for job %s', jobname)
                self.coll.update({'name':jobname},
                                       {'$set': job._updates()})
                return
            except pymongo.errors.OperationFailure as ex:
                logging.warn('update to job %s failed (attempt %d)',
                             jobname, i)
                if i > 5: raise IOError, str(ex)
                time.sleep(3.0)

    def _read_cache(self):
        try:
            with open(self.cachefn) as r:
                jobs = json.loads(r.read())
            # TODO check validity
            self.jobs = jobs
        except IOError, ex:
            if ex.errno in (os.errno.ENOTDIR, os.errno.ENOENT):
                logging.debug('%s: %s', self.cachefn, str(ex))
            else:
                logging.error('%s: %s', self.cachefn, str(ex))
        except ValueError, ex:
            # bad data - log, and discard
            logging.warn('%s: JSON decode  failed: %s', self.cachefn, str(ex))

    def _write_cache(self):
        while 1:
            try:
                with open(self.cachefn, 'w') as w:
                    w.write(json.dumps(self.jobs, separators=',:',
                                       cls=JobConfigJSONEncoder))
                return
            except Exception, ex:
                if isinstance(ex, IOError) and ex.errno in (
                    os.errno.ENOTDIR, os.errno.ENOENT):
                    try:
                        os.makedirs(self.cachedir)
                    except:
                        logging.error('failed to mkdir %s', self.cachedir,
                                      exc_info=1)
                        return
                    continue
                logging.error('failed to write jobconfigs cache %s',
                              self.cachefn, exc_info=1)
                return

    def _get_jobs(self):
        logging.debug('_get_jobs')
        with self.jobslock:
            if self.jobs is None:
                try:
                    self._load()
                    self._write_cache()
                except:
                    logging.warn('failed to load JobConfigs from MongoDB.'
                                 ' reading cached data', exc_info=1)
                    self._read_cache()
        return self.jobs

    def _add_job(self, jobdic):
        if 'name' not in jobdic:
            raise ValueError, '"name" is required for a job'
        with self.jobslock:
            jobs = self._get_jobs()
            if jobdic['name'] in jobs:
                raise ValueError, '%s: already exists' % jobdic['name']
            j = JobConfig(self, jobdic)
            jobname = jobdic['name']
            jobs[jobname] = j
            self._write_cache()
            #self._savejob(jobname)

    def get_alljobs(self):
        return self._get_jobs().values()

    def get_job(self, job):
        jobs = self._get_jobs()
        if job not in jobs:
            raise ValueError, '%s: no such job' % job
        return jobs[job]

    def get_jobconf(self, job, pname, default=None, nocreate=0):
        jobs = self._get_jobs()
        if job not in jobs:
            if nocreate:
                raise ValueError, '%s: no such job' % job
            else:
                self._add_job({'name':job})
                # calling recursively with nocreate=1
                return self.get_jobconf(job, pname, default, 1)
        return jobs[job].get(pname, default)

    def save_jobconf(self, jobname, pname, value, nocreate=0):
        try:
            job = self.get_job(jobname)
        except ValueError:
            if nocreate: raise
            self._add_job({'name':job, pname:value})
            return
        job[pname] = value
        self._savejob(jobname)

    def job_exists(self, job):
        jobs = self._get_jobs()
        return job in jobs

    def reload(self):
        with self.jobslock:
            try:
                self._load()
                self._write_cache()
                return False
            except:
                logging.warn('failed to reload JobConfigs from MongoDB.'
                             ' continue to use existing data', exc_info=1)
                return False
