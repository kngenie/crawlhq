class JobConfig(object):
    """collection of property for the given job"""
    def __init__(self, db, job):
        self.db = db
        self.job = job
    @property
    def name(self):
        return self.job

    def __getitem__(self, k):
        jobconf = self.db.jobconfs.find_one({'name':self.job}, {k: 1})
        return jobconf and jobconf.get(k)

#
class JobConfigs(object):
    def __init__(self, db):
        self.db = db

    def shutdown(self):
        self.db = None

    def get_job(self, job):
        # TODO: manage singleton?
        if not self.job_exists(job):
            raise ValueError('%s: no such job' % job)
        return JobConfig(self.db, job)

    def get_jobconf(self, job, pname, default=None, nocreate=0):
        try:
            jobconf = self.db.jobconfs.find_one({'name':job}, {pname: 1})
            if jobconf is None and not nocreate:
                jobconf = {'name':self.job}
                self.db.jobconfs.save(jobconf)
            return jobconf.get(pname, default)
        except pymongo.errors.OperationFailure as ex:
            raise IOError, str(ex)

    def save_jobconf(self, job, pname, value, nocreate=0):
        try:
            if nocreate:
                self.db.jobconfs.update({'name': job}, {pname: value},
                                        multi=False, upsert=False)
            else:
                self.db.jobconfs.update({'name': job}, {pname: value},
                                        multi=False, upsert=True)
        except pymongo.errors.OperationFailure as ex:
            raise DatabaseError, str(ex)

    def job_exists(self, job):
        try:
            o = self.db.jobconfs.find_one({'name':job}, {'name':1})
            return o is not None
        except pymongo.errors.OperationFailure as ex:
            raise IOError, str(ex)

