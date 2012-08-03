import sys
import os
import subprocess

sys.path.append(os.path.join(os.path.dirname(__file__), '../lib'))

import hqconfig

class TestDatadir(object):
    def __init__(self, dirname='hq'):
        self.path = os.path.join('/tmp', dirname)
        if not os.path.isdir(self.path):
            os.makedirs(self.path)
        hqconfig.mergeconfig(['datadir=%s' % self.path])

    def cleanup(self):
        """remove all files/dirs under self.path, leaving self.path itself."""
        subprocess.check_call('/bin/rm -r "%s"/*' % self.path, shell=1)
    def __del__(self):
        if os.path.isdir(self.path):
            self.cleanup()
            os.rmdir(self.path)

    def inqdir(self, job):
        return hqconfig.inqdir(job)

    def wsdir(self, job, wsid=None):
        if wsid is None:
            return hqconfig.worksetdir(job)
        else:
            return os.path.join(hqconfig.worksetdir(job), '%d' % wsid)

