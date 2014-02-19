import sys
import os
import subprocess
import shutil
import logging

import pytest

# until we depend on nose 100%
libdir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../lib'))
if libdir not in sys.path:
    sys.path.append(libdir)
del libdir

import hqconfig

class TestDatadir(object):
    """
    a fixture for setting up test datadir. it sets test datadir path
    to datadir config parameter, so that all HQ code uses that directory
    for storing application state.
    """
    def __init__(self, dirname='hq', basedir='/tmp'):
        self.path = os.path.join(basedir, dirname)
        if not os.path.isdir(self.path):
            logging.info('Creating %s/', self.path)
            os.makedirs(self.path)
        hqconfig.mergeconfig(['datadir=%s' % self.path])

    def cleanup(self):
        """remove all files/dirs under self.path, leaving self.path itself."""
        logging.debug('Removing %s/*', self.path)
        subprocess.check_call('/bin/rm -rf {}/*'.format(self.path), shell=1)
        # if os.path.isdir(self.path):
        #     for fn in os.listdir(self.path):
        #         p = os.path.join(self.path, fn)
        #         if os.path.isdir(p):
        #             shutil.rmtree(p)
        #         else:
        #             os.remove(p)
    def __del__(self):
        logging.debug('TestDatadir.__del__')
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

    def __str__(self):
        return "{cl.__module__}.{cl.__name__}(path={path})".format(
            cl=self.__class__, path=self.path)

@pytest.fixture
def testdatadir(tmpdir):
    datadir = TestDatadir(dirname=str(tmpdir))
    return datadir

class TestDomainInfo(object):
    def __init__(self, excluded=0):
        self.excluded = 0
    def load(self):
        pass
    def shutdown(self):
        pass
    def get_byurl(self, url):
        return dict(exclude=self.excluded)

@pytest.fixture
def testdomaininfo():
    return TestDomainInfo()
