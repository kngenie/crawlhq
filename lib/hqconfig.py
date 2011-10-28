# HQ configuration
import os

HQ_HOME = '/1/crawling/hq'

INQDIR = os.path.join(HQ_HOME, 'inq')
SEENDIR = os.path.join(HQ_HOME, 'seen')
WORKSETDIR = os.path.join(HQ_HOME, 'ws')

# 255 worksets
NWORKSETS_BITS = 8

def inqdir(job):
    return os.path.join(INQDIR, job)
def seendir(job):
    return os.path.join(SEENDIR, job)
def worksetdir(job):
    return os.path.join(WORKSETDIR, job)

ZKHOSTS = ','.join(['crawl433.us.archive.org:2181',
                    'crawl434.us.archive.org:2181',
                    'crawl402.us.archive.org:2181'])

__jobconfig = None
def jobconfig():
    global __jobconfig
    if __jobconfig is None:
        __jobconfig = JobConfigs()
    return __jobconfig

_configobj = None
def configobj():
    global _configobj
    if _configobj is None:
        _configobj = ConfigObj('/opt/hq/conf/hq.conf')
    return _configobj

def get(p, dv=None):
    return configobj().get(p, dv)
