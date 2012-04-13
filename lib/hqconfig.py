# HQ configuration
import os
from configobj import ConfigObj

from mongojobconfigs import JobConfigs

# HQ installation directory
HQHOME = os.path.abspath(os.path.join(os.path.dirname('__file__'), '..'))

# 255 worksets
NWORKSETS_BITS = 8

def inqdir(job):
    return os.path.join(get('datadir'), job, get('inqdir'))
def seendir(job):
    return os.path.join(get('datadir'), job, get('seendir'))
def worksetdir(job):
    return os.path.join(get('datadir'), job, get('worksetdir'))

ZKHOSTS = ['crawl433.us.archive.org:2181',
           'crawl434.us.archive.org:2181',
           'crawl402.us.archive.org:2181']

DEFAULT_CONFIG = [
    # HQ data directory
    'datadir=/1/crawling/hq',
    'inqdir=inq',
    'seendir=seen',
    'worksetdir=ws',
    'confdir=conf',
    'zkhosts='+','.join(ZKHOSTS),
    'mongo=localhost',
    '[web]',
    'debug=0'
    ]

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
        _configobj = ConfigObj(DEFAULT_CONFIG)
        local_configobj = ConfigObj(os.path.join(HQHOME, get('confdir'),
                                                 'hq.conf'))
        _configobj.merge(local_configobj)
        # overriding config through env-var (meant for testing)
        envconf = os.environ.get('HQCONF')
        if envconf:
            env_configobj = ConfigObj(envconf.splitlines())
            _configobj.merge(env_configobj)
    return _configobj

def get(p, dv=None):
    return configobj().get(p, dv)

# def __getitem__(p):
#     return configobj().get(p, None)
