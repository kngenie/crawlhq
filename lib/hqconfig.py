"""
HQ configuration support.

configuration parameters are loaded with :module:`ConfigObj` from
ini-file style configuration file ``hq.conf`` in a directory where
HQ is deployed (detected by heuristics)

Configuration parameters can be accessed with :func:`get`:

    import hqconfig

    some_path = hqconfig.get("some_path")

Module also defines dedicated functions for commonly-used parameters:

* ``inqdir``
* ``seendir``
* ``worksetdir``
* ``cachedir``
"""
import os
from configobj import ConfigObj

def _find_home():
    """Deduce HQ installation directory.
    This assumes hqconfig.py is under ``HQHOME/lib`` or ``HQHOME/local/lib``.
    """
    d = os.path.dirname(__file__)
    while not d.endswith('/lib'):
        d = os.path.dirname(d)
    d = os.path.dirname(d)
    if d.endswith('/local'):
        d = os.path.dirname(d)
    return d
HQHOME = _find_home()

# 255 worksets
NWORKSETS_BITS = 8

def inqdir(job):
    return os.path.join(get('datadir'), job, get('inqdir'))
def seendir(job):
    return os.path.join(get('datadir'), job, get('seendir'))
def worksetdir(job):
    return os.path.join(get('datadir'), job, get('worksetdir'))

def cachedir():
    return os.path.join(get('datadir'), get('cachedir'))

ZKHOSTS = ['localhost:2181']

DEFAULT_CONFIG = [
    # HQ data directory
    'datadir=/1/crawling/hq',
    'inqdir=inq',
    'seendir=seen',
    'worksetdir=ws',
    'cachedir=cache',
    'confdir=conf',
    'zkhosts='+','.join(ZKHOSTS),
    'mongo=localhost',
    '[web]',
    'debug=0'
    ]

def confdir():
    return os.path.join(HQHOME, get('confdir'))

_configobj = None
def configobj():
    global _configobj
    if _configobj is None:
        _configobj = ConfigObj(DEFAULT_CONFIG)
        localconfpath = os.path.join(confdir(), 'hq.conf')
        local_configobj = ConfigObj(localconfpath)
        _configobj.merge(local_configobj)
        # overriding config through env-var (meant for testing)
        envconf = os.environ.get('HQCONF')
        if envconf:
            env_configobj = ConfigObj(envconf.splitlines())
            _configobj.merge(env_configobj)
    return _configobj

def mergeconfig(config):
    global _configobj
    if isinstance(config, basestring):
        config = ConfigObj(config.splitlines())
    elif isinstance(config, list):
        config = ConfigObj(config)
    elif not isinstance(config, ConfigObj):
        raise ValueError, 'config must be a ConfigObj'
    configobj().merge(config)

def get(p, dv=None, type=None):
    """Return configuration parameter named ``p`` as type `type`

    :param p: configuration parameter name
    :param dv: default value if `p` is undefined
    :param type: a function coverting parameter value \
    (not applied to `dv`)
    """
    if isinstance(p, basestring):
        p = p.split('.')
    if isinstance(p, (list, tuple)):
        m = configobj()
        for e in p:
            if not hasattr(m, 'get'): return dv
            m = m.get(e)
            if m is None: return dv
        if type:
            try:
                m = type(m)
            except:
                m = dv
        return m
    raise KeyError, 'bad key: %s' % p

def getint(p, dv=None):
    """Return configuration parameter named `p` as int

    :param p: parameter name
    :param dv: default value
    :type dv: int
    """
    return get(p, dv, type=int)
def getfloat(p, dv=None):
    """Return configuration parameter named `p` as int

    :param p: parameter name
    :param dv: default value
    :type dv: float
    """
    return get(p, dv, type=float)

import factory
factory_script = os.path.join(confdir(), 'factory.py')
if os.path.isfile(factory_script):
    execfile(factory_script, globals(), factory)

# def __getitem__(p):
#     return configobj().get(p, None)
