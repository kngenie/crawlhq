import hqconfig
import logging

# decorator
class SingletonDecorator(object):
    def __init__(self):
        self.__objects = {}
    def __call__(self, f):
        def get_or_create():
            o = self.__objects.get(f)
            if o is None:
                o = f()
                self.__objects[f] = o
            return o
        return get_or_create
singleton = SingletonDecorator()

@singleton
def mongo():
    import pymongo
    mongoserver = hqconfig.get('mongo')
    logging.warn('using MongoDB: %s', mongoserver)
    return pymongo.Connection(mongoserver)

@singleton
def configdb():
    return mongo().crawl

@singleton
def coordinator():
    from zkcoord import Coordinator
    return Coordinator(hqconfig.get('zkhosts'))

@singleton
def jobconfigs():
    from mongojobconfigs import JobConfigs
    return JobConfigs(configdb())

@singleton
def domaininfo():
    from mongodomaininfo import DomainInfo
    return DomainInfo(configdb())

@singleton
def crawlinfo():
    from mongocrawlinfo import CrawlInfo
    # CrawlInfo database is named 'wide' for historical reasons.
    return CrawlInfo(configdb(), 'wide')

@singleton
def seenfactory():
    from seen import SeenFactory
    return SeenFactory()
