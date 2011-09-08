#!
# MongoDB implementation of domain info database
import pymongo
import logging

class DomainInfo(object):
    '''DomainInfo backed by MongoDB. each domain info is stored as
       a document in crawl.domain collection whose _id is domain name'''
    def __init__(self, db='crawl'):
        self.mongo = pymongo.Connection()
        self.db = self.mongo[db]
        self.coll = self.db.domain

        self.root = dict()
        self.load()

    def shutdown(self):
        self.coll = None
        self.db = None
        self.mongo.disconnect()

    def __del__(self):
        if self.db:
            self.shutdown()

    def load(self):
        '''cache all data into memory'''
        root = dict()
        try:
            it = self.coll.find()
            for d in it:
                domain = str(d['_id'])
                dc = domain.split('.')
                dc.reverse()
                h = root
                for c in dc:
                    if c == '': continue
                    h1 = h.get(c)
                    if h1 is None:
                        h1 = dict()
                        h[c] = h1
                    h = h1
                # ignore domain info for "all"
                if h != root:
                    dd = dict(d)
                    dd.pop('_id', None)
                    h['.'] = dd
        except:
            logging.error('DomainInfo.load failed', exc_info=1)
        self.root = root
            
    def get(self, host):
        ch = host.split('.')
        ch.reverse()
        h = self.root
        hl = None
        for c in ch:
            hn = h.get(c)
            if hn is None: break
            h = hn
            if '.' in h: hl = h['.']
        return hl

if __name__ == '__main__':
    o = DomainInfo(db='crawltest')
    o.coll.remove()
    o.coll.insert(dict(_id='archive.org', exclude=1))
    o.coll.insert(dict(_id='techcrunch.com', exclude=1))

    o.load()
    print o.root

    assert(o.get('org') is None)
    print 'org ok'
    assert(o.get('archive.org')['exclude'] == 1)
    print 'archive.org ok'
    assert(o.get('www.techcrunch.com')['exclude'] == 1)
    print 'www.techcrunch.com ok'
    assert(o.get('www.google.com') is None)
    print 'www.google.com ok'

