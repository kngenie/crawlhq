import sys
import os
import pymongo
import urihash

class CrawlInfo(object):
    '''database of re-crawl infomation. keeps fetch result from previous
       crawl and makes it available in next cycle. this version uses MongoDB.'''
    def __init__(self, jobname):
        self.jobname = jobname
        self.mongo = pymongo.Connection()
        self.db = self.mongo.crawl
        self.coll = self.db.seen[self.jobname]
        
    def shutdown(self):
        self.coll = None
        self.db = None
        self.mongo.disconnect()

    def countitems(self):
        return self.coll.count()

    def save_result(self, furi):
        if 'a' not in furi: return
        if 'id' in furi:
            key = int(furi['id'])
        else:
            key = urihash.urikey(furi['u'])
        self.coll.update({'_id': key},
                         {'$set':{'a': furi['a'], 'u': furi['u']}},
                         upsert=True, multi=False)

    def update_crawlinfo(self, curis):
        '''updates curis with crawlinfo in bulk'''
        tasks = [(self._set_crawlinfo, (curi,))
                 for curi in curis if 'a' not in curi]
        if tasks:
            b = TaskBucket(tasks)
            b.execute_wait(executor, 4)
        
    def _set_crawlinfo(self, curi):
        key= curi['id']
        r = self.coll.find_one({'_id': key}, {'a': 1})
        if r and 'a' in r:
            curi['a'] = r['a']

    def get_crawlinfo(self, curi):
        if 'a' in curi:
            return curi['a']
        key = curi['id']
        r = self.coll.find_one({'_id': key}, {'a': 1})
        return r and r.get('a')
        
