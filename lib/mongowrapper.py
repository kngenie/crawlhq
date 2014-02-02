import sys
import time
import pymongo

class MongoDatabaseWrapper(object):
    """MongoDatabaseWrapper acts as always-available proxy
    to pymongo.Database, which is unavailable when connection
    cannot be made at HQ startup.
    """
    def __init__(self, connection_params, dbname):
        self.connection_params = connection_params
        self.dbname = dbname
        self.client = None
        self.fail_time = None

    def _connect(self):
        if self.client is None:
            try:
                self.client = pymongo.MongoClient(**self.connection_params)
            except pymongo.errors.ConnectionFailure as ex:
                self.fail_time = time.time()
                self.client = None
                raise
        
    def __getattr__(self, name):
        """get a collecion wrapper by name."""
        return MongoCollectionWrapper(self, name)
    def __getitem__(self, name):
        return MongoCollectionWrapper(self, name)

    def _get_collection(self, collname):
        self._connect()
        return self.client[self.dbname][collname]

class MongoCollectionWrapper(object):
    def __init__(self, dbw, name):
        self.dbw = dbw
        self.name = name

    def _coll(self):
        return self.dbw._get_collection(self.name)

    def find(self, *args, **kwargs):
        return self._coll().find(*args, **kwargs)

    def find_one(self, *args, **kwargs):
        return self._coll().find_one(*args, **kwargs)

    def update(self, *args, **kwargs):
        return self._coll().update(*args, **kwargs)

    def insert(self, *args, **kwargs):
        return self._coll().insert(*args, **kwargs)
    
    def remove(self, *args, **kwrgs):
        return self._coll().remove(*args, **kwargs)

    def save(self, *args, **kwargs):
        return self._coll().save(*args, **kwargs)

    def count(self):
        return self._coll().count()

    
