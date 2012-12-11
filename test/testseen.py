import hqconfig
import urihash
class TestSeen(object):
    def __init__(self, job):
        self.db = {}
    def already_seen(self, furi):
        key = furi.get('id')
        if key is None: key = urihash.urikey(furi['u'])
        if isinstance(key, long):
            raise ValueError
        if key in self.db:
            return {'_id': furi.get('id'), 'e': (1<<32)-1}
        else:
            self.db[key] = (1<<32)-1
            return {'_id': furi.get('id'), 'e': 0}
    def close(self):
        pass
hqconfig.factory.seenfactory = lambda: TestSeen
