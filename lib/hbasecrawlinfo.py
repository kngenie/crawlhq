import sys
import os
from struct import pack, unpack
import logging

from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import *

class CrawlInfo(object):
    """interface to re-crawl information database, implemented with Hbase.
    """
    def __init__(self, master, table='crawlinfo'):
        """master: thrift entry point to Hbase Master (default port 9090)
        """
        self.table = table

        if master: self.open(master)
        
    def open(self, master):
        if isinstance(master, basestring):
            master = master.split(':', 1)
        self.master = master
        self.transport = TTransport.TBufferedTransport(
            TSocket.TSocket(*master))
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(protocol)
        self.transport.open()
        
    def shutdown(self):
        self.transport.close()

    def countitems(self):
        """count crawlinfo records in the database.
        note that scanning Hbase table takes LOOOOONG TIME (~1000/s)."""
        count = 0
        sid = self.client.scannerOpen(self.table, '', ['f:s'])
        while 1:
            r = self.client.scannerGetList(sid, 1000)
            #r = self.client.scannerGet(sid)
            if not r: break
            count += len(r)
            logging.debug('%d %s', count, r[-1].row)
        self.scannerClose(sid)
        return count

    def save_result(self, furi):
        if 'a' not in furi: return
        mutations = []
        data = furi['a']
        mutations.append(Mutation(column='f:s', value=str(data['s'])))
        if 'd' in data:
            mutations.append(Mutation(column='f:d', value=data['d']))
        if 'e' in data:
            mutations.append(Mutation(column='f:e', value=data['e']))
        if 'm' in data:
            m = pack('>Q', int(data['m']))
            mutations.append(Mutation(column='f:m', value=m))
        self.client.mutateRow(self.table, curl['u'], mutations)

    def update_crawlinfo(self, curis):
        """updates curis with crawlinfo in bulk.
        not implemented - no-op. this is too slow anyways.
        """
        pass
        # tasks = [(self._set_crawlinfo, (curi,))
        #          for curi in curis if 'a' not in curi]
        # if tasks:
        #     b = TaskBucket(tasks)
        #     b.execute_wait(executor, 4)
        
    def _set_crawlinfo(self, curi):
        if 'a' not in curi:
            a = self.get_crawlinfo(curi)
            if a: curi['a'] = a

    def get_crawlinfo(self, curi):
        # getRow() returns a list with one TRowResult object in it,
        # or an empty list if row does not exist.
        rr = self.client.getRow(self.table, curi['u'])
        if not rr: return None
        rr = rr[0]
        a = {}
        # each column is a TCell object.
        if 'f:s' in rr.columns:
            a['s'] = int(rr.columns['f:s'].value)
        if 'f:m' in rr.columns:
            # f:m is in big-endian (network) order
            a['m'], = unpack('>Q', rr.columns['f:m'].value)
        for c in ('d', 'e', 'z'):
            cn = 'f:'+c
            if cn in rr.columns: a[c] = rr.columns[cn].value
        return a
