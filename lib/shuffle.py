import sys
import os
import re
import itertools

import hqconfig
from filequeue import FileDequeue
from client import DiscoveredClient

from zkcoord import Coordinator

class Shuffle(object):
    def __init__(self, jobname):
        self.jobname = jobname
        self.divbase = os.path.join(hqconfig.get('datadir'), jobname, 'div')
        #self.coord = hqconfig.factory.coordinator
        self.coord = Coordinator(hqconfig.get('zkhosts'), readonly=1)
        self._get_servers()
        self.nodename = os.uname()[1]

    def _get_servers(self):
        # TODO: read server info from coordinator
        # TODO: servers list can change in the middle of operation.
        self.id2host = self.coord.get_job_servers(self.jobname)
        self.servers = len(self.id2host)
        # currently fixed - TODO
        self.clients = 25
        
    def ws2id(self, wsid):
        return (wsid % self.clients) / self.servers
    
    def shuffle_divert(self, wsid):
        divdir = os.path.join(self.divbase, str(wsid))
        deque = FileDequeue(divdir)

        serverid = self.ws2id(wsid)
        if serverid not in self.id2host:
            raise ValueError, 'server for ws %d is unknown' % wsid
        server = self.id2host[serverid]
        if server == self.nodename:
            raise ValueError, 'refusing to shuffle to myself'
        if not self.coord.is_server_alive(server):
            raise IOError, 'server %s is not alive' % server

        client = DiscoveredClient(server, self.jobname)

        def dequewrapper(q):
            count = 0
            while 1:
                curi = q.get(timeout=0.1)
                if curi is None: break
                count += 1
                sys.stderr.write('\r%s/%s: submitting %d to %s' % (
                        self.jobname, wsid, count, server))
                yield curi
            sys.stderr.write('\n')

        client.batch_submit_discovered(dequewrapper(deque))

def main():
    from optparse import OptionParser

    opt = OptionParser('%prog JOBNAME')
    options, args = opt.parse_args()
    if len(args) < 1:
        opt.error('JOBNAME is required')
    shuffle = Shuffle(args[0])

    divqs = os.listdir(shuffle.divbase)
    for divq in divqs:
        wsid = int(divq)
        qfiles = [fn for fn in os.listdir(os.path.join(shuffle.divbase, divq))
                  if not fn.endswith('.open')]
        if len(qfiles) > 0:
            print >>sys.stderr, "processing %d files in %s" % (len(qfiles), divq)
            try:
                shuffle.shuffle_divert(wsid)
            except Exception, ex:
                print >>sys.stderr, str(ex)

if __name__ == '__main__':
    main()
