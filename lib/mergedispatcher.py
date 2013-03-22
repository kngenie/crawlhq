"""
Dispatcher using sort-merge for seen-check.
"""
import sys
import os
import re
import logging
import time
import json
import mmap

import hqconfig

from dispatcher import *

class MergeDispatcher(Dispatcher):

    def __init__(self, domaininfo, job, mapper, scheduler, inq,
                 maxsize=int(2e9)):
        # TODO: currently Dispatcher.__init__() initializes seenfactory,
        # which is not necessary for MergeDispatcher.
        #super(MergeDispatcher, self).__init__(domaininfo, job, mapper,
        #                                      scheduler, inq)
        # begin dup with Dispatcher.__init__()
        self.domaininfo = domaininfo
        self.jobname = job
        self.mapper = mapper
        self.scheduler = scheduler

        self.inq = inq
        self.diverter = Diverter(self.jobname, self.mapper)
        self.excludedlist = ExcludedList(self.jobname)

        self.processedcount = 0
        # end

        self.seendir = os.path.join(hqconfig.get('datadir'),
                                    self.jobname, 'mseen')
        self.maxsize = maxsize

    def shutdown(self):
        # similarly
        logging.info("shutting down diverter")
        self.diverter.shutdown()
        logging.info("shutting down excludedlist")
        self.excludedlist.shutdown()
        logging.info("done.")
        
    def clear_seen(self):
        pass

    def processinq(self, maxn):
        """maxn is unused."""
        result = dict(processed=0, excluded=0, saved=0, scheduled=0)
        deq = self.inq.deq
        # TODO: resuming on crash
        incomingfile = os.path.join(self.seendir, 'INCOMING')
        if os.path.isfile(incomingfile):
            raise RuntimeError, (
                'refusing to clobber existing %s' % incomingfile)
        index = []
        with open(incomingfile, 'w') as w:
            size = 0
            while 1:
                br = deq.bulkreader()
                if br is None:
                    # no more qfiles available at this moment
                    break
                for o in br:
                    result['processed'] += 1
                    self.processedcount += 1
                    ws = self.mapper.workset(o)
                    if not self.is_workset_active(ws):
                        self.diverter.divert(str(ws), o)
                        result['saved'] += 1
                        continue
                    di = self.domaininfo.get_byurl(o['u'])
                    if di and di['exclude']:
                        self.excludedlist.add(o)
                        result['excluded'] += 1
                        continue
                    urikey = urihash.urikey(o['u'])
                    l = w.tell()
                    index.append([urikey, l])
                    line = json.dumps(o, separators=',:')
                    w.write(line)
                    w.write('\n')
                size = w.tell()
                if size >= self.maxsize:
                    break
        if len(index) == 0:
            # nothing written
            os.remove(incomingfile)
            return result
        index.sort(lambda x,y: cmp(x[0], y[0]))
        
        seenfile = os.path.join(self.seendir, 'SEEN')
        if not os.path.isfile(seenfile):
            with open(seenfile, 'wb'):
                pass
        
        idxin = 0
        seenrec = None
        with open(seenfile, 'rb') as sr:
            seenrec = sr.read(8)
            if seenrec == '':
                seenkey = None
            else:
                seenkey = struct.unpack('l', seenrec)[0]
            with open(seenfile+'.new', 'wb') as sw:
                while idxin < len(index):
                    newrec = index[idxin]
                    if seenkey is None or newrec[0] < seenkey:
                        # newrec is not seen
                        sw.write(struct.pack('l', newrec[0]))
                        idxin += 1
                    elif newrec[0] == seenkey:
                        # newrec is seen
                        newrec[1] = None
                        idxin += 1
                    if newrec[0] >= seenkey:
                        # skip seenrec (copy to SEEN.new)
                        sw.write(seenrec)
                        seenrec = sr.read(8)
                        if seenrec == '':
                            seenkey = None
                        else:
                            seenkey = struct.unpack('l', seenrec)[0]
                # copy remaining seenrec's to SEEN.new
                if seenkey is not None:
                    sw.write(seenrec)
                    seenkey = None
                    while 1:
                        seenrec = sr.read(4096)
                        if seenrec == '': break
                        sw.write(seenrec)
        fd = os.open(incomingfile, os.O_RDONLY)
        map = mmap.mmap(fd, 0, access=mmap.ACCESS_READ)
        os.close(fd)
        for x in index:
            if x[1] is None: continue
            map.seek(x[1])
            l = map.readline()
            o = json.loads(l.rstrip())
            o['id'] = x[0]
            self.scheduler.schedule(o)
            result['scheduled'] += 1
        map.close()

        os.remove(incomingfile)
        os.remove(seenfile)
        os.rename(seenfile+'.new', seenfile)

        return result

        
                        
                        
                        
        
        
        
        
