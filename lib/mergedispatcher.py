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

    def _rebuild_index(self, incomingfile, result):
        index = []
        with open(incomingfile, 'r') as f:
            while 1:
                pos = f.tell()
                line = f.readline()
                if not line: break
                try:
                    o = json.loads(line.strip())
                    urikey = urihash.urikey(o['u'])
                except:
                    continue
                index.append([urikey, pos])
                result['processed'] += 1
                self.processedcount += 1
        return index

    def _build_incoming(self, incomingfile, result):
        deq = self.inq.deq
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
        return index

    def processinq(self, maxn):
        """maxn is unused."""
        result = dict(processed=0, excluded=0, saved=0, scheduled=0,
                      td=0.0, ts=0.0)

        seenfile = os.path.join(self.seendir, 'SEEN')
        # if SEEN.new exists, we need to reconstruct SEEN by combining
        # SEEN and SEEN.new. As this can be a time-consuming process,
        # we notify the problem by throwing an exception.
        if os.path.exists(seenfile+'.new'):
            if os.path.exists(seenfile):
                raise RuntimeException, ('%s.new exists. SEEN needs to be '
                                         'reconstructed with mseenrepair tool'
                                         % (seenfile,))
            # there's no SEEN file - we can simply mv SEEN.new to SEEN
            os.rename(seenfile+'.new', seenfile)
        # if SEEN does not exist, create an empty file.
        if not os.path.isfile(seenfile):
            with open(seenfile, 'wb'):
                pass

        # reprocess INCOMING if exists.
        incomingfile = os.path.join(self.seendir, 'INCOMING')
        if os.path.isfile(incomingfile):
            logging.warn('reprocessing %s', incomingfile)
            index = self._rebuild_index(incomingfile, result)
        else:
            index = self._build_incoming(incomingfile, result)

        if len(index) == 0:
            # nothing written
            try:
                os.remove(incomingfile)
            except OSError, ex:
                logging.warn('failed to delete file %r (%s)', incomingfile, ex)
            return result
        index.sort(lambda x,y: cmp(x[0], y[0]))
        
        idxin = 0
        seenrec = None
        with open(seenfile, 'rb') as sr:
            seenrec = sr.read(8)
            if seenrec == '':
                seenkey = None
            else:
                seenkey = struct.unpack('l', seenrec)[0]
            lastkeywritten = None
            with open(seenfile+'.new', 'wb') as sw:
                while idxin < len(index):
                    newrec = index[idxin]
                    if seenkey is None or newrec[0] < seenkey:
                        # newrec is not seen
                        if newrec[0] > lastkeywritten:
                            sw.write(struct.pack('l', newrec[0]))
                            lastkeywritten = newrec[0]
                        else:
                            # this happens when INCOMING has more than one
                            # instances of a URL (they should be adjacent)
                            # second and later instances must be dropped as
                            # seen. Note out-of-order URLs also falls here -
                            # it is an serious problem to be reported (TODO).
                            newrec[1] = None
                        idxin += 1
                    elif newrec[0] == seenkey:
                        # newrec is seen
                        newrec[1] = None
                        idxin += 1
                    if newrec[0] > seenkey:
                        # skip seenrec (copy to SEEN.new)
                        # drop duplicate items in SEEN
                        if seenkey > lastkeywritten:
                            sw.write(seenrec)
                            lastkeywritten = seenkey
                        seenrec = sr.read(8)
                        if seenrec == '':
                            seenkey = None
                        elif len(seenrec) < 8:
                            logging.warn('invalid seenrec of length %d' %
                                         len(seenrec))
                            seenrec = ''
                            seenkey = None
                        else:
                            seenkey = struct.unpack('l', seenrec)[0]
                # copy remaining seenrec's to SEEN.new
                if seenkey is not None:
                    if seenkey > lastkeywritten:
                        sw.write(seenrec)
                        lastkeywritten = seenkey
                    while 1:
                        seenrec = sr.read(8)
                        if seenrec == '': break
                        if len(seenrec) < 8:
                            logging.warn('invalid seenrec of length %d' %
                                         len(seenrec))
                            break;
                        seenkey = struct.unpack('l', seenrec)[0]
                        if seenkey > lastkeywritten:
                            sw.write(seenrec)
                            lastkeywritten = seenkey
        # TODO: possible data loss - if process is killed while we bulk
        # schedule "unseen" URLs here, remaining URLs will be rejected
        # as "seen" in reprocessing (because they are marked "seen" in
        # SEEN.new). We need to either schedule URLs in the seen check
        # loop above, or mark URLs "scheduled" here.
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

        
                        
                        
                        
        
        
        
        
