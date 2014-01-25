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
import struct

import hqconfig

from dispatcher import *

class SeenRecord(object):
    RECSIZE = 12
    def __init__(self, key, ts):
        self.key = key
        self.ts = ts
    def bits(self):
        if self.key is None:
            return ''
        else:
            return struct.pack('lI', self.key, self.ts or 0)
    @staticmethod
    def frombits(bits):
        if bits == '':
            return SeenRecord(None, None)
        else:
            key, ts = struct.unpack('lI', bits)
            return SeenRecord(key, ts)
    @classmethod
    def readfrom(cls, f):
        bits = f.read(cls.RECSIZE)
        if 0 < len(bits) < cls.RECSIZE:
            logging.warn('invalid seenrec of length %d bytes', len(bits))
            bits = ''
        return cls.frombits(bits)

class SeenFile(object):
    def __init__(self, fn, mode='rb'):
        self.fn = fn
        self.mode = mode
        self.f = open(self.fn, self.mode)
        self.lastkeywritten = None
    def __enter__(self):
        return self
    def __exit__(self, ext, exv, tb):
        if self.f:
            self.f.close()
            self.f = None
        return False

    def next(self):
        return SeenRecord.readfrom(self.f)

    def write(self, r):
        if isinstance(r, tuple):
            r = SeenRecord(*r)
        if r.key is not None:
            self.f.write(r.bits())
            self.lastkeywritten = r.key
    
        
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
        index.sort(lambda x, y: cmp(x[0], y[0]))
        return index

    def _replay_scheduled(self, incdelfile, index):
        # if incdel file exists, cross-out cURLs mentioned,
        # so that we could safely discard incomplete SEEN.new
        # and start over without worrying about scheduling
        # cURLs twice.
        if os.path.isfile(incdelfile):
            logging.debug('reading %s', incdelfile)
            with open(incdelfile, 'rb') as incdel:
                incdelcount = 0
                for l in incdel:
                    try:
                        idx = int(l.rstrip())
                        if idx < len(index):
                            index[idx][1] = None
                            logging.debug('marking #%d as seen', idx)
                            incdelcount += 1
                        else:
                            # TODO: this could be a serious problem
                            # that calls operator's attention
                            logging.warn('%s: index %d out of range %d',
                                         incdelfile, idx, len(index))
                    except ValueError as ex:
                        logging.warn('%s: invalid entry %r ignored',
                                     incdelfile, l)
            logging.debug('replayed %s entries from %s', incdelcount,
                          incdelfile)
        
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
        index.sort(lambda x, y: cmp(x[0], y[0]))
        return index

    def remove_if_exists(self, fn):
        if os.path.isfile(fn):
            try:
                os.remove(fn)
            except OSError as ex:
                logging.warn('failed to delete file %r (%s)', fn, ex)

    def processinq(self, maxn):
        """maxn is unused."""
        result = dict(processed=0, excluded=0, saved=0, scheduled=0,
                      td=0.0, ts=0.0)

        seenfile = os.path.join(self.seendir, 'SEEN')
        # INCOMING.del file has a journal of index (into INCOMING)
        # not to be scheduled during recovery run. 1) all seen URLs are
        # added to this list, then 2) bulk scheduling step adds more
        # as it schedules unseen URLs. 1) is not essential for recovery,
        # but it saves re-running time-consuming merge process (TODO:
        # we may want to force re-running merge process if it's been long
        # since the last (terminated) processinq because of time-based
        # seen expiration).
        incdelfile = os.path.join(self.seendir, 'INCOMING.del')

        # if SEEN.new exists, previous processing got terminated while
        # either 1) updating SEEN, or 2) SCHEDULING unseen cURLs. we used
        # to raise a RuntimeError for this situation for operator to resolve
        # it by hand. Now we can safely discard SEEN.new and start over
        # thanks to INCOMING.del journal file.
        if os.path.exists(seenfile+'.new'):
            if os.path.exists(seenfile):
                # case 1: terminated during updating SEEN. discard SEEN.new
                # and start over.
                os.remove(seenfile+'.new')
                # remove INCOMING.del as we're redoing seen check and
                # no-URLs has been scheduled.
                self.remove_if_exists(incdelfile)
            else:
                # case 2: terminated during scheduling. no need to update
                # SEEN list. just redo scheduling taking INCOMING.del into
                # account.
                pass
        else:
            # if SEEN does not exist, create an empty file.
            if not os.path.isfile(seenfile):
                with open(seenfile, 'wb'):
                    pass

        # reprocess INCOMING if exists.
        incomingfile = os.path.join(self.seendir, 'INCOMING')
        if os.path.isfile(incomingfile):
            logging.warn('reprocessing %s', incomingfile)
            index = self._rebuild_index(incomingfile, result)
            self._replay_scheduled(incdelfile, index)
        else:
            index = self._build_incoming(incomingfile, result)
            # delete INCOMING.del file if exists (not strictly
            # necessary; it will be just ignored and overwritten,
            # and this only happens when processinq is terminated
            # during cleanup steps below.) just a precaution as
            # it'd be disasterous if it get somehow applied on
            # newly built index)
            self.remove_if_exists(incdelfile)

        if len(index) == 0:
            # nothing written
            try:
                os.remove(incomingfile)
            except OSError as ex:
                logging.warn('failed to delete file %r (%s)', incomingfile, ex)
            return result

        if os.path.isfile(seenfile):
            incdel = open(incdelfile, 'wb')
            idxin = 0
            seenrec = None
            with SeenFile(seenfile, 'rb') as sr:
                seenrec = sr.next()
                seenkey = seenrec.key
                with SeenFile(seenfile+'.new', 'wb') as sw:
                    while idxin < len(index):
                        newrec = index[idxin]
                        if seenkey is None or newrec[0] < seenkey:
                            # newrec is not seen
                            if newrec[0] > sw.lastkeywritten:
                                # TODO: get ts from per-domain config
                                ts = 0
                                sw.write((newrec[0], ts))
                            else:
                                # this happens when INCOMING has more than one
                                # instances of a URL (they should be adjacent)
                                # second and later instances must be dropped as
                                # seen. Note out-of-order URLs also falls here -
                                # it is an serious problem to be reported
                                # (TODO).
                                newrec[1] = None
                                incdel.write('{}\n'.format(idxin))
                            idxin += 1
                        elif newrec[0] == seenkey:
                            # newrec is seen
                            newrec[1] = None
                            incdel.write('{}\n'.format(idxin))
                            idxin += 1
                        if newrec[0] > seenkey:
                            # skip seenrec (copy to SEEN.new)
                            # drop duplicate items in SEEN
                            if seenkey > sw.lastkeywritten:
                                sw.write(seenrec)
                            seenrec = sr.next()
                            seenkey = seenrec.key
                    # copy remaining seenrec's to SEEN.new
                    if seenkey is not None:
                        if seenkey > sw.lastkeywritten:
                            sw.write(seenrec)
                        while 1:
                            seenrec = sr.next()
                            seenkey = seenrec.key
                            if seenkey is None: break
                            if seenkey > sw.lastkeywritten:
                                sw.write(seenrec)
            # this signifies SEEN.new is now complete; i.e. it's not necessary
            # to do merge again during recovery, and all cURLs in INCOMING
            # but those marked in INCOMING.del can be scheduled.
            os.remove(seenfile)
            incdel.close()

        with open(incdelfile, 'ab') as incdel:
            fd = os.open(incomingfile, os.O_RDONLY)
            map = mmap.mmap(fd, 0, access=mmap.ACCESS_READ)
            os.close(fd)
            for i, x in enumerate(index):
                if x[1] is None:
                    logging.debug('not scheduling #%d id=%s', i, x[0])
                    continue
                map.seek(x[1])
                l = map.readline()
                o = json.loads(l.rstrip())
                o['id'] = x[0]
                self.scheduler.schedule(o)
                incdel.write('{}\n'.format(i))
                result['scheduled'] += 1
                logging.debug('scheduled #%d %s', i, o)
            map.close()

        # cleanup: order is important!
        os.remove(incomingfile)
        os.rename(seenfile+'.new', seenfile)
        os.remove(incdelfile)

        return result
