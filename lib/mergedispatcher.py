"""
Dispatcher using sort-merge for seen-check.

This veriant of Dispatcher stores seen table in a single binary file
consisting of fixed-length records, sorted by `key` (Rabin Fingerprint of
URL). Seen check is done by taking a bulk of input keys and performing
sort-merge oepration with existing seen table.
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
    """object for version-2 (with 4-byte time) seen database record.
    """
    RECSIZE = 12
    def __init__(self, key, ts):
        """initialize all attributes.

        :param key: 64-bit fingerprint.
        :type key: int
        :param ts: 32-bit time (in seconds) this seen record expires.
        :type ts: int
        """
        self.key = key
        assert self.key is None or isinstance(self.key, (int, long))
        self.ts = ts
    def bits(self):
        """return binary representation of this record.

        :rtype: str
        """
        if self.key is None:
            return ''
        else:
            return struct.pack('lI', self.key, self.ts or 0)
    @staticmethod
    def frombits(bits):
        """create SeenRecord from binary representation.
        if `bits` is an empty str, returns SeenRecord with
        all attributes set to ``None``.

        :param bits: binary representation
        :type bits: str
        """
        if bits == '':
            return SeenRecord(None, None)
        else:
            key, ts = struct.unpack('lI', bits)
            return SeenRecord(key, ts)
    @classmethod
    def readfrom(cls, f):
        """Read bits for one SeenRecord from `f`, and return
        SeenRecord object. At EOF, returns SeenRecord with
        all attributes set to ``None``.

        :param f: file-like to read bits from.
        :rtype: :class:`SeenRecord`
        """
        bits = f.read(cls.RECSIZE)
        if 0 < len(bits) < cls.RECSIZE:
            logging.warn('invalid seenrec of length %d bytes', len(bits))
            bits = ''
        return cls.frombits(bits)

class SeenRecordV1(object):
    """older format of seen record. that has URL hash key only.
    defined primarily for seen-table conversion tools.
    """
    RECSIZE = 8
    def __init__(self, key, ts):
        self.key = key
        self.ts = ts
    def bits(self):
        if self.key is None:
            return ''
        else:
            return struct.pack('l', self.key)
    @staticmethod
    def frombits(bits):
        if bits == '':
            return SeenRecordV1(None, None)
        else:
            key, = struct.unpack('l', bits)
            return SeenRecord(key, 0)
    @classmethod
    def readfrom(cls, f):
        bits = f.read(cls.RECSIZE)
        if 0 < len(bits) < cls.RECSIZE:
            logging.warn('invalid seenrec of length %d bytes', len(bits))
            bits = ''
        return cls.frombits(bits)

FORMAT_ID = {
    1: SeenRecordV1,
    2: SeenRecord
    }

class SeenFile(object):
    """representation of seen-table.
    binary file consisting of fixed-length record.
    this class can read/write either of two formats.
    object may be passed to ``with`` statement for automatic closure.
    """
    def __init__(self, fn, mode='rb', fmt=2):
        """
        :param fn: filename
        :param mode: open mode. use ``"wb"`` for writing.
        :param fmt: format ID. either 1 or 2. \
        other values will reslutl in :exc:`KeyError`.
        """
        self.fn = fn
        self.mode = mode
        self.f = open(self.fn, self.mode)
        self.lastkeywritten = None
        self.RECORD = FORMAT_ID[fmt]
    def __enter__(self):
        return self
    def __exit__(self, ext, exv, tb):
        if self.f:
            self.f.close()
            self.f = None
        return False

    def next(self):
        """return next record.

        :rtype: either :class:`SeenRecord` or :class:`SeenRecordV1`
        """
        return self.RECORD.readfrom(self.f)

    def tell(self):
        """return file pointer if file is open. return `None` otherwise.
        """
        return self.f and self.f.tell()
    def write(self, r):
        """write record `r` to file.
        it is legal to pass a record object of different version.

        :param r: record object
        """
        if isinstance(r, tuple):
            r = self.RECORD(*r)
        if r.key is not None:
            self.f.write(r.bits())
            self.lastkeywritten = r.key

def check_seenfile(seenfn, fmt=2):
    """check if keys are in order."""
    reccount = 0
    errorcount = 0
    firstuid = None
    lastuid = None
    with SeenFile(seenfn, 'rb', fmt=fmt) as f:
        while True:
            pos = f.tell()
            rec = f.next()
            reccount += 1
            uid = rec.key
            if uid is None:
                break
            if firstuid is None: firstuid = uid
            if lastuid is not None:
                if uid < lastuid:
                    print "out of order record: {} at {} after {}".format(
                        uid, pos, lastuid)
                    errorcount += 1
                elif uid == lastuid:
                    print "duplicate record {} at {}".format(uid, pos)
                    errorcount += 1
            lastuid = uid

    print "{} records, {} errors".format(reccount, errorcount)
    print "first {}, last {}".format(firstuid, lastuid)

    return errorcount == 0

def copy_seenfile(srcfn, dstfn, srcfmt=1, dstfmt=2):
    print "reading {} in format {}, writing {} in format {}".format(
        srcfn, srcfmt, dstfn, dstfmt)
    reccount = 0
    with SeenFile(srcfn, 'rb', fmt=srcfmt) as sf:
        with SeenFile(dstfn, 'wb', fmt=dstfmt) as df:
            while True:
                rec = sf.next()
                if rec.key is None:
                    break
                df.write(rec)
                reccount += 1
    print "copied {} records".format(reccount)

# entry points for command line tools
def main_check():
    from optparse import OptionParser
    opt = OptionParser('%prog [--fmt {1|2}] SEEN-FILE')
    opt.add_option('--fmt', type='int', default=2)
    options, args = opt.parse_args()
    if len(args) < 1:
        opt.error('specify SEEN-FILE to check')
    seenfile = args[0]
    exit(0 if check_seenfile(seenfile, options.fmt) else 1)

def main_convert():
    from optparse import OptionParser
    opt = OptionParser('%prog V1-SEEN-FILE V2-SEEN-FILE')
    options, args = opt.parse_args()
    if len(args) < 2:
        opt.error('specify V1-SEEN-FILE and V2-SEEN-FILE')
    srcfn, dstfn = args[:2]
    copy_seenfile(srcfn, dstfn, 1, 2)
    exit(0)

class MergeDispatcher(Dispatcher):

    def __init__(self, domaininfo, job, mapper, scheduler, inq,
                 maxsize=int(2e9)):
        """Dispatcher that performs seen check by merging sorted
        cURL records against fixed-size records of URL-IDs.

        This version can resume processing previously terminated by
        system crash etc. without double scheduling.

        :param domaininfo:
        :param job: crawl job name
        :type job: str
        :param mapper: workset mapper
        :param scheduler: workset scheduler
        :param inq: incoming queue
        :param maxsize: max size of input for a batch
        """
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
        """collect big-enough number of incomign cURLs (from multiple
        incoming queues), excluding those for *no-crawl* domains and
        offline queues, and save them in single queue file ``INCOMING``
        in ``mseen``.
        ``processed``, ``saved`` and ``excluded`` counters in `result`
        will be updated.

        :param incomingfile: queuefile for storing cURLs \
        (typically ``mseen/INCOMING``)
        :param result: counters
        :type result: dict
        """
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
        """main procedure for inqueue processing.

        :param maxn: unused
        """
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
