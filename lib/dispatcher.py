# HQ dispatcher - seen check and scheduling to worksets
#

import os
import re
import threading
import logging
from urlparse import urlsplit
from Queue import Queue, Empty, Full
import time

import hqconfig
from filequeue import FileDequeue, FileEnqueue
import sortdequeue
import urihash
try:
    from cfpgenerator import FPGenerator
except:
    from fpgenerator import FPGenerator
from scheduler import Scheduler, WorkSet
from diverter import Diverter

import tasprefix
import weblib

class WorksetMapper(object):
    """maps URLs to WorkSet"""
    def __init__(self, nworksets_bits):
        self.nworksets_bits = nworksets_bits
        self.nworksets = (1 << self.nworksets_bits)

        self._fp31 = FPGenerator(0xBA75BB4300000000, 31)

    def hosthash(self, curi):
        prefix = tasprefix.prefix(curi)
        if isinstance(prefix, unicode):
            prefix = prefix.encode('utf-8')
        return int(self._fp31.fp(prefix))

    def workset(self, curi):
        '''reutrns WorkSet id to which curi should be dispatched'''
        hosthash = self.hosthash(curi['u'])
        # Note: don't use % (mod) - FP hash is much less evenly distributed
        # in lower bits.
        ws = hosthash >> (64 - self.nworksets_bits)
        return ws

def FPSortingQueueFileReader(qfile, **kwds):
    def urikey(o):
        return urihash.urikey(o['u'])
    return sortdequeue.SortingQueueFileReader(qfile, urikey)
    
class ExcludedList(object):
    """URL list for storing excluded URLs. As URLs are checked for exclusion
    before seen check, there are (a lot of) duplicates.
    read-out is not supported because current HQ makes no use of these URLs.
    """
    # TODO: duplicated code with DivertQueue
    def __init__(self, jobname, bufsize=20):
        self.qdir = os.path.join(hqconfig.get('datadir'), jobname, 'ex')
        if not os.path.isdir(self.qdir):
            os.makedirs(self.qdir)
        FileEnqueue.recover(self.qdir)
        self.enq = FileEnqueue(self.qdir, buffer=bufsize, suffix='ex')
        self.queuedcount = 0

    def flush(self):
        self.enq._flush()
        return self.enq.close()

    def shutdown(self):
        self.flush()

    def get_status(self):
        r = dict(queued=self.queuedcount)
        return r

    def add(self, furi):
        self.enq.queue(furi)
        self.queuedcount += 1

class Dispatcher(object):
    inqwatcher = None

    # TODO: take JobConfig, instead of job
    def __init__(self, domaininfo, job, mapper,
                 scheduler, inq=None):
        self.domaininfo = domaininfo
        self.jobname = job
        self.mapper = mapper
        self.scheduler = scheduler

        # TODO: inject these objects from outside
        self.inq = inq
        if self.inq is None:
            qdir = hqconfig.inqdir(self.jobname)
            self.inq = FileDequeue(qdir, reader=FPSortingQueueFileReader)
        self.diverter = Diverter(self.jobname, self.mapper)
        self.excludedlist = ExcludedList(self.jobname)

        self.workset_state = [0 for i in range(self.mapper.nworksets)]

        # stats
        self.processedcount = 0

    def shutdown(self):
        #if self.job: self.job.shutdown()
        # logging.info("shutting down scheduler")
        # self.scheduler.shutdown()
        logging.info("shutting down diverter")
        self.diverter.shutdown()
        logging.info("shutting down excludedlist")
        self.excludedlist.shutdown()
        logging.info("done.")

    def flush(self):
        """flushes URIs buffered in workset objects"""
        self.scheduler.flush_clients()
    flush_worksets = flush

    def get_status(self):
        r = dict(
            processedcount=self.processedcount
            )
        return r

    def is_client_active(self, clid):
        """is client clid active?"""
        # TODO: update ZooKeeper when active status changes
        #t = self.client_last_active.get(str(clid))
        return self.scheduler.is_active(clid)

    def is_workset_active(self, wsid):
        """is workset wsid assigned to any active client?"""
        clid = self.mapper.worksetclient[wsid]
        return self.is_client_active(clid)

    def workset_activating(self, wsid):
        """activates working set wsid; start sending CURIs to Scheduler
        and enqueue diverted CURIs back into incoming queue so that
        processinq will process them (again). called by Scheduler,
        through CrawlMapper, when client starts feeding.
        note, unlike workset_deactivating, this method shall not be
        called from inside processinq method below, because processinq
        executes it only when at least one CURI is available for processing.
        if inq is empty, CURIs in divert queues would never be enqueued back.
        """
        # this could be executed asynchronously
        logging.info('workset %s activated', wsid)
        self.workset_state[wsid] = 1
        # is it better to move files back into inq directory?
        qfiles = self.diverter.listqfiles(wsid)
        logging.info('re-scheduling %s to inq', str(qfiles))
        self.inq.qfiles_available(qfiles)

    def workset_deactivating(self, wsid):
        """deactivates working set wsid; start sending CURIs into
        divert queues."""
        logging.info('workset %s deactivated', wsid)
        self.workset_state[wsid] = 0
        # flush Workset queues. we don't move qfiles to diverter yet.
        # it will be done when other HQ server becomes active on the
        # workset, and this HQ server starts forwarding CURIs.
        self.scheduler.flush_workset(wsid)

    def processinq(self, maxn):
        """take maxn chunk of incoming cURLs, and schedule those 'unseen'
        (whatever defined by implementation).
        maxn is just an advice on how many cURLs it should process in
        one cycle. actual implementation may ignore this value and choose
        whatever unit of work suitable.
        """
        raise Exception('{}.processinq() has no implementation'.format(self))

    def count_seen(self):
        raise Exception('{}.count_seen() is not implemented'.format(self))
    def clear_seen(self):
        raise Exception('{}.clear_seen() is not implemented'.format(self))
