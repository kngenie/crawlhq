"""
runs dispatcher when necessary.
"""
import sys, os
import re
from optparse import OptionParser
from threading import Thread, Condition, RLock
import time
import traceback
import logging
import signal

import hqconfig
from dispatcher import WorksetMapper
from scheduler import Scheduler
from fileinq import IncomingQueue
from priorityqueue import PriorityDequeue

##----------------------------------------------------------------
## TODO: make this part of FileInqueue?
import ctypes, ctypes.util
import select
import fcntl
import termios
import struct
import errno
import array
import threading

libc = ctypes.cdll.LoadLibrary(ctypes.util.find_library('libc'))

class Watch(object):
    def __init__(self, wd):
        self.wd = wd
        self.c = threading.Condition()
        self.available = False
    def wait(self, timeout=None):
        with self.c:
            self.available = False
            self.c.wait(timeout)
            return self.available

class InqueueWatcher(threading.Thread):
    def __init__(self):
        super(InqueueWatcher, self).__init__()
        self.daemon = True
        self.in_fd = libc.inotify_init()
        if self.in_fd < 0:
            logging.error('inotify_init failed')
        self.watches = {}

    def __del__(self):
        self.shutdown()
        super(InqueueWatcher, self).__del__()

    def shutdown(self):
        self.running = False

    def run(self):
        try:
            self._poll = select.poll()
            self._poll.register(self.in_fd, select.POLLIN)
            self.loop()
        except:
            logging.error('InqueueWatcher.loop error', exc_info=1)
        finally:
            logging.debug('InqueueWatcher.loop exited')
            self._poll.unregister(self.in_fd)
            os.close(self.in_fd)
            for w in self.watches.values():
                with w.c:
                    w.c.notify_all()

    def process_events(self):
        # structure of fixed length part of inotify_event struct
        fmt = 'iIII'
        ssize = struct.calcsize(fmt)
        # each event record must be read with one os.read() call.
        # so you cannot do "read the first 16 byte, then name_len
        # more bytes"
        #b = os.read(self.in_fd, struct.calcsize(fmt))
        #b = os.read(self.in_fd, 16)
        buf = array.array('i', [0])
        if fcntl.ioctl(self.in_fd, termios.FIONREAD, buf, 1) < 0:
            return
        rlen = buf[0]
        try:
            b = os.read(self.in_fd, rlen)
        except:
            logging.warn('read on in_fd failed', exc_info=1)
            return
        while b:
            wd, mask, cookie, name_len = struct.unpack(fmt, b[:ssize])
            name = b[ssize:ssize + name_len]
            b = b[ssize + name_len:]
            # notify Condition corresponding to wd
            if (mask & 0x80) == 0x80:
                logging.info('new queue file %s' % name)
                for w in self.watches.values():
                    if w.wd == wd:
                        with w.c:
                            w.available = True
                            w.c.notify()

    def loop(self):
        self.running = True
        while self.running:
            # poll with timeout for checking self.running
            try:
                r = self._poll.poll(500)
            except select.error as ex:
                if ex[0] == errno.EINTR:
                    # nofity all conditions so that waiting clients
                    # wake up and (probably) exit
                    # (this is ineffective since only main thread
                    # receives INT signal)
                    for w in self.watches.values():
                        with w.c:
                            w.c.notify_all()
                else:
                    logging.error('poll failed', exc_info=1)
                continue
            if r and r[0][1] & select.POLLIN:
                self.process_events()
    def addwatch(self, path):
        if path in self.watches:
            return self.watches[path]
        mask = 0x80 # IN_MOVED_TO
        wd = libc.inotify_add_watch(self.in_fd, path, mask)
        watch = self.watches[path] = Watch(wd)
        return watch

    def delwatch(self, path):
        e = self.watches[path]
        s = libc.inotity_rm_watch(self.in_fd, e.wd)
        if s < 0:
            logging.warn('failed to remove watch on %s(wd=%d)', path, e.wd)
        else:
            with e.c:
                e.available = True
                e.c.notify_all()
            del self.watches[path]
        
    def available(self, path, timeout=None):
        e = self.watches[path]
        return e.wait(timeout)
##----------------------------------------------------------------

def dispatcher_leveldb(domaininfo, job, *args, **kwargs):
    # refuse to run if MergeDispatcher files exist
    mseendir = os.path.join(hqconfig.get('datadir'), job, 'mseen')
    if os.path.isdir(mseendir):
        raise Exception('found directory %r, which suggests "merge"'
                        ' dispatcher is in use. remove it if that is'
                        ' no longer the case' % mseendir)
    from dispatcher import Dispatcher
    return Dispatcher(domaininfo, job, *args, **kwargs)
def dispatcher_merge(domaininfo, job, *args, **kwargs):
    # refuse to run if MergeDispatcher directory does not exist
    mseendir = os.path.join(hqconfig.get('datadir'), job, 'mseen')
    if not os.path.isdir(mseendir):
        raise Exception('directory %r does not exist. create it and'
                        ' put SEEN file with initial seen list.'
                        % mseendir)
    from mergedispatcher import MergeDispatcher
    return MergeDispatcher(domaininfo, job, *args, **kwargs)

# TODO move to "dispatcher" package.
DISPATCHER_CHOICES = {
    'leveldb': dispatcher_leveldb,
    'merge': dispatcher_merge
}
def build_dispatcher(name, domaininfo, job, mapper, scheduler, inqueue):
    """build a Dispatcher instance of type name."""
    C = DISPATCHER_CHOICES[name]
    return C(domaininfo, job, mapper, scheduler, inqueue)

class InqueueProcessor(object):
    inqwatcher = None
    # TODO: allow different type of Dispatcher for each job.
    def __init__(self, job, dispatcher_type, maxn):
        self.job = job
        self.maxn = maxn

        self.domaininfo = hqconfig.factory.domaininfo()
        self.jobconfigs = hqconfig.factory.jobconfigs()
        self.coordinator = hqconfig.factory.coordinator()

        # per-job objects
        # TODO: process multiple jobs in one process
        self.mapper = CrawlMapper(CrawlJob(self.job, self.jobconfigs),
                                  hqconfig.NWORKSETS_BITS)
        self.scheduler = Scheduler(hqconfig.worksetdir(self.job),
                                   self.mapper, reading=False)
        self.inqueue = IncomingQueue(hqconfig.inqdir(self.job),
                                     deq=PriorityDequeue)
        self.dispatcher = build_dispatcher(dispatcher_type,
                                           self.domaininfo, self.job,
                                           mapper=self.mapper,
                                           scheduler=self.scheduler,
                                           inqueue=self.inqueue)

        if os.uname()[0] == 'Linux':
            if InqueueProcessor.inqwatcher is None:
                iqw = InqueueProcessor.inqwatcher = InqueueWatcher()
                iqw.start()
            self.watch = InqueueProcessor.inqwatcher.addwatch(self.inqueue.qdir)

    def shutdown(self):
        self.dispatcher.shutdown()
        self.scheduler.shutdown()
        self.jobconfigs.shutdown()
        self.domaininfo.shutdown()

    def stop(self):
        self.__repeat = False

    def processinq(self):
        # t and job should be set in Dispatcher.processinq
        start = time.time()
        r = self.dispatcher.processinq(self.maxn)
        r['t'] = time.time() - start
        r['job'] = self.job
        r.update(
            ps=r.get('processed', 0)/r['t'] if r['t'] != 0 else 0,
            eps=r.get('scheduled', 0)/r['t'] if r['t'] != 0 else 0
            )
        return r

    def run(self, repeat=True):
        self.__repeat = repeat
        do_flush = False
        while 1:
            r = self.processinq()
            logging.info("{job} {scheduled:d}/{processed:d} "
                         "X:{excluded:d} T:{t:.3f}(D{td:.3f},S{ts:.3f}) "
                         "{eps:8.2f}/{ps:8.2f}/s".format(**r))
            if not r.get('processed'):
                # flush worksets before sleeping - this is not perfect,
                # but better-than-nothing solution until we implement
                # inter-process communication.
                if do_flush:
                    dispatcher.flush_worksets()
                    do_flush = False
                logging.info("inq exhausted, sleeping for %ds",
                             options.exhaust_delay)
                # because wait_available cannot be interrupted by INT signal,
                # we repeat short waits for exhaust_delay.
                #time.sleep(options.exhaust_delay)
                until = time.time() + options.exhaust_delay
                while time.time() < until and self.__repeat:
                    if self.watch.wait(1.0):
                        break
            else:
                # flush if clients are starved.
                if self.scheduler.flush_starved():
                    logging.info("workset flushed")
                    do_flush = False
                else:
                    do_flush = (r.get('scheduled', 0) > 0)
            if not self.__repeat:
                break

class CrawlMapper(WorksetMapper):
    """maps client queue id to set of WorkSets
    """
    def __init__(self, crawljob, nworksets_bits):
        super(CrawlMapper, self).__init__(nworksets_bits)
        self.crawljob = crawljob
        self.jobconfigs = self.crawljob.jobconfigs
        self.job = self.crawljob.jobname
        self.load_workset_assignment()

        self.client_last_active = {}

    def create_default_workset_assignment(self):
        num_nodes = self.jobconfigs.get_jobconf(self.job, 'nodes', 20)
        return list(itertools.islice(
                itertools.cycle(xrange(num_nodes)),
                0, self.nworksets))
        
    def load_workset_assignment(self):
        wscl = self.jobconfigs.get_jobconf(self.job, 'wscl')
        if wscl is None:
            # wscl = self.create_default_workset_assignment()
            # self.jobconfigs.save_jobconf(self.job, 'wscl', wscl)
            raise RuntimeError, 'workset-client map (wscl) is not set up'
        if len(wscl) > self.nworksets:
            # wscl[self.nworksets:] = ()
            raise RuntimeError('size of workset-client map (wscl) %d'
                               ' does not match workset size %d',
                               (len(wscl), self.nworksets))
        elif len(wscl) < self.nworksets:
            # wscl.extend(itertools.repeat(None, self.nworksets-len(wscl)))
            raise RuntimeError('size of workset-client map (wscl) %d'
                               ' doe not match workset size %d',
                               (len(wscl), self.worksets))
        self.worksetclient = wscl
        self.numclients = max(wscl) + 1

    def wsidforclient(self, clid):
        '''return list of workset ids for node name of nodes-node cluster'''
        # XXX linear search
        qids = [i for i in xrange(len(self.worksetclient))
                if self.worksetclient[i] == clid]
        return qids

    def client_activating(self, clid):
        """called back by Scheduler when client gets a new feed request
        when it is inactive."""
        for wsid in self.wsidforclient(clid):
            self.crawljob.workset_activating(wsid)

    def clientforwsid(self, wsid):
        if wsid < 0 or wsid >= len(self.worksetclient):
            raise ValueError, 'wsid %d out range (0-%d)' % (
                wsid, len(self.worksetclient) - 1)
        return self.worksetclient[wsid]

    def client_activating(self, clid):
        """called back by Scheduler when client gets a new feed request
        when it is inactive."""
        for wsid in self.wsidforclient(clid):
            self.crawljob.workset_activating(wsid)

    def clients(self, server):
        """returns a list of client-IDs server serves in job."""
        servers = coordinator.get_job_servers(self.job)
        for sid, name in servers.iteritems():
            if name == server:
                # assuming sid is zero-based and all sid < number of servers:
                clpsv = (self.numclients + len(servers) - 1) / len(servers)
                clients = range(self.numclients)[sid * clpsv:(sid + 1) * clpsv]
                return [str(n) for n in clients]
        return []

class CrawlJob(object):
    """just enough to satisfy CrawlMapper requirements"""
    def __init__(self, name, jobconfigs):
        self.jobname = name
        self.jobconfigs = jobconfigs

# entry-point for processinq-sa
# TODO: move this to a module dedicated for command-line processinq?
def main_standalone():
    from optparse import OptionParser
    opt = OptionParser(usage='%prog [OPTIONS] JOBNAME CLIENT-ID ...')
    opt.add_option('-n', action='store', dest='maxn', default=500, type='int',
                   help='maximum number of URLs to process in each cycle'
                   ' (effective with "leveldb" dispatcher only)')
    opt.add_option('-1', action='store_true', dest='justonce', default=False,
                   help="runs processinq just once, then exits"
                   )
    opt.add_option('-w', action='store', dest='exhaust_delay', default=60,
                   type='int',
                   help='seconds to wait between processing when queue '
                   'got exhausted')
    opt.add_option('-v', action='store_true', dest='verbose', default=False,
                   help='print out extra information')
    opt.add_option('-D', '--dispatcher', dest='dispatcher', type='choice',
                   choices=DISPATCHER_CHOICES.keys(), default='merge',
                   help='type of dispatcher to use %s' %
                   DISPATCHER_CHOICES.keys())
    opt.add_option('-L', '--logfile', default=None,
                   help='a file to send log output to (default %default)')
    options, args = opt.parse_args()

    logconf = dict(
        level=(logging.DEBUG if options.verbose else logging.INFO),
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
        )
    if options.logfile:
        logconf['filename'] = options.logfile
    logging.basicConfig(**logconf)

    if len(args) < 1:
        opt.error('JOBNAME is missing')
    job = args[0]
    active_clids = args[1:]

    processor = InqueueProcessor(job, options.dispatcher, options.maxn)
    if len(active_clids) < 1:
        # ugly direct access
        active_clids = processor.mapper.clients(os.uname()[1])
        if not active_clids:
            opt.error('cannot infer CLIENT-IDs. specify CLIENT-IDs manually')
        logging.warn("using default CLIENT-IDs:%s",
                     ' '.join(map(str, active_clids)))
    # standalone processinq's mapper cannot be updated by ClientQueue. so
    # we override is_client_active() to tell dispatcher all assigned worksets
    # are *always* active.
    # TODO: needs clearner way to do this
    processor.dispatcher.is_client_active = \
        lambda clid: str(clid) in active_clids

    def interrupted(sig, frame):
        logging.info("Interrupted, exiting...")
        processor.stop()
    signal.signal(signal.SIGINT, interrupted)
    # TODO: stop processinq right away upon SIGTERM
    signal.signal(signal.SIGTERM, interrupted)

    try:
        processor.run(not options.justonce)
    finally:
        processor.shutdown()
