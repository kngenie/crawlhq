import sys, os
import re
import zookeeper as zk
import logging
import time

__all__ = ['Coordinator']

NODE_HQ_ROOT = '/hq'

PERM_WORLD = [dict(perms=zk.PERM_ALL,scheme='world',id='anyone')]

# this has global effect
zk.set_debug_level(zk.LOG_LEVEL_WARN)

def create_flags(s):
    """convert textual node creation flags into numerical value for API."""
    f = 0
    for c in s:
        if c == 'e': f |= zk.EPHEMERAL
        if c == 's': f |= zk.SEQUENCE
    return f
    
class Coordinator(object):
    def __init__(self, zkhosts, root=NODE_HQ_ROOT, alivenode='alive'):
        """zkhosts: a string or a list. list will be ','.join-ed into a string.
        root: root node path (any parents must exist, if any)
        """
        
        if not isinstance(zkhosts, basestring):
            zkhosts = ','.join(zkhosts)
        self.ROOT = root
        self.alivenode = alivenode
        self.nodename = os.uname()[1]

        self.NODE_SERVERS = self.ROOT + '/servers'
        self.NODE_ME = self.NODE_SERVERS+'/'+self.nodename

        self.zh = zk.init(zkhosts, self.__watcher)

        self.jobs = {}

        self.initialize_node_structure()

        if not zk.exists(self.zh, self.NODE_ME):
            zk.create(self.zh, self.NODE_ME, '', PERM_WORLD)

        # setup notification
        zk.get_children(self.zh, self.NODE_SERVERS, self.__servers_watcher)
        #zk.get_children(self.zh, self.NODE_ME, self.__watcher)

        self.NODE_JOBS = self.NODE_ME+'/jobs'
        zk.acreate(self.zh, self.NODE_JOBS, '', PERM_WORLD)

        #self.publish_alive()

    def create(self, path, data='', perm=PERM_WORLD, flags=''):
        return zk.create(self.zh, path, data, perm, create_flags(flags))
    def acreate(self, path, data='', perm=PERM_WORLD, flags=''):
        try:
            return zk.acreate(self.zh, path, data, perm, create_flags(flags))
        except SystemError:
            # acreate C code often returns error without setting exception,
            # which cuases SystemError.
            pass

    def initialize_node_structure(self):
        if not zk.exists(self.zh, self.ROOT):
            self.create(self.ROOT)

        if not zk.exists(self.zh, self.NODE_SERVERS):
            self.create(self.NODE_SERVERS)

        self.NODE_GJOBS = self.ROOT + '/jobs'
        if not zk.exists(self.zh, self.NODE_GJOBS):
            self.create(self.NODE_GJOBS)
        
    def __watcher(self, zh, evtype, state, path):
        """connection/session watcher"""
        logging.debug('event%s', str((evtype, state, path)))
        if evtype == zk.SESSION_EVENT:
            if state == zk.CONNECTED_STATE:
                self.publish_alive()
            elif state == zk.EXPIRED_SESSION_STATE:
                # unrecoverable state - close and reconnect
                zk.close(self.zh)
                self.zh = zk.init(zkhosts, self.__watcher)

    def __servers_watcher(self, zh, evtype, state, path):
        try:
            ch = zk.get_children(self.zh, self.NODE_SERVERS, self.__servers_watcher)
            logging.info('servers added/removed:%s', str(ch))
        except zk.ZooKeeperException:
            logging.warn('zk.get_children failed', exc_info=1)
        
    def publish_alive(self):
        node_alive = self.NODE_ME+'/'+self.alivenode
        self.acreate(node_alive, flags='e')

    def publish_job(self, job):
        '''job: hq.CrawlJob'''
        ju = self.jobs.get(job)
        # update 10 minutes interval
        if ju is None or ju < time.time() - 10*60:
            NODE_JOB = self.NODE_JOBS+'/'+job.jobname
            def set_complete(zh, rc, pv):
                #print >>sys.stderr, "aset completed: %s" % str(args)
                if rc == zk.NONODE:
                    # does not exist yet - create anew
                    zk.acreate(zh, NODE_JOB, '', PERM_WORLD)
            try:
                zk.aset(self.zh, NODE_JOB, '', -1, set_complete)
            except:
                logging.warn('aset failed', exc_info=1)
                pass

            node2 = self.NODE_GJOBS+'/'+job.jobname
            self.acreate(node2)
            self.acreate('/'.join((self.NODE_GJOBS, job.jobname, self.nodename)),
                         flags='e')
            self.jobs[job] = time.time()

        node = self.NODE_GJOBS+'/'+job.jobname
        self.acreate(node)
        self.acreate('/'.join((self.NODE_GJOBS, job.jobname, self.nodename)),
                     flags='e')

    def publish_client(self, job, client):
        pass
    
    def get_servers(self):
        return zk.get_children(self.zh, self.NODE_SERVERS)

    def get_status_of(self, server=None):
        server = server or self.nodename
        status = dict(name=server)
        try:
            node = zk.get(self.zh, self.NODE_SERVERS+'/'+server+'/alive')
            status['alive'] = node[1]
        except zk.NoNodeException:
            pass
        try:
            jobspath = self.NODE_SERVERS+'/'+server+'/jobs'
            jobs = zk.get_children(self.zh, jobspath)
            status['jobs'] = []
            for j in jobs:
                jobpath = jobspath+'/'+j
                nodevals = zk.get(self.zh, jobpath)
                mtime = nodevals[1]['mtime']
                status['jobs'].append(dict(name=j, ts=mtime/1000.0))
        except zk.NoNodeException:
            status['jobs'] = null;
        return status

    def get_servers_status(self):
        return [self.get_status_of(server) for server in self.get_servers()]

    def shutdown(self):
        if self.zh:
            zk.close(self.zh)
            self.zh = None
    
