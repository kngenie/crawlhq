import sys, os
import re
import zookeeper as zk
import logging

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
        return zk.acreate(self.zh, path, data, perm, create_flags(flags))

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
        if evtype == zk.SESSION_EVENT and state == zk.CONNECTED_STATE:
            self.publish_alive()

    def __servers_watcher(self, zh, evtype, state, path):
        ch = zk.get_children(self.zh, self.NODE_SERVERS, self.__servers_watcher)
        logging.info('servers added/removed:%s', str(ch))
        
    def publish_alive(self):
        node_alive = self.NODE_ME+'/'+self.alivenode
        self.acreate(node_alive, flags='e')

    def publish_job(self, job):
        '''job: hq.CrawlJob'''
        node = self.NODE_JOBS+'/'+job.jobname
        zk.acreate(self.zh, node, '', PERM_WORLD)

        node = self.NODE_GJOBS+'/'+job.jobname
        self.acreate(node)
        self.acreate('/'.join((self.NODE_GJOBS, job.jobname, self.nodename)),
                     flags='e')

    def publish_client(self, job, client):
        pass
    
    def get_servers(self):
        return zk.get_children(self.zh, self.NODE_SERVERS)

    def get_status_of(self, server):
        status = dict(name=server)
        try:
            node = zk.get(self.zh, self.NODE_SERVERS+'/'+server+'/alive')
            status['alive'] = node[1]
        except zk.NoNodeException:
            pass
        try:
            jobs = zk.get_children(self.zh, self.NODE_SERVERS+'/'+server+'/jobs')
            status['jobs'] = [dict(name=j) for j in jobs]
        except zk.NoNodeException:
            status['jobs'] = null;
        return status

    def get_servers_status(self):
        return [self.get_status_of(server) for server in self.get_servers()]

    def shutdown(self):
        if self.zh:
            zk.close(self.zh)
            self.zh = None
    
