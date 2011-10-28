import sys, os
import re
import zookeeper as zk

NODE_HQ_ROOT = '/hq'
NODE_SERVERS = NODE_HQ_ROOT+'/servers'

PERM_WORLD = [dict(perms=zk.PERM_ALL,scheme='world',id='anyone')]

class Coordinator(object):
    def __init__(self, zkhosts):
        if not isinstance(zkhosts, basestring):
            zkhosts = ','.join(zkhosts)
        self.nodename = os.uname()[1]
        self.zh = zk.init(zkhosts)

        if not zk.exists(self.zh, NODE_HQ_ROOT):
            zk.create(self.zh, NODE_HQ_ROOT, '', PERM_WORLD)
        # TODO: setup notification
        if not zk.exists(self.zh, NODE_SERVERS):
            zk.create(self.zh, NODE_SERVERS, '', PERM_WORLD)

        self.node_me = NODE_SERVERS+'/'+self.nodename
        if not zk.exists(self.zh, self.node_me):
            zk.create(self.zh, self.node_me, '', PERM_WORLD)

        self.node_jobs = self.node_me+'/jobs'
        zk.acreate(self.zh, self.node_jobs, '', PERM_WORLD)

        self.publish_alive()

    def publish_alive(self):
        node_alive = self.node_me+'/alive'
        zk.acreate(self.zh, node_alive, '', PERM_WORLD, zk.EPHEMERAL)

    def publish_job(self, job):
        '''job: hq.CrawlJob'''
        node = self.node_jobs+'/'+job.jobname
        zk.acreate(self.zh, node, '', PERM_WORLD)

    def publish_client(self, job, client):
        pass
    
    def get_servers(self):
        return zk.get_children(self.zh, NODE_SERVERS)

    def get_status_of(self, server):
        status = dict(name=server)
        try:
            node = zk.get(self.zh, NODE_SERVERS+'/'+server+'/alive')
            status['alive'] = node[1]
        except zk.NoNodeException:
            pass
        try:
            jobs = zk.get_children(self.zh, NODE_SERVERS+'/'+server+'/jobs')
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
    
