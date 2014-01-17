import sys, os
import re
from kazoo.client import (KazooClient, KazooState)
from kazoo.exceptions import (ZookeeperError, NoNodeError)
import logging
import time

__all__ = ['Coordinator']

NODE_HQ_ROOT = '/hq'

class Coordinator(object):
    def __init__(self, zkhosts, root=NODE_HQ_ROOT, alivenode='alive',
                 readonly=False, role=None):
        """zkhosts: a string or a list. list will be ','.join-ed into a string.
        root: root node path (any parents must exist, if any)
        """
        self.LOGGER = logging.getLogger('hq.zkcoord')
        
        if not isinstance(zkhosts, basestring):
            zkhosts = ','.join(zkhosts)
        self.zkhosts = zkhosts
        self.ROOT = root
        self.alivenode = alivenode
        self.readonly = readonly
        self.nodename = os.uname()[1]

        self.NODE_SERVERS = self.ROOT + '/servers'
        self.NODE_ME = self.NODE_SERVERS+'/'+self.nodename
        self.NODE_MYJOBS = self.NODE_ME + '/jobs'
        self.NODE_GJOBS = self.ROOT + '/jobs'

        self.__listeners = {}

        self.zh = None
        self.zstate = None

        self._connect()

    def _connect(self):
        try:
            if self.zh: self.zh.stop()
            self.LOGGER.debug("connecting to %s", self.zkhosts)
            self.zh = KazooClient(hosts=self.zkhosts)
            self.zh.add_listener(self.__watcher)
            # this will wait until connection is established.
            self.zh.start()
        except ZookeeperError as ex:
            self.zh = None
            self.zkerror = ex

    def _initialize(self):
        if not self.readonly:
            if self.zstate is None:
                self.zh.ensure_path(self.NODE_SERVERS)
                self.zh.ensure_path(self.NODE_GJOBS)
            self.publish_alive()
            if not self.zh.exists(self.NODE_ME):
                self.zh.create(self.NODE_ME, '')
            if not self.zh.exists(self.NODE_MYJOBS):
                self.zh.acreate(self.NODE_MYJOBS)
        # setup notifications
        self.zh.get_children(self.NODE_SERVERS, self.__servers_watcher)
        self.zh.get_children(self.NODE_GJOBS, self.__jobs_watcher)

    def __watcher(self, state):
        # client level callback method. this method should not block.
        if state == KazooState.LOST:
            # session expiration
            self.zstate = state
        elif state == KazooState.SUSPENDED:
            # disconnected, session is still alive
            self.zstate = state
        else:
            # (re)connected
            self.LOGGER.debug("connected")
            self.zh.handler.spawn(self._initialize)
            self.zstate = state

    def get_status_text(self):
        return self.zkerror

    def create(self, path, data=''): #, perm=PERM_WORLD, flags=''):
        return self.zh.create(path, data)
    def acreate(self, path, data=''): #, perm=PERM_WORLD, flags=''):
        return self.zh.acreate(path, data)
    def exists(self, path):
        return self.zh.exists(path)
    def delete(self, path):
        try:
            return self.zh.delete(path)
        except NoNodeError as ex:
            pass
    def get_children(self, path, watch=None):
        return self.zh.get_children(path, watch=watch)

    def __servers_watcher(self, zh, evtype, state, path):
        """called when HQ servers are added / dropped."""
        try:
            ch = self.get_children(self.NODE_SERVERS,
                                   watch=self.__servers_watcher)
            self.LOGGER.info('servers added/removed:%s', str(ch))
            self.fire_event('serverschanged')
        except ZookeeperError as ex:
            self.LOGGER.warn('zk.get_children(%r) failed', self.NODE_SERVERS,
                             exc_info=1)
        
    def __jobs_watcher(self, zh, evtype, state, path):
        """called when jobs are added / dropped."""
        try:
            self.LOGGER.info('%s children changed', self.NODE_GJOBS)
            ch = self.get_children(self.NODE_GJOBS, watch=self.__jobs_watcher)
            self.fire_event('jobschanged')
        except ZooKeeperError as ex:
            self.LOGGER.warn('get_children(%r) failed', self.NODE_GJOBS,
                             exc_info=1)

    def publish_alive(self):
        node_alive = self.NODE_ME+'/'+self.alivenode
        self.zh.create_async(node_alive, ephemeral=True)

    def publish_job(self, job):
        '''job: hq.CrawlJob'''
        ju = self.jobs.get(job)
        # update 10 minutes interval
        if ju is None or ju < time.time() - 10*60:
            NODE_MYJOB = self.NODE_MYJOBS+'/'+job.jobname
            def set_complete(a):
                #print >>sys.stderr, "aset completed: %s" % str(args)
                if a.exception == NoNodeError:
                    # node does not exist yet - create anew
                    self.zh.create_async(NODE_MYJOB, '')
            try:
                a = self.zh.set_async(NODE_MYJOB, '')
                a.rawlink(set_complete)
            except:
                self.LOGGER.warn('aset failed', exc_info=1)
                pass

            node2 = self.NODE_GJOBS+'/'+job.jobname
            self.zh.create_async(node2)
            self.zh.create_async('{}/{}/{}'.format(self.NODE_GJOBS,
                                                   job.jobname,
                                                   self.nodename),
                                 ephemeral=True)
            self.jobs[job] = time.time()

    def publish_client(self, job, client):
        pass
    
    def get_servers(self):
        return self.zh.get_children(self.NODE_SERVERS)

    def get_server_job(self, server, job):
        p = self.NODE_SERVERS+'/'+server+'/jobs/'+job
        j = dict()
        try:
            nodeval = self.zh.get(p)
            attr = nodeval[1]
            j['ts'] = attr['mtime'] / 1000.0
        except NoNodeException, ex:
            j['ts'] = 0
        return j
            
    def get_status_of(self, server=None, jobs=None):
        if self.zh is None: return None
        server = server or self.nodename
        status = dict(name=server)
        try:
            node = self.zh.get(self.NODE_SERVERS+'/'+server+'/alive')
            status['alive'] = node[1]
        except NoNodeError as ex:
            status['alive'] = False

        jobspath = self.NODE_SERVERS+'/'+server+'/jobs'
        if jobs is None:
            try:
                jobs = self.get_children(jobspath)
            except NoNodeError:
                jobs = []
        elif isinstance(jobs, basestring):
            jobs = [jobs]
        status['jobs'] = []
        for j in jobs:
            jobj = self.get_server_job(server, j)
            jobj['name'] = j
            status['jobs'].append(jobj)
        return status

    def get_servers_status(self):
        return [self.get_status_of(server) for server in self.get_servers()]

    def get_job_servers(self, jobname):
        """return a map of integer identifier to server name, which
        is configured for the job jobname.
        """
        p = self.NODE_GJOBS+'/'+jobname+'/servers'
        try:
            svids = self.get_children(p)
        except NoNodeError as ex:
            return {}
            
        servers = {}
        for name in svids:
            try:
                svid = int(name)
            except:
                continue
            nodevals = self.zh.get(p+'/'+str(svid))
            if nodevals[0]:
                servers[svid] = nodevals[0]
        return servers

    def get_job_servers2(self, jobname):
        """returns servers for job "jobname", including those active
        but not registered at jobs/JOBNAME/servers. elements of returned
        list are dict with svid and name keys. svid key only exists for
        those registered for the "jobname".
        """
        servers = [dict(svid=svid, name=name)
                   for svid, name in self.get_job_servers(jobname).items()]
        regservers = set(s['name'] for s in servers)

        try:
            p = self.NODE_SERVERS
            ss = self.get_children(self.NODE_SERVERS)
            for s in ss:
                if s in regservers: continue
                if not self.is_server_alive(s): continue
                p = self.NODE_SERVERS+'/'+s+'/jobs/'+jobname
                if self.exists(p):
                    servers.append(dict(name=s))
        except ZookeeperError as ex:
            self.LOGGER.debug('zookeeper access failed', exc_info=1)
        
        return servers

    def add_job_server(self, job, server):
        pass

    def delete_job_server(self, job, server):
        jobservers = dict((v, k) for k, v in self.get_job_servers(job).items())
        if server in jobservers:
            p = self.NODE_GJOBS+'/'+job+'/servers/'+jobservers[server]
            self.delete(p)

        p = self.NODE_SERVERS+'/'+server+'/jobs/'+job
        # assumption: there's no child under the server/job node.
        try:
            self.delete(p)
        except NoNodeException, ex:
            pass
        except NotEmptyException, ex:
            # XXX
            pass

    def is_server_alive(self, server):
        p = self.NODE_SERVERS+'/'+server+'/alive'
        return self.exists(p)

    def add_listener(self, ev, listener):
        if not isinstance(ev, basestring):
            raise ValueError, 'ev must be a string'
        ll = self.__listeners.get(ev)
        if not ll:
            self.__listeners[ev] = set((listener,))
        else:
            ll.add(listener)
        
    def remove_listener(self, ev, listener):
        if not isinstance(ev, basestring):
            raise ValueError, 'ev must be a string'
        ll = self.__listeners.get(ev)
        if ll:
            try:
                ll.remove(listener)
            except KeyError:
                pass

    def fire_event(self, ev, *args):
        ll = self.__listeners.get(ev)
        if ll:
            for listener in ll:
                try:
                    listener(*args)
                except:
                    self.LOGGER.warn('error running listener %r '
                                     'with ev=%r, args=%r', listener, ev, args,
                                     exc_info=1)
            
    def shutdown(self):
        if self.zh:
            self.zh.stop()
            self.zh = None
    

if __name__ == '__main__':
    # for quick testing
    logging.basicConfig(level=logging.DEBUG)
    zkhosts = sys.argv[1]
    coord = Coordinator(zkhosts, readonly=True)
    logging.debug("coord=%s", coord)

    logging.debug("get_servers()")
    print coord.get_servers()

    print >>sys.stderr, "shutting down {}".format(coord)
    coord.shutdown()
