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
    def __init__(self, zkhosts, root=NODE_HQ_ROOT, alivenode='alive',
                 readonly=False, role=None):
        """zkhosts: a string or a list. list will be ','.join-ed into a string.
        root: root node path (any parents must exist, if any)
        """
        
        if not isinstance(zkhosts, basestring):
            zkhosts = ','.join(zkhosts)
        self.zkhosts = zkhosts
        self.ROOT = root
        self.alivenode = alivenode
        self.readonly = readonly
        self.nodename = os.uname()[1]

        self.NODE_SERVERS = self.ROOT + '/servers'
        self.NODE_ME = self.NODE_SERVERS+'/'+self.nodename
        self.NODE_GJOBS = self.ROOT + '/jobs'

        self.__listeners = {}

        # allow system to start without ZooKeeper
        if self._connect():

            self.jobs = {}

            if not readonly:
                self.initialize_node_structure()

                if not zk.exists(self.zh, self.NODE_ME):
                    zk.create(self.zh, self.NODE_ME, '', PERM_WORLD)

                self.NODE_MYJOBS = self.NODE_ME+'/jobs'
                if not zk.exists(self.zh, self.NODE_MYJOBS):
                    zk.acreate(self.zh, self.NODE_MYJOBS, '', PERM_WORLD)

            # setup notification
            zk.get_children(self.zh, self.NODE_SERVERS, self.__servers_watcher)
            zk.get_children(self.zh, self.NODE_GJOBS, self.__jobs_watcher)
            #zk.get_children(self.zh, self.NODE_ME, self.__watcher)

            #self.publish_alive()

    def _connect(self):
        try:
            self.zh = zk.init(self.zkhosts, self.__watcher)
            self.zkerror = None
            return True
        except zk.ZooKeeperException, ex:
            self.zh = None
            self.zkerror = ex
            return False

    def get_status_text(self):
        return self.zkerror

    def create(self, path, data='', perm=PERM_WORLD, flags=''):
        return zk.create(self.zh, path, data, perm, create_flags(flags))
    def acreate(self, path, data='', perm=PERM_WORLD, flags=''):
        try:
            return zk.acreate(self.zh, path, data, perm, create_flags(flags))
        except SystemError:
            # acreate C code often returns error without setting exception,
            # which cuases SystemError.
            pass
    def delete(self, path):
        try:
            return zk.delete(self.zh, path)
        except zk.NoNodeException, ex:
            pass

    def initialize_node_structure(self):
        for p in (self.ROOT, self.NODE_SERVERS, self.NODE_GJOBS):
            if not zk.exists(self.zh, p):
                self.create(p)
        
    def __watcher(self, zh, evtype, state, path):
        """connection/session watcher"""
        logging.debug('event%s', str((evtype, state, path)))
        if evtype == zk.SESSION_EVENT:
            if state == zk.CONNECTED_STATE:
                self.publish_alive()
            elif state == zk.EXPIRED_SESSION_STATE:
                # unrecoverable state - close and reconnect
                zk.close(self.zh)
                self._connect()

    def __servers_watcher(self, zh, evtype, state, path):
        try:
            ch = zk.get_children(self.zh, self.NODE_SERVERS,
                                 self.__servers_watcher)
            logging.info('servers added/removed:%s', str(ch))
            self.fire_event('serverschanged')
        except zk.ZooKeeperException:
            logging.warn('zk.get_children(%r) failed', self.NODE_SERVERS,
                         exc_info=1)
        
    def __jobs_watcher(self, zh, evtype, state, path):
        try:
            logging.info('%s children changed', self.NODE_GJOBS)
            ch = zk.get_children(self.zh, self.NODE_GJOBS,
                                 self.__jobs_watcher)
            self.fire_event('jobschanged')
        except zk.ZooKeeperException:
            logging.warn('zk.get_children(%r) failed', self.NODE_GJOBS,
                         exc_info=1)

    def publish_alive(self):
        node_alive = self.NODE_ME+'/'+self.alivenode
        self.acreate(node_alive, flags='e')

    def publish_job(self, job):
        '''job: hq.CrawlJob'''
        ju = self.jobs.get(job)
        # update 10 minutes interval
        if ju is None or ju < time.time() - 10*60:
            NODE_MYJOB = self.NODE_MYJOBS+'/'+job.jobname
            def set_complete(zh, rc, pv):
                #print >>sys.stderr, "aset completed: %s" % str(args)
                if rc == zk.NONODE:
                    # does not exist yet - create anew
                    zk.acreate(zh, NODE_MYJOB, '', PERM_WORLD)
            try:
                zk.aset(self.zh, NODE_MYJOB, '', -1, set_complete)
            except:
                logging.warn('aset failed', exc_info=1)
                pass

            node2 = self.NODE_GJOBS+'/'+job.jobname
            self.acreate(node2)
            self.acreate('/'.join((self.NODE_GJOBS, job.jobname, self.nodename)),
                         flags='e')
            self.jobs[job] = time.time()

    def publish_client(self, job, client):
        pass
    
    def get_servers(self):
        return zk.get_children(self.zh, self.NODE_SERVERS)

    def get_server_job(self, server, job):
        p = self.NODE_SERVERS+'/'+server+'/jobs/'+job
        j = dict()
        try:
            nodeval = zk.get(self.zh, p)
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
            node = zk.get(self.zh, self.NODE_SERVERS+'/'+server+'/alive')
            status['alive'] = node[1]
        except zk.NoNodeException:
            status['alive'] = False

        jobspath = self.NODE_SERVERS+'/'+server+'/jobs'
        if jobs is None:
            try:
                jobs = zk.get_children(self.zh, jobspath)
            except zk.NoNodeException:
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
            svids = zk.get_children(self.zh, p)
        except zk.NoNodeException, ex:
            return {}
            
        servers = {}
        for name in svids:
            try:
                svid = int(name)
            except:
                continue
            nodevals = zk.get(self.zh, p+'/'+str(svid))
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
            ss = zk.get_children(self.zh, self.NODE_SERVERS)
            for s in ss:
                if s in regservers: continue
                if not self.is_server_alive(s): continue
                p = self.NODE_SERVERS+'/'+s+'/jobs/'+jobname
                if zk.exists(self.zh, p):
                    servers.append(dict(name=s))
        except zk.ZooKeeperException, ex:
            logging.debug('zookeeper access failed', exc_info=1)
        
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
        return zk.exists(self.zh, p)

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
                    logging.warn('error running listener %r '
                                 'with ev=%r, args=%r', listener, ev, args,
                                 exc_info=1)
            
    def shutdown(self):
        if self.zh:
            zk.close(self.zh)
            self.zh = None
    
