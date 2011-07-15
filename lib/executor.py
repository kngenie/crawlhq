import sys
from Queue import Queue, Empty
import threading
import traceback

class ThreadPoolExecutor(object):
    '''thread pool executor with fixed number of threads'''
    def __init__(self, poolsize=15, queuesize=5000):
        self.poolsize = poolsize
        self.queuesize = queuesize
        self.work_queue = Queue(self.queuesize)
        self.__shutdown = False
        self.workers = [
            threading.Thread(target=self._work, name="poolthread-%d" % i)
            for i in range(self.poolsize)
            ]
        for th in self.workers:
            th.daemon = True
            th.start()

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        if self.__shutdown: return
        print >>sys.stderr, "ThreadPoolExecutor(%s) shutting down" % id(self)
        self.__shutdown = True
        def NOP(): pass
        for th in self.workers:
            self.work_queue.put((NOP,[]))
        for th in self.workers:
            # wait up to 5 secs for thread to terminate (too long?)
            th.join(5.0)

    def _work(self):
        thname = threading.current_thread().name
        # print >>sys.stderr,"%s starting _work" % thname
        while 1:
            f, args = None, None
            try:
                f, args = self.work_queue.get()
                # print >>sys.stderr, "%s work starting %s%s" % (thname, f, args)
                f(*args)
                # print >>sys.stderr, "%s work done %s%s" % (thname, f, args)
            except Exception as ex:
                print >>sys.stderr, "work error in %s%s" % (f, args)
                traceback.print_exc()
            if self.__shutdown:
                break

    def execute(self, f, *args):
        self.work_queue.put((f, args))

    def __call__(self, f, *args):
        self.work_queue.put((f, args))

class TaskBucket(object):
    def __init__(self, tasks):
        self.tasks = Queue()
        for t in tasks:
            self.tasks.put(t)
    def thread_execute(self):
        while 1:
            try:
                f, args = self.tasks.get_nowait()
                try:
                    f(*args)
                except Exception as ex:
                    print >>sys.stderr, "work error in %s%s" % (f, args)
                    traceback.print_exc()
                self.tasks.task_done()
            except Empty:
                break
    def buckets(self, n=2):
        return [(self.thread_execute,)
                for i in xrange(min(n, self.tasks.qsize))]
    def wait(self):
        self.tasks.join()

    def execute_wait(self, executor, n):
        for t in self.buckets(n):
            executor.execute(*t)
        self.wait()

