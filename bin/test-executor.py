#!/usr/bin/python
import sys
sys.path.append('/opt/hq/lib')
import os, re
from executor import *
import threading

executor = ThreadPoolExecutor(poolsize=20)

used_threads = set()

def task(n):
    print >>sys.stderr, n
    used_threads.add(threading.current_thread().name)

b = TaskBucket([(task, (i,)) for i in xrange(200)])
for t in b.buckets(3):
    executor.execute(*t)
b.wait()

print >>sys.stderr, "done"
print >>sys.stderr, "threads=%s" % used_threads
assert len(used_threads) <= 3



