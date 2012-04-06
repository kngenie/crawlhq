#!/usr/bin/python

import sys, os
sys.path[0:0] = (os.path.join(os.path.dirname(__file__), '../lib'),)
from filequeue import QueueFileReader

dq = QueueFileReader(sys.argv[1])
while 1:
    o = next(dq, None)
    if not o: break
    print o
dq.close()
