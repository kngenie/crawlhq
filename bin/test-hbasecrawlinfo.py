#!/usr/bin/python
#
import sys
sys.path[0:0] = ('/opt/hq/lib',)
import os
import re
from optparse import OptionParser
import time

import filequeue
from hbasecrawlinfo import CrawlInfo

opt = OptionParser()
options, args = opt.parse_args()

dq = filequeue.QueueFileReader(args[0], noupdate=True)
ci = CrawlInfo('crawl437.us.archive.org')

count = 0
t0 = time.time()
while 1:
    u = next(dq, None)
    if not u: break
    a = ci.get_crawlinfo(u)
    print "%s %s" % (u['u'], a)
    count += 1
t = time.time() - t0
print "%d in %.3fs, %.3f/s" % (count, t, count / t)


