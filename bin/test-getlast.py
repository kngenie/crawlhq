#!/usr/bin/python
import os, sys
import pymongo
from optparse import OptionParser
import time

opt = OptionParser()
options, args = opt.parse_args()

conn = pymongo.Connection()
db = conn.crawl
seen = db.seen.wide

REPORT_INTERVAL = 5.0
qfile = args[0]
f = open(qfile)
processed_count = 0
exists_count = 0
last_update = time.time()
last_processed_count = 0
for l in f:
    hash = int(l.rstrip())
    data = seen.find_one({'_id': hash}, {'a': 1})
    processed_count += 1
    if data: exists_count += 1

    now = time.time()
    if now >= last_update + REPORT_INTERVAL:
        ups = int((processed_count - last_processed_count) / (now - last_update))
        print "processed=%d (%d URI/s)" % (processed_count, ups)
        last_update = now
        last_processed_count = processed_count
