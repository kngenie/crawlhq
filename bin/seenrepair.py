#!/usr/bin/python
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '../lib'))
import leveldb
job = 'wide'
if len(sys.argv) > 1: job = sys.argv[1]
leveldb.IntHash.repair_db(os.path.join('/1/crawling/hq/seen', job))

