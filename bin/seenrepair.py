#!/usr/bin/python
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '../lib'))
import leveldb

leveldb.IntHash.repair_db('/1/crawling/hq/seen')

