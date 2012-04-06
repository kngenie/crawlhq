#!/usr/bin/python
#
"""script for rebuilding seen database from crawl.log"""

import os
import sys
sys.path[0:0] = (os.path.join(os.path.dirname(__file__), '../lib'),)
import re
from gzip import GzipFile
from optparse import OptionParser

import hqconfig
#from dispatcher import Seen
import urihash
from seen import Seen

def parse_crawllog(line):
    fields = re.split(r'\s+', line)
    if len(fields) < 11:
        raise Exception, 'must have 11 fields at least'
    ts, code, size, uri, path, via, ct, th, stel, digest, tags = fields[:11]
    if int(code) <= 0 or size== '-':
        return None
    if re.match(r'.*/robots.txt$', uri):
        return None
    if re.match(r'dns:', uri):
        return None
    data = dict(s=code)
    curi = dict(u=uri, a=data)
    if digest != '-': data.update(d=digest)
    # there's no last-modified info, so use fetch start time as a substitute
    try:
        start = time.strptime(stel[:14], '%Y%m%d%H%M%S')
        startts = calendar.timegm(start)
        data.update(m=startts)
    except:
        pass
    # there's no etag info
    return curi

class BatchSeenWriter(object):
    def __init__(self, dbdir):
        self.seen = Seen(dbdir, options.cachesize*(1024*1024))
        self.buffer = []

    def put(self, curi):
        curi['id'] = urihash.urikey(curi['u'])
        self.buffer.append(curi)
        if len(self.buffer) >= options.batchsize:
            self.flush()

    def flush(self):
        self.buffer.sort(lambda x, y: cmp(x['id'], y['id']))
        for u in self.buffer:
            self.seen.already_seen(u)
        self.buffer = []

    def processfile(self, fn):
        print >>sys.stderr, fn
        count = 0
        f = GzipFile(fn) if fn.endswith('.gz') else open(fn)
        for l in f:
            try:
                o = parse_crawllog(l)
            except Exception as ex:
                print >>sys.stderr, "skipped %s" % str(ex)
                continue
            if o is None:
                continue
            self.put(o)
            count += 1
            print >>sys.stderr, "\r%d" % count,
        f.close()
        self.flush()
        sys.stderr.write("\n")

opt = OptionParser(usage='%prog [OPTIONS] JOB crawl.log...')
opt.add_option('-C', action='store', dest='cachesize', type='int',
               default=1024,
               help='LevelDB block cache size in MiB (default 1024)')
opt.add_option('-b', action='store', dest='batchsize', type='int',
               default=1000000,
               help='number of URIs to process in a batch (default 1M)')

options, args = opt.parse_args()
if len(args) < 1:
    opt.error('specify JOB')
job = args.pop(0)

batchseen = BatchSeenWriter(hqconfig.seendir(job))
for fn in args:
    batchseen.processfile(fn)
