#!/usr/bin/python
#
import os, sys
sys.path[0:0] = ('/opt/hq/lib',)
import re
import json
import time
from cfpgenerator import FPGenerator
import traceback
import threading

_fp64 = FPGenerator(0xD74307D3FD3382DB, 64)
S64 = 1 << 63

def urifp(uri):
    uhash = _fp64.sfp(uri)
    # if uhash is 64bit negative number, make it so in Python, too
    #if uhash & S64: uhash = int(uhash & (S64 - 1)) - S64
    return uhash

class Worker(object):
    def __init__(self, qdir):
        self.st = dict(all=0, novel=0)
        self.seen = {}
        self.qdir = qdir

    def run(self):
        self.st['start'] = time.time()
        qfiles = [f for f in os.listdir(self.qdir) if re.match(r'\d+$', f)]
        try:
            for qfile in qfiles:
                f = open(os.path.join(self.qdir, qfile), 'r')
                ln = 0
                for l in f:
                    ln += 1
                    if len(l) < 2 or l[1] != '{': continue
                    self.st['all'] += 1
                    try:
                        curi = json.loads(l[1:].rstrip())
                        hash = urifp(curi['u'])
                    except:
                        print >>sys.stderr, "%s(%d):%s" % (qfile, ln, l)
                        raise
                    if hash not in self.seen:
                        self.st['novel'] += 1
                        self.seen[hash] = 1
                f.close()
        except:
            traceback.print_exc()
        self.st.update(end=time.time())


# main
worker = Worker(sys.argv[1])
th = threading.Thread(target=worker.run)
th.start()

while th.is_alive:
    th.join(1.0)
    now = time.time()
    st = worker.st
    print "%s %.1f%% novel, %.1f URI/s" % (
        str(st), 
        float(st['novel']) / float(st['all']) * 100,
        float(st['all']) / (now - st['start'])
        )



