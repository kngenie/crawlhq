# HTTP handlers mix-in
import os
import web
import time
import json
import logging

import weblib

# Dependencies
# self.hq: global Headquarter singleton
# self.decode_object(): provided by weblib.QueryApp
#
class DiscoveredHandler(object):

    def post_discovered(self, job):
        '''receives URLs found as 'outlinks' in just downloaded page.
        do_discovered runs already-seen test and then schedule a URL
        for crawling with last-modified and content-hash obtained
        from seen database (if previously crawled)'''
        
        p = web.input(force=0)
        if 'u' not in p:
            return {error:'u value missing'}

        furi = dict(u=p.u)
        for k in ('p', 'v', 'x'):
            if k in p and p[k] is not None: furi[k] = p[k]

        try:
            cj = self.hq.get_job(job)
        except Exception as ex:
            raise web.notfound(json.dumps(dict(err=str(ex))))

        if p.force:
            return cj.schedule([furi])
        else:
            return cj.discovered([furi])

    def post_mdiscovered(self, job):
        '''receives submission of "discovered" events in batch.
        this version simply queues data submitted in incoming queue
        to minimize response time. entries in the incoming queue
        will be processed by separate processinq call.'''
        result = dict(processed=0)
        data = None
        try:
            data = web.data()
            curis = json.loads(self.decode_content(data))
        except:
            web.debug("json.loads error:data=%s" % data)
            result['error'] = 'invalid data - json parse failed'
            return result
        if isinstance(curis, dict):
            force = curis.get('f')
            curis = curis['u']
        elif isinstance(curis, list):
            force = False
        else:
            result['error'] = 'invalid data - not an array'
            return result
        if len(curis) == 0:
            return result

        try:
            cj = self.hq.get_job(job)
        except Exception as ex:
            logging.exception('get_job(%r) failed', job)
            result['error'] = str(ex)
            raise web.notfound(json.dumps(result))

        start = time.time()
        if force:
            for curi in curis:
                curi['f'] = 1
            result.update(cj.discovered(curis))
        else:
            result.update(cj.discovered(curis))
        t = time.time() - start
        result.update(t=t)
        if t / len(curis) > 1e-3:
            logging.warn("slow discovered: %.3fs for %d", t, len(curis))
        else:
            logging.debug("mdiscovered %s", result)
        return result

    def do_flush(self, job):
        '''flushes cached objects into database for safe shutdown'''
        try:
            self.hq.get_job(job).flush()
        except Exception, ex:
            return dict(success=0, worker=os.getpid(), err=str(ex))
        r = dict(success=1, worker=os.getpid())
        return r

class DispatcherAPI(weblib.QueryApp):
    def do_processinq(self, job):
        """process incoming queue. max parameter advise uppoer limit on
        number of URIs processed. actually processed URIs may exceed that
        number if incoming queue is storing URIs in chunks
        """
        p = web.input(max=500)
        maxn = int(p.max)
        result = dict(job=job, inq=0, processed=0, scheduled=0, max=maxn,
                      td=0.0, ts=0.0)
        start = time.time()

        result.update(self.get_job(job).processinq(maxn))
        
        result.update(t=(time.time() - start))
        return result
