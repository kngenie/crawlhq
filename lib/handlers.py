# HTTP handlers mix-in
import os
import web
import time
import json
import logging

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
            # TODO: return 404?
            return dict(err=str(ex))

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
            result['error'] = str(ex)
            return result

        start = time.time()
        if force:
            result.update(cj.schedule(curis))
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
        self.hq.get_job(job).flush()
        r = dict(ok=1, worker=os.getpid())
        return r
