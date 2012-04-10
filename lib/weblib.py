import os
import sys
import json

import web
from cStringIO import StringIO
from gzip import GzipFile
import logging

def lref(name):
    """returns local path for script-relative resource 'name'
    """
    # note: SCRIPT_FILENAME under lighttpd is broken. you'd need
    # to create a symbolic link from /var/www
    if 'SCRIPT_FILENAME' not in web.ctx.environ:
        return os.path.join(sys.path[0], name)
    path = web.ctx.environ['SCRIPT_FILENAME']
    return os.path.join(os.path.dirname(path), name)

class BaseApp(object):
    def __init__(self):
        tglobals = dict(format=format)
        try:
            tmpldir = web.ctx.environ.get('TMPLDIR') or lref('t')
            self.templates = web.template.render(tmpldir, globals=tglobals)
        except:
            logging.critical('BaseApp.__init__ failed', exc_info=1)

    def render(self, tmpl, *args, **kw):
        # note self.templates[tmpl] does not work.
        t = getattr(self.templates, tmpl)
        return t(*args)
    
class QueryApp(object):
    def _dispatch(self, method, *args):
        f = getattr(self, method, None)
        if not f: raise web.notfound('bad action %s on %s' % method)
        r = f(*args)
        if isinstance(r, dict):
            r = json.dumps(r, check_circular=False, separators=',:') + '\n'
            web.header('content-type', 'text/json')
        return r

    def decode_content(self, data):
        if web.ctx.env.get('HTTP_CONTENT_ENCODING') == 'gzip':
            ib = StringIO(data)
            zf = GzipFile(fileobj=ib)
            return zf.read()
        else:
            return data
            
    def GET(self, c):
        return self._dispatch('do_'+c)
    def POST(self, c):
        return self._dispatch('post_'+c)
        
