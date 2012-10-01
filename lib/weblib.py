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
        path = __file__
        logging.warn('SCRIPT_FILENAME not in environ,'
                     ' using __file__=%s instead', path)
    else:
        path = web.ctx.environ['SCRIPT_FILENAME']
    return os.path.join(os.path.dirname(path), name)

class BaseApp(object):
    def __init__(self, tmpldir=None):
        tglobals = dict(format=format)
        try:
            if not tmpldir:
                tmpldir = (web.ctx.environ.get('TMPLDIR') or
                           getattr(self, 'TMPLDIR', None) or
                           lref('t'))
            self.templates = web.template.render(tmpldir, globals=tglobals)
        except:
            logging.critical('BaseApp.__init__ failed', exc_info=1)

    def render(self, tmpl, *args, **kw):
        # note self.templates[tmpl] does not work.
        t = getattr(self.templates, tmpl)
        return t(*args)
    
class QueryApp(object):
    def _dispatch(self, p, c, *args):
        f = getattr(self, p + c, None)
        if not f: raise web.notfound('bad action %s (no method %s in %s)' % (
                c, p + c, self))
        r = f(*args)
        if isinstance(r, (dict, list)):
            try:
                r = json.dumps(r, check_circular=False, separators=',:') + '\n'
            except Exception, ex:
                logging.error('json.dumps failed on "%r"', r)
                r = json.dumps(dict(success=0, err=str(ex),
                                    value=str(r)))
            web.header('content-type', 'text/json')
        return r

    def decode_content(self, data):
        """decod if content is gzipped"""
        if web.ctx.env.get('HTTP_CONTENT_ENCODING') == 'gzip':
            ib = StringIO(data)
            zf = GzipFile(fileobj=ib)
            return zf.read()
        else:
            return data
            
    def GET(self, c, *args):
        return self._dispatch('do_', c, *args)
    def POST(self, c, *args):
        return self._dispatch('post_', c, *args)
    
