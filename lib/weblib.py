import os
import json

import web

def lref(name):
    """returns local path for script-relative resource 'name'
    """
    # note: SCRIPT_FILENAME is only available in mod_wsgi
    if 'SCRIPT_FILENAME' not in web.ctx.environ:
        return os.path.join(sys.path[0], name)
    path = web.ctx.environ['SCRIPT_FILENAME']
    return os.path.join(os.path.dirname(path), name)

class BaseApp(object):
    def __init__(self):
        tglobals = dict(format=format)
        self.templates = web.template.render(lref('t'), globals=tglobals)

    def render(self, tmpl, *args, **kw):
        # note self.templates[tmpl] does not work.
        t = getattr(self.templates, tmpl)
        return t(*args)
    
class QueryApp(object):
    def _dispatch(self, method, *args):
        f = getattr(self, method, None)
        if not f: raise web.notfound('bad action %s' % c)
        r = f(*args)
        if isinstance(r, dict):
            r = json.dumps(r, check_circular=False, separators=',:') + '\n'
            web.header('content-type', 'text/json')
        return r
        
    def GET(self, c):
        return self._dispatch('do_'+c)
    def POST(self, c):
        return self._dispatch('post_'+c)
        
