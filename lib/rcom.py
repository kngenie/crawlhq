"""utility functions for query/configure application components at runtime.
currently this module has very limited functionality.
"""

def parse_bool(s):
    if isinstance(s, bool):
        return s
    if isinstance(s, basestring):
        s = s.lower()
        if s == 'true': return True
        if s == 'false': return False
    try:
        i = int(s)
        return bool(i)
    except ValueError:
        pass
    return bool(s)

class Model(object):
    def __init__(self, topobj):
        self.topobj = topobj
    def _find_property(self, name):
        nc = name.split('.')
        if len(nc) != 2:
            raise ValueError, 'bad parameter name'
        on, pn = nc
        o = self.topobj.get(on)
        if o is None:
            raise ValueError, 'no such object'
        params = getattr(o, 'PARAMS', [])
        pe = None
        for d in params:
            if d[0] == pn:
                pe = d
                break
        if pe is None:
            raise ValueError, 'no such parameter'
        return (o, pe)

        return getattr(o, pe[0], None)
        
    def get_property(self, name):
        o, pe = self._find_property(name)
        return getattr(o, pe[0], None)

    def set_property(self, name, value):
        o, pe = self._find_property(name)
        ov = getattr(o, pe[0], None)
        conv = pe[1]
        if conv == bool: conv = parse_bool
        try:
            nv = conv(value)
            setattr(o, pe[0], nv)
            return (nv, ov)
        except ValueError:
            raise
        except AttributeError as ex:
            raise ValueError, ex
            #raise ValueError, 'no such attribute %s in %s' % (pe[0], o)
