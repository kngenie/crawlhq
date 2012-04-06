class seen_shpq(object):
    def longkeyhash(self, s):
        return ("#%x" % _fp64.fp(s))

    def urlkey(self, url):
        scheme, netloc, path, query, fragment = urlsplit(url)
        k = dict(s=scheme, h=netloc)
        if len(path) < 800:
            k.update(p=path)
        else:
            k.update(P=path, p=self.longkeyhash(path))
        if len(query) < 800:
            k.update(q=query)
        else:
            k.update(Q=query, q=self.longkeyhash(query))
        return k

    def keyurl(self, u):
        return urlunsplit(u['s'], u['h'],
                          u['P'] if 'op' in u else u['p'],
                          u['Q'] if 'oq' in u else u['q'])

    def uriquery(self, uri):
        return {'u.s': uri['s'],
                'u.h': uri['h'],
                'u.p': uri['p'],
                'u.q': uri['q']}
    
