class seen_du(object):
    def longkeyhash64(self, s):
        return ("#%x" % _fp64.fp(s))

    # always use fp - this is way too slow (>1.6s/80URIs)
    def urlkey(self, url):
        k = dict(h=self.longkeyhash64(url), u=url)
        return k

    def keyurl(self, k):
        return k['u']

    def uriquery(self, k):
        return k
    
