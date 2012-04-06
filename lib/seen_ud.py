class seen_ud(object):
    # split long URL, use fp for the tail (0.02-0.04s/80URIs)
    def longkeyhash32(self, s):
        return ("#%x" % (_fp32.fp(s) >> 32))
    def hosthash(self, h):
        # mongodb can only handle upto 64bit signed int
        #return (_fp63.fp(h) >> 1)
        return int(_fp31.fp(h) >> 33)

    def urlkey(self, url):
        k = {}
        # 790 < 800 - (32bit/4bit + 1)
        if len(url) > 790:
            u1, u2 = url[:790], url[790:]
            k.update(u1=u1, u2=u2, h=self.longkeyhash32(u2))
        else:
            k.update(u1=url, h='')
        return k
    def keyurl(self, k):
        return k['u1']+k['u2'] if 'u2' in k else k['u1']
    def keyfp(self, k):
        url = k['u1']
        p1 = url.find('://')
        if p1 > 0:
            p2 = url.find('/', p1+3)
            host = url[p1+3:p2] if p2 >= 0 else url[p1+3:]
        else:
            host = ''
        return self.hosthash(host)
    def keyhost(self, k):
        return k['H']
    # name is incorrect
    def keyquery(self, k):
        # in sharded environment, it is important to have shard key in
        # a query. also it is necessary for non-multi update to work.
        return {'fp':self.keyfp(k), 'u.u1':k['u1'], 'u.h':k['h']}
    # old and incorrect name
    uriquery = keyquery

