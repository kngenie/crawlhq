try:
    from cfpgenerator import FPGenerator
except:
    from fpgenerator import FPGenerator

_fp64 = FPGenerator(0xD74307D3FD3382DB, 64)

def urikey(uri):
    if isinstance(uri, unicode):
        uri = uri.encode('utf-8')
    uhash = _fp64.sfp(uri)
    return uhash
