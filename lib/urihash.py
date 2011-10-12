from cfpgenerator import FPGenerator

_fp64 = FPGenerator(0xD74307D3FD3382DB, 64)

def urikey(uri):
    uhash = _fp64.sfp(uri)
    return uhash
