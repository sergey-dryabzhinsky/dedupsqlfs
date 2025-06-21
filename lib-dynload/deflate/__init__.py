import zlib

def compress(data, level=6):
    from zlib import compressobj
    c = compressobj(level)
    return c.compress(data)

def decompress(cdata):
    from zlib import decompressobj
    d = decompressobj()
    return d.decompress(cdata)