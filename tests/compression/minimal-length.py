#/usr/bin/env python3

import sys
import os

dirname = "dedupsqlfs"

# Figure out the directy which is the prefix
# path-of-current-file/..
curpath = os.path.abspath( sys.argv[0] )
if os.path.islink(curpath):
    curpath = os.readlink(curpath)
currentdir = os.path.dirname( curpath )
basedir = os.path.abspath( os.path.join( currentdir, "..", ".." ) )

dynloaddir = os.path.abspath( os.path.join( basedir, "lib-dynload" ) )

sys.path.insert( 0, dynloaddir )
sys.path.insert( 0, basedir )
os.chdir(basedir)

COMPRESSION_SUPPORTED=('lzo', 'zlib', 'bz2', 'xz', 'snappy', 'lz4', 'zstd', 'brotli',)

CLENGTHS={}

for l in range(1, 256, 1):
    print("Length: %d" % l)

    done = True

    for c in COMPRESSION_SUPPORTED:

        if type(c) is tuple:
            _c = c
            c = _c[1]
            m = __import__(_c[0])
            method = getattr(m, _c[2])
        else:
            m = __import__(c)
            method = getattr(m, "compress")

        if not c in CLENGTHS:
            CLENGTHS[ c ] = {
                "done" : False
            }

        if CLENGTHS[ c ]["done"]:
            continue
        else:
            done = False

        CLENGTHS[ c ]["length"] = l

        s = b'a' * l
        cs = method(s)
        if len(s) > len(cs):
            CLENGTHS[ c ]["done"] = True


    if done:
        break

for c in CLENGTHS:
    print("method: %r, min length: %r" % (c, CLENGTHS[c]["length"]))
