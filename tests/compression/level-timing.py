#/usr/bin/env python3
# -*- coding: utf8 -*-

import sys
import os
import time
import random
import pprint

dirname = "dedupsqlfs"

# Figure out the directy which is the prefix
# path-of-current-file/..
curpath = os.path.abspath( sys.argv[0] )
if os.path.islink(curpath):
    curpath = os.readlink(curpath)
currentdir = os.path.dirname( curpath )
basedir = os.path.abspath( os.path.join( currentdir, "..", ".." ) )

sys.path.insert( 0, basedir )
os.chdir(basedir)

nROUNDS = 64
cROUNDS = range(nROUNDS)
changeData = 4
dataRange = 1024*128
dataMin = 0
dataMax = 15

def do_simple_ctest(method):
    dt = 0
    lcdata = 0
    data = b''
    for cr in cROUNDS:

        if cr % changeData == 0:
            data = b''
            for l in range(1, dataRange):
                data += chr( random.randint(dataMin, dataMax)).encode()

        t1 = time.time()
        cdata = method.compress(data)
        t2 = time.time()
        dt += t2 - t1

        lcdata += len(cdata)

    return dt / nROUNDS, lcdata / nROUNDS

def do_level_ctest(method, level):
    dt = 0
    lcdata = 0
    data = b''
    for cr in cROUNDS:

        if cr % changeData == 0:
            data = b''
            for l in range(1, dataRange):
                data += chr( random.randint(dataMin, dataMax)).encode()

        t1 = time.time()
        cdata = method.compress(data, level)
        t2 = time.time()
        dt += t2 - t1

        lcdata += len(cdata)

    return dt / nROUNDS, lcdata / nROUNDS

def do_level_ctest_lzma(method, level):
    dt = 0
    lcdata = 0
    data = b''
    for cr in cROUNDS:

        if cr % changeData == 0:
            data = b''
            for l in range(1, dataRange):
                data += chr( random.randint(dataMin, dataMax)).encode()

        t1 = time.time()
        cdata = method.compress(data, preset=level)
        t2 = time.time()
        dt += t2 - t1

        lcdata += len(cdata)

    return dt / nROUNDS, lcdata / nROUNDS

COMPRESSION_SUPPORTED={
    'lzo' : (False, do_simple_ctest,),
    'lz4' : (False, do_simple_ctest,),
    'snappy' : (False, do_simple_ctest,),
    'zlib' : (range(0,10), do_level_ctest,),
    'bz2' : (range(1,10), do_level_ctest,),
    'lzma' : (range(0,10), do_level_ctest_lzma,),
    }

CTIMING={}

print("Do %s compressions of random string collect mean time..." % nROUNDS)

print("\n")

for c, data in COMPRESSION_SUPPORTED.items():

    print("Test %r" % c)

    m = __import__(c)

    levels = data[0]
    test_func = data[1]

    if not levels:
        dt, lcdata = test_func(m)
        CTIMING[ c ] = (dt, lcdata,)
    else:
        CTIMING[ c ] = {}
        for level in levels:
            print("-- level: %r" % level)
            dt, lcdata = test_func(m, level)
            CTIMING[c][level] = (dt, lcdata,)

#print("\nRaw Results:\n")
#pprint.pprint(CTIMING)

min_values = {}
max_values = {}

ldata = 1024*128

print("\nResults:\n")

for c, results in CTIMING.items():

    if type(results) is tuple:

        dt = results[0]
        lcdata = results[1]

        eff = lcdata * 100.0 / ldata

        print("Compression %r: %.6f sec, %.2f %% ratio" % (c, dt, eff))
        min_values[c] = "%.6f" % dt
        max_values[c] = "%.6f" % dt
    else:
        data = results.values()

        min_v = 10**10
        max_v = 0
        for lvl, _results in results.items():
            dt = _results[0]
            lcdata = _results[1]
            if dt > max_v:
                max_v = dt
            if dt < min_v:
                min_v = dt

        min_values[c] = "%.6f" % min_v
        max_values[c] = "%.6f" % max_v

        dv = max_v - min_v
        print("Compression %r:" % c)

        for lvl, _results in results.items():
            dt = _results[0]
            lcdata = _results[1]
            eff = lcdata * 100.0 / ldata
            print("-- level: %s, time: %.6f sec, about %.2f %% times, %.2f %% ratio" % (lvl, dt, (dt-min_v)*100.0/dv, eff))

#print("\nMinimal values:")
#pprint.pprint(min_values)

#print("\nMaximum values:")
#pprint.pprint(max_values)

print("\nTable of times:")

cmps = CTIMING.keys()

_cmps = ["level"]
_cmps.extend(cmps)

print("\t".join("%-9s" % c for c in _cmps))

for level in range(0,10):
    row = ["%8s" % level]
    for c in cmps:
        results = CTIMING[c]
        if type(results) is tuple:
            row.append("%.6f" % results[0])
        else:
            found = False
            for lvl, r in results.items():
                if lvl == level:
                    row.append("%.6f" % r[0])
                    found = True
            if not found:
                row.append(" "*8)

    print("\t".join(row))

print("\nTable of ratio in %:")

print("\t".join("%-9s" % c for c in _cmps))

for level in range(0,10):
    row = ["%8s" % level]
    for c in cmps:
        results = CTIMING[c]
        if type(results) is tuple:
            lcdata = results[1]
            eff = "%.2f" % (lcdata * 100.0 / ldata)
            row.append("%8s" % eff)
        else:
            found = False
            for lvl, r in results.items():
                if lvl == level:
                    lcdata = r[1]
                    eff = "%.2f" % (lcdata * 100.0 / ldata)
                    row.append("%8s" % eff)
                    found = True
            if not found:
                row.append(" "*8)

    print("\t".join(row))
