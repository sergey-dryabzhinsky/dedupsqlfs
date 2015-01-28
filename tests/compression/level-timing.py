#/usr/bin/env python3
# -*- coding: utf8 -*-

import sys
import os
import time
import random

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

nROUNDS = 32
cROUNDS = range(nROUNDS)
dataRange = 1024*16
dataMin = ord(' ')
dataMax = ord('z')

def generate_data():
    data_rounds = ()
    rnd = random.SystemRandom()
    for cr in cROUNDS:
        sys.stdout.write(".")
        sys.stdout.flush()
        data = b''
        for l in range(1, dataRange):
            data += chr( rnd.randint(dataMin, dataMax)).encode()
        data_rounds += (data * 128,)
    print("\n")
    return data_rounds

print("Generate random data rounds (%s):" % nROUNDS)
generated_data = generate_data()
print("Done")

def do_simple_ctest(method):
    dt = 0
    lcdata = 0
    ldata = 0
    for data in generated_data:

        t1 = time.time()
        cdata = method.compress(data)
        t2 = time.time()
        dt += t2 - t1

        lcdata += len(cdata)
        ldata += len(data)

    return dt / nROUNDS, ldata / nROUNDS, 100.0 * lcdata / ldata

def do_level_ctest(method, level):
    dt = 0
    lcdata = 0
    ldata = 0
    for data in generated_data:

        t1 = time.time()
        cdata = method.compress(data, level)
        t2 = time.time()
        dt += t2 - t1

        lcdata += len(cdata)
        ldata += len(data)

    return dt / nROUNDS, ldata / nROUNDS, 100.0 * lcdata / ldata

def do_level_ctest_lzma(method, level):
    dt = 0
    lcdata = 0
    ldata = 0
    for data in generated_data:

        t1 = time.time()
        cdata = method.compress(data, preset=level)
        t2 = time.time()
        dt += t2 - t1

        lcdata += len(cdata)
        ldata += len(data)

    return dt / nROUNDS, ldata / nROUNDS, 100.0 * lcdata / ldata

COMPRESSION_SUPPORTED={
    'lzo' : (False, do_simple_ctest,),
    'lz4' : (False, do_simple_ctest,),
    'snappy' : (False, do_simple_ctest,),
    'quicklz' : (False, do_simple_ctest,),
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
        dt, ldata, ratio_proc = test_func(m)
        CTIMING[ c ] = (dt, ratio_proc, ldata / dt / 1024.0 / 1024.0,)
    else:
        CTIMING[ c ] = {}
        for level in levels:
            print("-- level: %r" % level)
            dt, ldata, ratio_proc = test_func(m, level)
            CTIMING[c][level] = (dt, ratio_proc, ldata / dt / 1024.0 / 1024.0,)

print("\nResults:\n")

for c, results in CTIMING.items():

    if type(results) is tuple:

        dt = results[0]
        ratio_proc = results[1]
        speed_mbps = results[2]

        print("Compression %r: %.6f sec, %.2f %% ratio, %.2f Mb/s" % (c, dt, ratio_proc, speed_mbps,))
    else:
        data = results.values()

        min_v = 10**10
        max_v = 0
        for lvl, _results in results.items():
            dt = _results[0]
            ratio_proc = _results[1]
            speed_mbps = _results[2]
            if dt > max_v:
                max_v = dt
            if dt < min_v:
                min_v = dt

        dv = max_v - min_v
        print("Compression %r:" % c)

        for lvl, _results in results.items():
            dt = _results[0]
            ratio_proc = _results[1]
            speed_mbps = _results[2]
            print("-- level: %s, time: %.6f sec, about %.2f %% times, %.2f %% ratio, %.2f Mb/s" % (
                lvl, dt, (dt-min_v)*100.0/dv, ratio_proc, speed_mbps))

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
            eff = "%.2f" % results[1]
            row.append("%8s" % eff)
        else:
            found = False
            for lvl, r in results.items():
                if lvl == level:
                    eff = "%.2f" % r[1]
                    row.append("%8s" % eff)
                    found = True
            if not found:
                row.append(" "*8)

    print("\t".join(row))

print("\nTable of speed in Mb/s:")

print("\t".join("%-9s" % c for c in _cmps))

for level in range(0,10):
    row = ["%8s" % level]
    for c in cmps:
        results = CTIMING[c]
        if type(results) is tuple:
            eff = "%.2f" % results[2]
            row.append("%8s" % eff)
        else:
            found = False
            for lvl, r in results.items():
                if lvl == level:
                    eff = "%.2f" % r[2]
                    row.append("%8s" % eff)
                    found = True
            if not found:
                row.append(" "*8)

    print("\t".join(row))
