#/usr/bin/env python3
# -*- coding: utf8 -*-

import sys
import os
import time
import random
import struct
import io

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

nROUNDS = 5
cROUNDS = range(nROUNDS)
dataRange = 1024*128*4
dataMin = ord('A')
dataMax = ord('Z')
processData = b''

compressedData = {}

def generate_data():
    global processData
    rnd = random.SystemRandom()
    b = io.BytesIO()
    for n in range(dataRange):
        if n % 1024 == 0:
            sys.stdout.write(".")
            sys.stdout.flush()

        b.write(struct.pack('B', rnd.randint(dataMin, dataMax)))
    print("")
    processData = b.getvalue()
    return

def generate_file_data(file_path):
    global processData
    f = open(file_path, 'rb')
    processData = f.read()
    f.close()
    return

if len(sys.argv) > 1:
    print("Get file data")
    generate_file_data(sys.argv[1])
else:
    print("Generate random data")
    generate_data()
print("Done")

def do_simple_ctest(method, name):
    global compressedData

    dt = 0.0
    lcdata = 0.0
    ldata = len(processData) * nROUNDS

    for n in range(nROUNDS):
        sys.stdout.write(".")
        sys.stdout.flush()

        t1 = time.time()
        if name == 'lz4h':
            cdata = method.compressHC(processData)
        else:
            cdata = method.compress(processData)
        t2 = time.time()
        dt += t2 - t1

        if not compressedData.get(name):
            compressedData[ name ] = cdata

        lcdata += len(cdata)

    print("")
    return dt / nROUNDS, ldata / nROUNDS, 100.0 * lcdata / ldata

def do_level_ctest(method, name, level):
    global compressedData

    key = name + "_" + str(level)
    dt = 0.0
    lcdata = 0.0
    ldata = len(processData) * nROUNDS

    for n in range(nROUNDS):
        sys.stdout.write(".")
        sys.stdout.flush()

        t1 = time.time()
        cdata = method.compress(processData, level)
        t2 = time.time()
        dt += t2 - t1

        if not compressedData.get(key):
            compressedData[ key ] = cdata

        lcdata += len(cdata)

    print("")
    return dt / nROUNDS, ldata / nROUNDS, 100.0 * lcdata / ldata

def do_level_ctest_lzma(method, name, level):
    global compressedData

    key = name + "_" + str(level)
    dt = 0.0
    lcdata = 0.0
    ldata = len(processData) * nROUNDS

    for n in range(nROUNDS):
        sys.stdout.write(".")
        sys.stdout.flush()

        t1 = time.time()
        cdata = method.compress(processData, preset=level)
        t2 = time.time()
        dt += t2 - t1

        if not compressedData.get(key):
            compressedData[ key ] = cdata

        lcdata += len(cdata)

    print("")
    return dt / nROUNDS, ldata / nROUNDS, 100.0 * lcdata / ldata


def do_simple_dtest(method, name):
    global compressedData

    dt = 0.0
    ldata = 0.0

    cdata = compressedData[ name ]

    for n in range(nROUNDS):
        sys.stdout.write(".")
        sys.stdout.flush()

        t1 = time.time()
        data = method.decompress(cdata)
        t2 = time.time()
        dt += t2 - t1

        ldata += len(data)

    print("")
    return dt / nROUNDS, ldata / nROUNDS

def do_level_dtest(method, name, level):
    global compressedData

    dt = 0.0
    ldata = 0.0

    key = name + "_" + str(level)
    cdata = compressedData[ key ]

    for n in range(nROUNDS):
        sys.stdout.write(".")
        sys.stdout.flush()

        t1 = time.time()
        data = method.decompress(cdata)
        t2 = time.time()
        dt += t2 - t1

        ldata += len(data)

    print("")
    return dt / nROUNDS, ldata / nROUNDS


COMPRESSION_SUPPORTED={
    'lzo' : (False, do_simple_ctest,),
    'lz4' : (False, do_simple_ctest,),
    'lz4h' : (False, do_simple_ctest,),
    'snappy' : (False, do_simple_ctest,),
    'quicklz' : (False, do_simple_ctest,),
    'zlib' : (range(0,10), do_level_ctest,),
    'bz2' : (range(1,10), do_level_ctest,),
    'lzma' : (range(0,10), do_level_ctest_lzma,),
    }

DECOMPRESSION_SUPPORTED={
    'lzo' : (False, do_simple_dtest,),
    'lz4' : (False, do_simple_dtest,),
    'lz4h' : (False, do_simple_dtest,),
    'snappy' : (False, do_simple_dtest,),
    'quicklz' : (False, do_simple_dtest,),
    'zlib' : (range(0,10), do_level_dtest,),
    'bz2' : (range(1,10), do_level_dtest,),
    'lzma' : (range(0,10), do_level_dtest,),
    }

CTIMING={}

DTIMING={}

if len(sys.argv) > 1:
    print("Do %s compressions of file data to collect mean time and speed..." % nROUNDS)
else:
    print("Do %s compressions of random string to collect mean time and speed..." % nROUNDS)

print("\n")

for c, data in COMPRESSION_SUPPORTED.items():

    print("Test %r" % c)

    if c == 'lz4h':
        m = __import__('lz4')
    else:
        m = __import__(c)

    levels = data[0]
    test_func = data[1]

    if not levels:
        dt, ldata, ratio_proc = test_func(m, c)
        CTIMING[ c ] = (dt, ratio_proc, ldata / dt / 1024.0 / 1024.0,)
    else:
        CTIMING[ c ] = {}
        for level in levels:
            print("-- level: %r" % level)
            dt, ldata, ratio_proc = test_func(m, c, level)
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
                lvl, dt, (dt-min_v)*100.0/max_v, ratio_proc, speed_mbps))

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


print("\n")
if len(sys.argv) > 1:
    print("Do %s decompressions of file data to collect mean time and speed..." % nROUNDS)
else:
    print("Do %s decompressions of random string to collect mean time and speed..." % nROUNDS)

print("\n")

for c, data in DECOMPRESSION_SUPPORTED.items():

    print("Test %r" % c)

    if c == 'lz4h':
        m = __import__('lz4')
    else:
        m = __import__(c)

    levels = data[0]
    test_func = data[1]

    if not levels:
        dt, ldata = test_func(m, c)
        DTIMING[ c ] = (dt, ldata / dt / 1024.0 / 1024.0,)
    else:
        DTIMING[ c ] = {}
        for level in levels:
            print("-- level: %r" % level)
            dt, ldata = test_func(m, c, level)
            DTIMING[c][level] = (dt, ldata / dt / 1024.0 / 1024.0,)

print("\nResults:\n")

for c, results in DTIMING.items():

    if type(results) is tuple:

        dt = results[0]
        speed_mbps = results[1]

        print("Decompression %r: %.6f sec, %.2f Mb/s" % (c, dt, speed_mbps,))
    else:
        data = results.values()

        min_v = 10**10
        max_v = 0
        for lvl, _results in results.items():
            dt = _results[0]
            speed_mbps = _results[1]
            if dt > max_v:
                max_v = dt
            if dt < min_v:
                min_v = dt

        dv = max_v - min_v
        print("Decompression %r:" % c)

        for lvl, _results in results.items():
            dt = _results[0]
            speed_mbps = _results[1]
            print("-- level: %s, time: %.6f sec, about %.2f %% times, %.2f Mb/s" % (
                lvl, dt, (dt-min_v)*100.0/max_v, speed_mbps))

print("\nTable of times:")

cmps = DTIMING.keys()

_cmps = ["level"]
_cmps.extend(cmps)

print("\t".join("%-9s" % c for c in _cmps))

for level in range(0,10):
    row = ["%8s" % level]
    for c in cmps:
        results = DTIMING[c]
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

print("\nTable of speed in Mb/s:")

print("\t".join("%-9s" % c for c in _cmps))

for level in range(0,10):
    row = ["%8s" % level]
    for c in cmps:
        results = DTIMING[c]
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
