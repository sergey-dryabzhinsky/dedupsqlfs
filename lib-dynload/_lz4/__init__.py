import sys
import os

p1, p2 = sys.version_info[:2]

curpath = os.path.abspath( sys.argv[0] )
if os.path.islink(curpath):
    curpath = os.readlink(curpath)
currentdir = os.path.dirname( curpath )

build_dir = os.path.abspath( os.path.join(currentdir, "lib-dynload", "_lz4", "build") )
if not os.path.isdir(build_dir):
    build_dir = os.path.abspath( os.path.join(currentdir, "..", "lib-dynload", "_lz4", "build") )
if not os.path.isdir(build_dir):
    build_dir = os.path.abspath( os.path.join(currentdir, "..", "..", "lib-dynload", "_lz4", "build") )

dirs = []
if os.path.isdir(build_dir):
  dirs = os.listdir(build_dir)

svp=sys.path[0]
found = 0
for d in dirs:

    found = 0
    if d.find("lib.") == 0:
        found += 1
    if d.find("-%s.%s" % (p1, p2)) != -1:
        found += 1
    # python 3.10+
    if d.find("-cpython-%s%s" % (p1, p2)) != -1:
        found += 1
    # pypy
    if d.find("-pypy%s%s" % (p1, p2)) != -1:
        found += 1
    if found <= 1:
        continue

    svp = sys.path,pop(0)
    sys.path.insert(0, os.path.join(build_dir, d, "_lz4") )
    break

import importlib
if found:
    try:
        module = importlib.import_module("_block")
#        from _lz4.block import _block as module
        print("bundled")
    except Exception as e:
        print(e)
        module = None
        pass

    # try system or pypi
else: # not found
    try:
        print(sys.path)
        from lz4.block import _block as module
        print("system")
    except Exception as e:
        print(e)
        pass

#    print(dir(module))
    compress = module.compress
    decompress = module.decompress


    def compressHC(data):
        return compress(data, mode='high_compression')

    sys.path.pop(0)

    del module, importlib

sys.path.insert(0, svp)

del p1, p2, svp, found
del curpath, build_dir, dirs, currentdir
