import sys
import os

p1, p2 = sys.version_info[:2]

curpath = os.path.abspath( sys.argv[0] )
if os.path.islink(curpath):
    curpath = os.readlink(curpath)
currentdir = os.path.dirname( curpath )

build_dir = os.path.abspath( os.path.join(currentdir, "lib-dynload", "__zstd", "build") )
if not os.path.isdir(build_dir):
    build_dir = os.path.abspath( os.path.join(currentdir, "..", "lib-dynload", "__zstd", "build") )
if not os.path.isdir(build_dir):
    build_dir = os.path.abspath( os.path.join(currentdir, "..", "..", "lib-dynload", "__zstd", "build") )

dirs = os.listdir(build_dir)
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

    sys.path.insert(0, os.path.join(build_dir, d) )
    loaded = False
    version = '1.5.7.1'
    import importlib
    try:
        # bundled
        mod = importlib.import_module("__zstd")
        loaded = True
        version = module.version()
    except:
        pass

    import sys
    try:
        # py3.14
        mod = importlib.import_module("_zstd")
        loaded = True
        version = sys.version
    except:
        pass

    try:
        # pypi
        mod = importlib.import_module("zstd")
        loaded = True
        version = module.version()
    except:
        pass

    print(version)
    print(dir(mod))
    compress = mod.compress
    decompress = mod.decompress

    sys.path.pop(0)

    del importlib, mod

    break

del p1, p2, d, found
del curpath, dirs, currentdir, build_dir
