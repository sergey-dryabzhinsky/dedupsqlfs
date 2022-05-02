import sys
import os

p1, p2 = sys.version_info[:2]

curpath = os.path.abspath( sys.argv[0] )
if os.path.islink(curpath):
    curpath = os.readlink(curpath)
currentdir = os.path.dirname( curpath )

build_dir = os.path.abspath( os.path.join(currentdir, "lib-dynload", "_recordclass", "build") )
if not os.path.isdir(build_dir):
    build_dir = os.path.abspath( os.path.join(currentdir, "..", "lib-dynload", "_recordclass", "build") )
if not os.path.isdir(build_dir):
    build_dir = os.path.abspath( os.path.join(currentdir, "..", "..", "lib-dynload", "_recordclass", "build") )

module = None
loaded = False
__version__ = "0.0.0"

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

    import importlib
    module = importlib.import_module("recordclass")

    loaded = True

    __version__ = "0.17.2"

    sys.path.pop(0)

    del importlib

    break

del p1, p2, d, found
del curpath, currentdir, build_dir, dirs
