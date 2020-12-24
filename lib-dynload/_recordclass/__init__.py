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

dirs = os.listdir(build_dir)
for d in dirs:
    if d.find("-%s.%s" % (p1, p2)) != -1 and d.find("lib.") != -1:
        sys.path.insert(0, os.path.join(build_dir, d) )

        import importlib
        module = importlib.import_module("recordclass")

        loaded = True

        sys.path.pop(0)

        del p1, p2
        del currentdir, build_dir, dirs

        break
