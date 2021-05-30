import sys
import os

search_paths = []

curpath = os.path.abspath( sys.argv[0] )
search_paths.append(curpath)

if os.path.islink(curpath):
    curpath = os.readlink(curpath)
currentdir = os.path.dirname( curpath )
search_paths.append(currentdir)

# Evil hack for import not local module
imported = False

if not imported:
    p1, p2 = sys.version_info[:2]

    build_dir = None
    for d in search_paths:
        build_dir = os.path.abspath( os.path.join(d, "lib-dynload", "xz", "build") )
        if not os.path.isdir(build_dir):
            build_dir = os.path.abspath( os.path.join(d, "..", "lib-dynload", "xz", "build") )
        if not os.path.isdir(build_dir):
            build_dir = os.path.abspath( os.path.join(d, "..", "..", "lib-dynload", "xz", "build") )
        if os.path.isdir(build_dir):
            break

    dirs = []
    if not build_dir or not os.path.isdir(build_dir):
        pass
    else:
        dirs = os.listdir(build_dir)
    for d in dirs:
        if d.find("-%s.%s" % (p1, p2)) != -1 and d.find("lib.") != -1:
            sys.path.insert(0, os.path.join(build_dir, d) )

            from .module import compress, decompress
            imported = True

            sys.path.pop(0)

            break

if not imported:
    path = sys.path.pop(0)
    try:
        import importlib
        module = importlib.import_module("lzma")

        compress = module.compress
        decompress = module.decompress

        imported = True
    except:
        pass

    sys.path.insert(0, path)
    del path, importlib, module

del search_paths, p1, p2, dirs, build_dir, curpath, currentdir
