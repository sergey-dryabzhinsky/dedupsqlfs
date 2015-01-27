import sys
import os
import imp

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
        build_dir = os.path.abspath( os.path.join(d, "lib-dynload", "lzma", "build") )
        if not os.path.isdir(build_dir):
            build_dir = os.path.abspath( os.path.join(d, "..", "lib-dynload", "lzma", "build") )
        if os.path.isdir(build_dir):
            break

    if not build_dir or not os.path.isdir(build_dir):
        raise OSError("not found module build dir: lzma")

    dirs = os.listdir(build_dir)
    for d in dirs:
        if d.find("-%s.%s" % (p1, p2)) != -1 and d.find("lib.") != -1:
            sys.path.insert(0, os.path.join(build_dir, d) )

#            fp, pathname, description = imp.find_module("lzma")
#            module = imp.load_module("lzma", fp, pathname, description)
            from .module import *

            compress = module.compress
            decompress = module.decompress

            sys.path.pop(0)

            break

if not imported:
    try:
        path = sys.path.pop(0)

        fp, pathname, description = imp.find_module("lzma")
        module = imp.load_module("lzma", fp, pathname, description)

        compress = module.compress
        decompress = module.decompress

        sys.path.insert(0, path)

        imported = True
    except:
        pass
