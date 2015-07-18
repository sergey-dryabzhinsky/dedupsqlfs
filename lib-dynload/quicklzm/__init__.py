import sys
import os

p1, p2 = sys.version_info[:2]

compress = None
decompress = None

curpath = os.path.abspath( sys.argv[0] )
if os.path.islink(curpath):
    curpath = os.readlink(curpath)
currentdir = os.path.dirname( curpath )

build_dir = os.path.abspath( os.path.join(currentdir, "lib-dynload", "quicklzm", "build") )
if not os.path.isdir(build_dir):
    build_dir = os.path.abspath( os.path.join(currentdir, "..", "lib-dynload", "quicklzm", "build") )
if not os.path.isdir(build_dir):
    build_dir = os.path.abspath( os.path.join(currentdir, "..", "..", "lib-dynload", "quicklzm", "build") )

dirs = os.listdir(build_dir)
for d in dirs:
    if d.find("-%s.%s" % (p1, p2)) != -1 and d.find("lib.") != -1:
        sys.path.insert(0, os.path.join(build_dir, d) )
        import imp
        fp, pathname, description = imp.find_module("quicklz")
        module = imp.load_module("quicklz", fp, pathname, description)

        def compress_data(raw_data):
            """
            This function accepts uncompressed data as an argument,
            and returns the compressed form of that data.
            """
            state = module.QLZStateCompress()
            return module.qlz_compress(raw_data, state)

        def decompress_data(compressed_chunk):
            """
            This function accepts a self-contained (not streamed) chunk of data
            and returns the decompressed contents.
            """
            state = module.QLZStateDecompress()
            return module.qlz_decompress(compressed_chunk, state)

        compress = compress_data
        decompress = decompress_data

        sys.path.pop(0)

        break
