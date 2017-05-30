import sys
from distutils.core import setup, Extension
from distutils import ccompiler

EXTRA_OPT=0
if "--extra-optimization" in sys.argv:
    # Support legacy output format functions
    EXTRA_OPT=1
    sys.argv.remove("--extra-optimization")

if ccompiler.get_default_compiler() == "msvc":
    extra_compile_args = ["/Wall"]
    if EXTRA_OPT:
        extra_compile_args.insert(0, "/O2")
    else:
        extra_compile_args.insert(0, "/Ot")
else:
    extra_compile_args = ["-std=c99", "-Wall", "-DFORTIFY_SOURCE=2", "-fstack-protector"]
    if EXTRA_OPT:
        extra_compile_args.insert(0, "-march=native")
        extra_compile_args.insert(0, "-O3")
    else:
        extra_compile_args.insert(0, "-O2")

define_macros = [("QLZ_COMPRESSION_LEVEL","2"),("QLZ_STREAMING_BUFFER","%s" % (256*1024,)),]

setup(
    name = "qlzm",
    version = "1.0",
    packages=[],
    package_dir={'': 'src'},
    ext_modules = [
        Extension(
            "qlzm",
            ["src/quicklz.c", "src/quicklzpy.c"],
            extra_compile_args=extra_compile_args,
            define_macros=define_macros
        )
    ]
)
