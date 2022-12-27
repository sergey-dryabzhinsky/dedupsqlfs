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
    extra_compile_args = ["-std=c99", "-Wall", "-D_FORTIFY_SOURCE=2", "-fstack-protector"]
    if EXTRA_OPT:
        extra_compile_args.insert(0, "-march=native")
        extra_compile_args.insert(0, "-O3")
    else:
        extra_compile_args.insert(0, "-O2")

setup(
    name = "lzo",
    version = "1.14",
    packages=[],
    package_dir={'': 'src'},
    ext_modules = [
        Extension(
            "_lzo",
            ["src/lzomodule.c"],
            libraries=['lzo2'],
            extra_compile_args=extra_compile_args
        )
    ]
)
