import sys

from distutils.command.build_ext import build_ext
from distutils.core import setup
from distutils.extension import Extension
from distutils import ccompiler

__version__ = "0.7.0.1"
LZ4_VERSION = "r131"

if sys.version_info < (2,6):
    sys.stderr.write("ERROR: Python 2.5 and older are not supported, and probably never will be.\n")
    sys.exit(1)

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
    extra_compile_args = ["-std=c99", "-Wall"]
    if EXTRA_OPT:
        extra_compile_args.insert(0, "-march=native")
        extra_compile_args.insert(0, "-O3")
    else:
        extra_compile_args.insert(0, "-O2")

if ccompiler.get_default_compiler() == "msvc":
    define_macros = [("LZ4_VERSION","\\\"%s\\\"" % LZ4_VERSION)]
else:
    extra_compile_args.extend(["-W", "-Wundef", "-DFORTIFY_SOURCE=2", "-fstack-protector",])
    define_macros = [("LZ4_VERSION","\"%s\"" % LZ4_VERSION)]

lz4mod = Extension(
    'lz4',
    [
        'src/lz4.c',
        'src/lz4hc.c',
        'src/python-lz4.c'
    ],
    extra_compile_args=extra_compile_args,
    define_macros=define_macros,
)

setup(
    name='lz4',
    version=__version__,
    description="LZ4 Bindings for Python",
    long_description=open('README.rst', 'r').read(),
    author='Steeve Morin',
    author_email='steeve.morin@gmail.com',
    url='https://github.com/steeve/python-lz4',
    packages=[],
    package_dir={'': 'src'},
    ext_modules=[lz4mod,],
    cmdclass = {
        'build_ext': build_ext,
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Programming Language :: C',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],
)
