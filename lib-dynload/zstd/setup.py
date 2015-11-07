import sys

from distutils.command.build_ext import build_ext
from distutils.core import setup
from distutils.extension import Extension

__version__ = "0.0.1"

if sys.version_info < (2,6):
    sys.stderr.write("ERROR: Python 2.5 and older are not supported, and probably never will be.\n")
    sys.exit(1)

setup(
    name='zstd',
    version=__version__,
    description="ZSTD Bindings for Python",
    author='Sergey Dryabzhinsky',
    author_email='sergey.dryabzhinsky@gmail.com',
    url='https://github.com/sergey-dryabzhinsky/python-zstd',
    packages=[],
    package_dir={'': 'src'},
    ext_modules=[
        Extension('zstd', [
            'src/zstd.c',
            'src/python-zstd.c'
        ], extra_compile_args=[
            "-std=c99",
            "-O3",
            "-Wall",
            "-W",
            "-Wundef",
# Hardening
            "-DFORTIFY_SOURCE=2", "-fstack-protector",
# Full CPU optimization, for custom build by hand
#            "-march=native",
# GCC Graphite
#            "-floop-interchange", "-floop-block", "-floop-strip-mine", "-ftree-loop-distribution",
       ])
    ],
    cmdclass = {
        'build_ext': build_ext,
    },
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Programming Language :: C',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],
)
