import sys

from distutils.command.build_ext import build_ext
from distutils.core import setup
from distutils.extension import Extension

__version__ = "0.7.0.1"

if sys.version_info < (2,6):
    sys.stderr.write("ERROR: Python 2.5 and older are not supported, and probably never will be.\n")
    sys.exit(1)

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
    ext_modules=[
        Extension('lz4', [
            'src/lz4.c',
            'src/lz4hc.c',
            'src/python-lz4.c'
        ], extra_compile_args=[
            "-std=c99",
            "-O2",
            "-Wall",
            "-W",
            "-Wundef",
            "-DLZ4_VERSION=\"r131\"",
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
