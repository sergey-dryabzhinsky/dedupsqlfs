#!/usr/bin/env python

import sys
from setuptools import setup, find_packages, Extension
from setuptools.command.build_ext import build_ext
from distutils import ccompiler

VERSION = (0, 3, 6)
VERSION_STR = ".".join([str(x) for x in VERSION])

EXTRA_OPT = 0
if "--extra-optimization" in sys.argv:
    # Support legacy output format functions
    EXTRA_OPT = 1
    sys.argv.remove("--extra-optimization")

if ccompiler.get_default_compiler() == "msvc":
    extra_compile_args = [
        "/Wall",
        '/Izstd\\lib', '/Izstd\\lib\\legacy',
        '/DVERSION=\"\\\"%s\\\"\"' % VERSION_STR
    ]
    if EXTRA_OPT:
        extra_compile_args.insert(0, "/O2")
    else:
        extra_compile_args.insert(0, "/Ot")
else:
    extra_compile_args = [
        "-std=c99", "-Wall", "-DFORTIFY_SOURCE=2", "-fstack-protector",
        '-Izstd/lib', '-Izstd/lib/legacy',
        '-DVERSION="%s"' % VERSION_STR
    ]
    if EXTRA_OPT:
        extra_compile_args.insert(0, "-march=native")
        extra_compile_args.insert(0, "-O3")
    else:
        extra_compile_args.insert(0, "-O2")


class BuildExtSubclass(build_ext):

    def build_extensions(self):
        for e in self.extensions:
            e.extra_compile_args = extra_compile_args
        build_ext.build_extensions(self)

setup(
    name='zstd',
    version=VERSION_STR,
    description="ZSTD Bindings for Python",
    long_description=open('README.rst', 'r').read(),
    author='Sergey Dryabzhinsky, Anton Stuk',
    author_email='sergey.dryabzhinsky@gmail.com',
    maintainer='Sergey Dryabzhinsky',
    maintainer_email='sergey.dryabzhinsky@gmail.com',
    url='https://github.com/sergey-dryabzhinsky/python-zstd',
    keywords='zstd, zstandard, compression',
    license='BSD',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    ext_modules=[
        Extension('zstd', [
            'zstd/lib/huff0.c',
            'zstd/lib/fse.c',
            'zstd/lib/legacy/zstd_v01.c',
            'zstd/lib/legacy/zstd_v02.c',
            'zstd/lib/zstd.c',
            'zstd/lib/zstdhc.c',
            'src/python-zstd.c'
        ])
    ],
    cmdclass={'build_ext': BuildExtSubclass},
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Development Status :: 4 - Beta',
        'Operating System :: POSIX',
        'Programming Language :: C',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],
)
