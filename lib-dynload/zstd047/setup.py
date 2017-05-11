#!/usr/bin/env python

import sys
from setuptools import setup, find_packages, Extension
from setuptools.command.build_ext import build_ext
from distutils import ccompiler

VERSION = (0, 4, 7)
VERSION_STR = ".".join([str(x) for x in VERSION])

SUP_LEGACY=0
if "--legacy" in sys.argv:
    # Support legacy output format functions
    SUP_LEGACY=1
    sys.argv.remove("--legacy")

EXTRA_OPT = 0
if "--extra-optimization" in sys.argv:
    # Support legacy output format functions
    EXTRA_OPT = 1
    sys.argv.remove("--extra-optimization")

if ccompiler.get_default_compiler() == "msvc":
    extra_compile_args = [
        "/Wall",
        '/Izstd\\lib',
        '/DVERSION=\"\\\"%s\\\"\"' % VERSION_STR,
        '/DZSTD_LEGACY_SUPPORT=%d' % SUP_LEGACY
    ]
    if EXTRA_OPT:
        extra_compile_args.insert(0, "/O2")
    else:
        extra_compile_args.insert(0, "/Ot")
else:
    extra_compile_args = [
        "-std=c99", "-Wall", "-DFORTIFY_SOURCE=2", "-fstack-protector",
        '-Izstd/lib',
        '-DVERSION="%s"' % VERSION_STR,
        '-DZSTD_LEGACY_SUPPORT=%d' % SUP_LEGACY
    ]
    if EXTRA_OPT:
        extra_compile_args.insert(0, "-march=native")
        extra_compile_args.insert(0, "-O3")
    else:
        extra_compile_args.insert(0, "-O2")


zstdFiles = [
            'zstd/lib/huff0.c',
            'zstd/lib/fse.c',
            'zstd/lib/zstd_compress.c',
            'zstd/lib/zstd_decompress.c',
        ]


if SUP_LEGACY:
    if ccompiler.get_default_compiler() == "msvc":
        extra_compile_args.extend(['/Izstd\\lib\\legacy',])
    else:
        extra_compile_args.extend(['-Izstd/lib/legacy',])

    zstdFiles.extend([
        'zstd/lib/legacy/zstd_v01.c',
        'zstd/lib/legacy/zstd_v02.c',
        'zstd/lib/legacy/zstd_v03.c',
    ])

zstdFiles.append('src/python-zstd.c')


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
        Extension('zstd', zstdFiles)
    ],
    cmdclass={'build_ext': BuildExtSubclass},
    test_suite="tests",
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
