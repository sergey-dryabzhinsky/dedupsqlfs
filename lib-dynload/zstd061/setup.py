#!/usr/bin/env python

import sys
from setuptools import setup, find_packages, Extension
from setuptools.command.build_ext import build_ext
from distutils import ccompiler

VERSION = (0, 6, 1)
VERSION_STR = ".".join([str(x) for x in VERSION])

SUP_LEGACY = 0
if "--legacy" in sys.argv:
    # Support legacy output format functions
    SUP_LEGACY = 1
    sys.argv.remove("--legacy")

EXTRA_OPT = 0
if "--extra-optimization" in sys.argv:
    # Support legacy output format functions
    EXTRA_OPT = 1
    sys.argv.remove("--extra-optimization")

if ccompiler.get_default_compiler() == "msvc":
    extra_compile_args = [
        "/Wall",
        '/Izstd\\lib\\common', '/Izstd\\lib\\compress',
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
        '-Izstd/lib/common', '-Izstd/lib/compress',
        '-DVERSION="%s"' % VERSION_STR,
        '-DZSTD_LEGACY_SUPPORT=%d' % SUP_LEGACY
    ]
    if EXTRA_OPT:
        extra_compile_args.insert(0, "-march=native")
        extra_compile_args.insert(0, "-O3")
    else:
        extra_compile_args.insert(0, "-O2")


zstdFiles = []
for f in [
        'compress/zstd_compress.c', 'compress/fse_compress.c', 'compress/huf_compress.c', 'compress/zbuff_compress.c',
        'decompress/zstd_decompress.c', 'decompress/huf_decompress.c', 'decompress/zbuff_decompress.c',
        'common/fse_decompress.c', 'common/entropy_common.c', 'common/zstd_common.c',
    ]:
    zstdFiles.append('zstd/lib/'+f)

if SUP_LEGACY:
    if ccompiler.get_default_compiler() == "msvc":
        extra_compile_args.extend(['/Izstd\\lib\\legacy', ])
    else:
        extra_compile_args.extend(['-Izstd/lib/legacy', ])
    for f in [
            'legacy/zstd_v01.c', 'legacy/zstd_v02.c', 'legacy/zstd_v03.c', 'legacy/zstd_v04.c', 'legacy/zstd_v05.c',
        ]:
        zstdFiles.append('zstd/lib/'+f)


zstdFiles.append('src/python-zstd.c')


class BuildExtSubclass(build_ext):

    def build_extensions(self):
        for e in self.extensions:
            e.extra_compile_args = extra_compile_args
        build_ext.build_extensions(self)

setup(
    name='zstd061',
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
        Extension('zstd061', zstdFiles)
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
        'Programming Language :: Python :: 3.5',
    ],
)
