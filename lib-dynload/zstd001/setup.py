#!/usr/bin/env python


import sys
from setuptools import setup, find_packages, Extension
from distutils import ccompiler

VERSION = (0, 0, 1)
VERSION_STR = ".".join([str(x) for x in VERSION])


EXTRA_OPT = 0
if "--extra-optimization" in sys.argv:
    # Support legacy output format functions
    EXTRA_OPT = 1
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

setup(
    name='zstd001',
    version=VERSION_STR,
    description="ZSTD Bindings for Python (alpha)",
    author='Sergey Dryabzhinsky',
    author_email='sergey.dryabzhinsky@gmail.com',
    url='https://github.com/sergey-dryabzhinsky/python-zstd',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    ext_modules=[
        Extension('zstd001', [
            'src/zstd.c',
            'src/python-zstd.c'
        ], extra_compile_args=extra_compile_args)
    ],
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Programming Language :: C',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.2',
    ],
)
