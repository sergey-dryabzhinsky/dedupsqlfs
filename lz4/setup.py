#!/usr/bin/env python

"""Setup file for veezio backend"""

from setuptools import setup, find_packages, Extension

VERSION = (0, 6, 0)

setup(
    name='lz4',
    version=".".join([str(x) for x in VERSION]),
    description="LZ4 Bindings for Python",
    long_description=open('README.rst', 'r').read(),
    author='Steeve Morin',
    author_email='steeve.morin@gmail.com',
    url='https://github.com/steeve/python-lz4',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    ext_modules=[
        Extension('lz4', [
            'src/lz4.c',
            'src/lz4hc.c',
            'src/python-lz4.c'
        ], extra_compile_args=["-O2", "-DFORTIFY_SOURCE=2", "-fstack-protector"])
#        ], extra_compile_args=["-O2", "-march=native"])
#        ], extra_compile_args=["-O2", "-march=native", "-floop-interchange", "-floop-block", "-floop-strip-mine", "-ftree-loop-distribution"])
    ],
)
