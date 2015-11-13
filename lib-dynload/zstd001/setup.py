#!/usr/bin/env python


from setuptools import setup, find_packages, Extension

VERSION = (0, 0, 1)
VERSION_STR = ".".join([str(x) for x in VERSION])

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
        ], extra_compile_args=[
            "-std=c99",
            "-O3",
            "-DFORTIFY_SOURCE=2", "-fstack-protector",
#            "-march=native",
#            "-floop-interchange", "-floop-block", "-floop-strip-mine", "-ftree-loop-distribution",
        ])
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
