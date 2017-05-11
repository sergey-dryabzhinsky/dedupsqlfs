#!/usr/bin/env python


from setuptools import setup, find_packages, Extension
import subprocess
import sys
import os
from distutils import ccompiler

VERSION = (1, 7, 5,)
VERSION_STR = ".".join([str(x) for x in VERSION])
LZ4_VERSION = "1.7.5"

# Check to see if we have a lz4 library installed on the system and
# use it if so. If not, we'll use the bundled library. If lz4 is
# installed it will have a pkg-config file, so we'll use pkg-config to
# check for existence of the library.
try:
    pkg_config_exe = os.environ.get('PKG_CONFIG', None) or 'pkg-config'
    cmd = '{0} --exists liblz4'.format(pkg_config_exe).split()
    liblz4_found = subprocess.call(cmd) == 0
except OSError: 
    # pkg-config not present
    liblz4_found = False

EXTRA_OPT=0
if "--extra-optimization" in sys.argv:
    # Support legacy output format functions
    EXTRA_OPT=1
    sys.argv.remove("--extra-optimization")

if ccompiler.get_default_compiler() == "msvc":
    extra_compile_args = ["/Wall"]
    if EXTRA_OPT:
        extra_compile_args.insert(0, "/Og")
        extra_compile_args.insert(0, "/Ox")
    else:
        extra_compile_args.insert(0, "/Ot")
else:
    extra_compile_args = ["-std=c99", "-Wall"]
    if EXTRA_OPT:
        extra_compile_args.insert(0, "-march=native")
        extra_compile_args.insert(0, "-O3")
    else:
        extra_compile_args.insert(0, "-O2")

if liblz4_found:
    # Use system lz4, and don't set optimization and warning flags for
    # the compiler. Specifically we don't define LZ4_VERSION since the
    # system lz4 library could be updated (that's the point of a
    # shared library).
    if ccompiler.get_default_compiler() == "msvc":
        define_macros = [("VERSION","\\\"%s\\\"" % VERSION_STR),]
    else:
        define_macros = [("VERSION","\"%s\"" % VERSION_STR),]

    lz4mod = Extension(
        'lz4',
        [
            'src/python-lz4.c'
        ],
        extra_compile_args=extra_compile_args,
        define_macros=define_macros,
        libraries=['lz4'],
    )
else:
    # Use the bundled lz4 libs, and set the compiler flags as they
    # historically have been set. We do set LZ4_VERSION here, since it
    # won't change after compilation.
    if ccompiler.get_default_compiler() == "msvc":
        define_macros = [("VERSION","\\\"%s\\\"" % VERSION_STR), ("LZ4_VERSION","\\\"%s\\\"" % LZ4_VERSION)]
    else:
        extra_compile_args.extend(["-W", "-Wundef", "-DFORTIFY_SOURCE=2", "-fstack-protector",])
        define_macros = [("VERSION","\"%s\"" % VERSION_STR), ("LZ4_VERSION","\"%s\"" % LZ4_VERSION)]

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
    version=VERSION_STR,
    description="LZ4 Bindings for Python",
    long_description=open('README.rst', 'r').read(),
    author='Steeve Morin',
    author_email='steeve.morin@gmail.com',
    url='https://github.com/python-lz4/python-lz4',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    ext_modules=[lz4mod,],
    tests_require=["nose>=1.0"],
    test_suite = "nose.collector",
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
        'Programming Language :: Python :: 3.5',
    ],
)
