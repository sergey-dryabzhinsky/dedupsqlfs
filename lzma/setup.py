#!/usr/bin/python -u
#
# Python Bindings for XZ/LZMA backported from Python 3.3.0
#
# This file copyright (c) 2012 Peter Cock, p.j.a.cock@googlemail.com
# See other files for separate copyright notices.

import sys, os
from warnings import warn

from distutils import log
from distutils.command.build_ext import build_ext
from distutils.core import setup
from distutils.extension import Extension

__version__ = "0.0.2"

packages = []
home = os.path.expanduser("~")
extens = [Extension('_lzma',
                    ['src/_lzmamodule.c'],
                    libraries = ['lzma'],
                    include_dirs = [os.path.join(home, 'include'), '/opt/local/include', '/usr/local/include'],
                    library_dirs = [os.path.join(home, 'lib'), '/opt/local/lib', '/usr/local/lib'],
                    extra_compile_args = ["-O2", "-DFORTIFY_SOURCE=2", "-fstack-protector"]
                    )
]

descr = "Backport of Python 3.3's 'lzma' module for XZ/LZMA compressed files."
long_descr = """This is a backport of the 'lzma' module included in Python 3.3 or later
by Nadeem Vawda and Per Oyvind Karlsen, which provides a Python wrapper for XZ Utils
(aka LZMA Utils v2) by Igor Pavlov.

In order to compile this, you will need to install XZ Utils from http://tukaani.org/xz/
"""

if sys.version_info < (2,6):
    sys.stderr.write("ERROR: Python 2.5 and older are not supported, and probably never will be.\n")
    sys.exit(1)

setup(
    name = "lzma",
    version = __version__,
    description = descr,
    author = "Peter Cock, based on work by Nadeem Vawda and Per Oyvind Karlsen",
    author_email = "p.j.a.cock@googlemail.com",
    url = "https://github.com/peterjc/backports.lzma",
    license='3-clause BSD License',
    keywords = "xz lzma compression decompression",
    long_description = long_descr,
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        #'Operating System :: OS Independent',
        'Topic :: System :: Archiving :: Compression',
    ],
    packages = packages,
    package_dir={'': 'src'},
    ext_modules = extens,
    cmdclass = {
        'build_ext': build_ext,
    },
)
