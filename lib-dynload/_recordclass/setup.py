# The MIT License (MIT)
#
# Copyright (c) <2015-2024> <Shibzukhov Zaur, szport at gmail dot com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

from setuptools import setup, find_packages
from setuptools.command.build_ext import build_ext
from setuptools.extension import Extension
import sys, os, io

_PY310 = sys.version_info[:2] >= (3, 10)

# extra_compile_args = ["-O3", "-Wfatal-errors"]
# extra_compile_args = ["-Wfatal-errors"]
extra_compile_args = []
extra_link_args = []

use_cython = 1

if use_cython:
    ext_modules = [
        Extension(
            "recordclass._dataobject",
            ["lib/recordclass/_dataobject.c"],
            # extra_compile_args = extra_compile_args,
            # extra_link_args = extra_link_args,
        ),
        Extension(
            "recordclass._litetuple",
            ["lib/recordclass/_litetuple.c"],
            # extra_compile_args = extra_compile_args,
            # extra_link_args = extra_link_args,
        ),
        Extension(
            "recordclass._litelist",
            ["lib/recordclass/_litelist.c"],
            # extra_compile_args = extra_compile_args,
            # extra_link_args = extra_link_args,
        ),
        Extension(
            "recordclass._linkedlist",
            ["lib/recordclass/_linkedlist.pyx"],
            # extra_compile_args = extra_compile_args,
            # extra_link_args = extra_link_args,
        ),
    ]
else:
    ext_modules = [
        Extension(
            "recordclass._dataobject",
            ["lib/recordclass/_dataobject.c"],
            # extra_compile_args = extra_compile_args,
            # extra_link_args = extra_link_args,
        ),
        Extension(
            "recordclass._litetuple",
            ["lib/recordclass/_litetuple.c"],
            # extra_compile_args = extra_compile_args,
            # extra_link_args = extra_link_args,
        ),
        Extension(
            "recordclass._litelist",
            ["lib/recordclass/_litelist.c"],
            # extra_compile_args = extra_compile_args,
            # extra_link_args = extra_link_args,
        ),
        Extension(
            "recordclass._linkedlist",
            ["lib/recordclass/_linkedlist.c"],
            # extra_compile_args = extra_compile_args,
            # extra_link_args = extra_link_args,
        ),
    ]

packages = [ 'recordclass',
             'recordclass.test',
             'recordclass.test.typing',
             'recordclass.typing',
             'recordclass.tools',
           ]
if _PY310:
    packages.append('recordclass.test.match')

root = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(root, "lib", "recordclass", "about.py"), encoding="utf8") as f:
    about = {}
    exec(f.read(), about)

with open(os.path.join(root, "README.md"), encoding="utf8") as f:
    readme = f.read()

del os, io

setup(
    name = about["__title__"],
    version = about["__version__"],
    description = about["__summary__"],
    author = about["__author__"],
    author_email = about["__email__"],
    url = about["__uri__"],
    license = about["__license__"],
    cmdclass = {'build_ext': build_ext},
    ext_modules = ext_modules,
    package_dir = {'': 'lib'},
    packages = packages,
    python_requires = '>=3.8',
    download_url = 'https://pypi.org/project/recordclass/#files',
    long_description = readme,
    long_description_content_type = "text/markdown",
    platforms = 'Linux, Mac OS X, Windows',
    keywords = ['namedtuple', 'recordclass', 'dataclass', 'dataobject'],
    classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
