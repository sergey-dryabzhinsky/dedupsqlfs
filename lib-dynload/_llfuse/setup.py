#!/usr/bin/env python3
'''
setup.py

Installation script for Python-LLFUSE.

Copyright (c) 2010 Nikolaus Rath <Nikolaus.org>

This file is part of Python-LLFUSE. This work may be distributed under
the terms of the GNU LGPL.
'''


import sys
import os
import subprocess
import warnings
import re

# Disable Cython support in setuptools. It fails under some conditions
# (http://trac.cython.org/ticket/859), and we have our own build_cython command
# anyway.
try:
    import Cython.Distutils.build_ext
except ImportError:
    pass
else:
    # We can't delete Cython.Distutils.build_ext directly,
    # because the build_ext class (that is imported from
    # the build_ext module in __init__.py) shadows the
    # build_ext module.
    module = sys.modules['Cython.Distutils.build_ext']
    del module.build_ext

try:
    import setuptools
except ImportError:
    raise SystemExit('Setuptools package not found. Please install from '
                     'https://pypi.python.org/pypi/setuptools')
from setuptools import Extension
from distutils.version import LooseVersion

# Add util to load path
basedir = os.path.abspath(os.path.dirname(sys.argv[0]))
sys.path.insert(0, os.path.join(basedir, 'util'))

# when running with DEVELOPER_MODE=1 in the environment:
# enable all warnings, abort on some warnings.
DEVELOPER_MODE = os.getenv('DEVELOPER_MODE', '0') == '1'
if DEVELOPER_MODE:
    print('running in developer mode')
    warnings.resetwarnings()
    # We can't use `error`, because e.g. Sphinx triggers a
    # DeprecationWarning.
    warnings.simplefilter('default')

# Add src to load path, important for Sphinx autodoc
# to work properly
sys.path.insert(0, os.path.join(basedir, 'src'))

LLFUSE_VERSION = '1.5.1'


EXTRA_OPT="RC_EXTRAOPT" in os.environ
if "--extra-optimization" in sys.argv:
    # Support legacy output format functions
    EXTRA_OPT=True
    sys.argv.remove("--extra-optimization")


def main():

    try:
        from sphinx.application import Sphinx #pylint: disable-msg=W0612
    except ImportError:
        pass
    else:
        fix_docutils()

    with open(os.path.join(basedir, 'README.rst')) as fh:
        long_desc = fh.read()

    compile_args = pkg_config('fuse', cflags=True, ldflags=False, min_ver='2.8.0')
    compile_args += ['-DFUSE_USE_VERSION=29', '-Wall', '-Wextra', '-Wconversion',
                     '-Wsign-compare', '-DLLFUSE_VERSION="%s"' % LLFUSE_VERSION]

    # We may have unused functions if we compile for older FUSE versions
    compile_args.append('-Wno-unused-function')

    # Nothing wrong with that if you know what you are doing
    # (which Cython does)
    compile_args.append('-Wno-implicit-fallthrough')

    # Due to platform specific conditions, these are unavoidable
    compile_args.append('-Wno-unused-parameter')

    # Enable all fatal warnings only when compiling from Mercurial tip.
    # (otherwise we break forward compatibility because compilation with newer
    # compiler may fail if additional warnings are added)
    if DEVELOPER_MODE:
        compile_args.append('-Werror')
        compile_args.append('-Wfatal-errors')

        # Unreachable code is expected because we need to support multiple
        # platforms and architectures.
        compile_args.append('-Wno-error=unreachable-code')

        # Value-changing conversions should always be explicit.
        compile_args.append('-Werror=conversion')

        # Note that (i > -1) is false if i is unsigned (-1 will be converted to
        # a large positive value). We certainly don't want to do this by
        # accident.
        compile_args.append('-Werror=sign-compare')

    link_args = pkg_config('fuse', cflags=False, ldflags=True, min_ver='2.8.0')
    link_args.append('-lpthread')
    c_sources = ['src/llfuse.c', 'src/lock.c']

    if os.uname()[0] in ('Linux', 'GNU/kFreeBSD'):
        link_args.append('-lrt')
    elif os.uname()[0] == 'Darwin':
        c_sources.append('src/darwin_compat.c')

    if EXTRA_OPT:
        compile_args.insert(0, "-march=native")
        compile_args.insert(0, "-O3")
        link_args.insert(0, "-Wl,-s")
    else:
        compile_args.insert(0, "-O2")


    setuptools.setup(
          name='llfuse',
          zip_safe=True,
          version=LLFUSE_VERSION,
          description='Python bindings for the low-level FUSE API',
          long_description=long_desc,
          author='Nikolaus Rath',
          author_email='Nikolaus@rath.org',
          url='https://github.com/python-llfuse/python-llfuse/',
          license='LGPL',
          classifiers=['Development Status :: 4 - Beta',
                       'Intended Audience :: Developers',
                       'Programming Language :: Python',
                       'Programming Language :: Python :: 3',
                       'Programming Language :: Python :: 3.4',
                       'Programming Language :: Python :: 3.5',
                       'Programming Language :: Python :: 3.6',
                       'Programming Language :: Python :: 3.7',
                       'Programming Language :: Python :: 3.8',
                       'Programming Language :: Python :: 3.9',
                       'Programming Language :: Python :: 3.10',
                       'Programming Language :: Python :: 3.11',
                       'Programming Language :: Python :: 3.12',
                       'Topic :: Software Development :: Libraries :: Python Modules',
                       'Topic :: System :: Filesystems',
                       'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
                       'Operating System :: POSIX :: Linux',
                       'Operating System :: MacOS :: MacOS X',
                       'Operating System :: POSIX :: BSD :: FreeBSD'],
          platforms=[ 'Linux', 'FreeBSD', 'OS X' ],
          keywords=['FUSE', 'python' ],
          package_dir={'': 'src'},
          packages=setuptools.find_packages('src'),
          python_requires='>=3.4',
          tests_require=['pytest >= 3.4.0'],
          provides=['llfuse'],
          ext_modules=[Extension('llfuse', c_sources,
                                  extra_compile_args=compile_args,
                                  extra_link_args=link_args)],
        cmdclass={'upload_docs': upload_docs,
                  'build_cython': build_cython },
          command_options={
            'build_sphinx': {
                'version': ('setup.py', LLFUSE_VERSION),
                'release': ('setup.py', LLFUSE_VERSION),
            }},
          )


def pkg_config(pkg, cflags=True, ldflags=False, min_ver=None):
    '''Frontend to ``pkg-config``'''

    if min_ver:
        cmd = ['pkg-config', pkg, '--atleast-version', min_ver ]

        if subprocess.call(cmd) != 0:
            cmd = ['pkg-config', '--modversion', pkg ]
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            version = proc.communicate()[0].strip()
            if not version:
                raise SystemExit(2) # pkg-config generates error message already
            else:
                raise SystemExit('%s version too old (found: %s, required: %s)'
                                 % (pkg, version, min_ver))

    cmd = ['pkg-config', pkg ]
    if cflags:
        cmd.append('--cflags')
    if ldflags:
        cmd.append('--libs')

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    cflags = proc.stdout.readline().rstrip()
    proc.stdout.close()
    if proc.wait() != 0:
        raise SystemExit(2) # pkg-config generates error message already

    return cflags.decode('us-ascii').split()


class upload_docs(setuptools.Command):
    user_options = []
    boolean_options = []
    description = "Upload documentation"

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        subprocess.check_call(['rsync', '-aHv', '--del', os.path.join(basedir, 'doc', 'html') + '/',
                               'ebox.rath.org:/srv/www.rath.org/llfuse-docs/'])

class build_cython(setuptools.Command):
    user_options = []
    boolean_options = []
    description = "Compile .pyx to .c"

    def initialize_options(self):
        pass

    def finalize_options(self):
        self.extensions = self.distribution.ext_modules

    def run(self):
        try:
            version = subprocess.check_output(['cython', '--version'],
                                              universal_newlines=True,
                                              stderr=subprocess.STDOUT)
        except OSError:
            raise SystemExit('Cython needs to be installed for this command')

        hit = re.match('^Cython version (.+)$', version)
        if not hit or LooseVersion(hit.group(1)) < "0.29":
            # in fact, we need a very recent Cython version (like 0.29.24) to support py39
            raise SystemExit('Need Cython 0.29 or newer, found ' + version)

        cmd = ['cython', '-Wextra', '--force', '-3', '--fast-fail',
               '--directive', 'embedsignature=True', '--include-dir',
               os.path.join(basedir, 'Include'), '--verbose' ]
        if DEVELOPER_MODE:
            cmd.append('-Werror')

        # Work around http://trac.cython.org/cython_trac/ticket/714
        cmd += ['-X', 'warn.maybe_uninitialized=False' ]

        for extension in self.extensions:
            for file_ in extension.sources:
                (file_, ext) = os.path.splitext(file_)
                path = os.path.join(basedir, file_)
                if ext != '.c':
                    continue
                if os.path.exists(path + '.pyx'):
                    if subprocess.call(cmd + [path + '.pyx']) != 0:
                        raise SystemExit('Cython compilation failed')

def fix_docutils():
    '''Work around https://bitbucket.org/birkenfeld/sphinx/issue/1154/'''

    import docutils.parsers
    from docutils.parsers import rst
    old_getclass = docutils.parsers.get_parser_class

    # Check if bug is there
    try:
        old_getclass('rst')
    except AttributeError:
        pass
    else:
        return

    def get_parser_class(parser_name):
        """Return the Parser class from the `parser_name` module."""
        if parser_name in ('rst', 'restructuredtext'):
            return rst.Parser
        else:
            return old_getclass(parser_name)
    docutils.parsers.get_parser_class = get_parser_class

    assert docutils.parsers.get_parser_class('rst') is rst.Parser

if __name__ == '__main__':
    main()
