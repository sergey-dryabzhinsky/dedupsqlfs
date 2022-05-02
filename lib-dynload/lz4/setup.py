#!/usr/bin/env python
from setuptools import setup, find_packages, Extension
import sys
from distutils import ccompiler

# Note: if updating LZ4_REQUIRED_VERSION you need to update docs/install.rst as
# well.
LZ4_REQUIRED_VERSION = '>= 1.7.5'

# Check to see if we have a suitable lz4 library installed on the system and
# use if so. If not, we'll use the bundled libraries.
liblz4_found = False

EXTRA_OPT=0
if "--extra-optimization" in sys.argv:
    # Support legacy output format functions
    EXTRA_OPT=1
    sys.argv.remove("--extra-optimization")

try:
    from pkgconfig import installed as pkgconfig_installed
    from pkgconfig import parse as pkgconfig_parse
except ImportError:
    # pkgconfig is not installed. It will be installed by setup_requires.
    pass
else:
    def pkgconfig_installed_check(lib, required_version, default):
        installed = default
        try:
            installed = pkgconfig_installed(lib, required_version)
        except EnvironmentError:
            # Windows, no pkg-config present
            pass
        except ValueError:
            # pkgconfig was unable to determine if
            # required version of liblz4 is available
            # Bundled version of liblz4 will be used
            pass
        return installed
    liblz4_found = pkgconfig_installed_check('liblz4', LZ4_REQUIRED_VERSION, default=False)

# Set up the extension modules. If a system wide lz4 library is found, and is
# recent enough, we'll use that. Otherwise we'll build with the bundled one. If
# we're building against the system lz4 library we don't set the compiler
# flags, so they'll be picked up from the environment. If we're building
# against the bundled lz4 files, we'll set the compiler flags to be consistent
# with what upstream lz4 recommends.

extension_kwargs = {}

lz4version_sources = [
    '_lz4/_version.c'
]

lz4block_sources = [
    '_lz4/block/_block.c'
]

lz4frame_sources = [
    '_lz4/frame/_frame.c'
]

lz4stream_sources = [
    '_lz4/stream/_stream.c'
]

if liblz4_found is True:
    extension_kwargs['libraries'] = ['_lz4']
else:
    extension_kwargs['include_dirs'] = ['lz4libs']
    lz4version_sources.extend(
        [
            'lz4libs/lz4.c',
        ]
    )
    lz4block_sources.extend(
        [
            'lz4libs/lz4.c',
            'lz4libs/lz4hc.c',
        ]
    )
    lz4frame_sources.extend(
        [
            'lz4libs/lz4.c',
            'lz4libs/lz4hc.c',
            'lz4libs/lz4frame.c',
            'lz4libs/xxhash.c',
        ]
    )
    lz4stream_sources.extend(
        [
            'lz4libs/lz4.c',
            'lz4libs/lz4hc.c',
        ]
    )

compiler = ccompiler.get_default_compiler()

if compiler == 'msvc':
    extension_kwargs['extra_compile_args'] = [
        EXTRA_OPT and '/O2' or '/Ot',
        '/Wall',
        '/wd4711',
        '/wd4820',
    ]
elif compiler in ('unix', 'mingw32'):
    if liblz4_found:
        extension_kwargs = pkgconfig_parse('liblz4')
    else:
        extension_kwargs['extra_compile_args'] = [
            '-O2',
            '-Wall',
            '-Wundef'
        ]
        if EXTRA_OPT:
            extension_kwargs['extra_compile_args'].extend(['-O3', '-march=native'])
else:
    print('Unrecognized compiler: {0}'.format(compiler))
    sys.exit(1)

lz4version = Extension('_lz4._version',
                       lz4version_sources,
                       **extension_kwargs)

lz4block = Extension('_lz4.block._block',
                     lz4block_sources,
                     **extension_kwargs)

lz4frame = Extension('_lz4.frame._frame',
                     lz4frame_sources,
                     **extension_kwargs)

lz4stream = Extension('_lz4.stream._stream',
                      lz4stream_sources,
                      **extension_kwargs)

install_requires = []

# On Python earlier than 3.0 the builtins package isn't included, but it is
# provided by the future package
if sys.version_info < (3, 0):
    install_requires.append('future')


# Finally call setup with the extension modules as defined above.
setup(
    name='_lz4',
    version='4.0.0',
    python_requires=">=3.4",
    setup_requires=[
        'setuptools',
        'pkgconfig',
    ],
    install_requires=install_requires,
    description="LZ4 Bindings for Python",
    long_description=open('README.rst', 'r').read(),
    author='Jonathan Underwood',
    author_email='jonathan.underwood@gmail.com',
    url='https://github.com/python-lz4/python-lz4',
    packages=find_packages(),
    ext_modules=[
        lz4version,
        lz4block,
        lz4frame,
        lz4stream
    ],
    extras_require={
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Programming Language :: C',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
)
