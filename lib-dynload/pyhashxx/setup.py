from setuptools import find_packages, setup, Extension

headers = [  'src/xxhash.h',
             'src/pycompat.h',
         ]
sources = [ 'src/xxhash.c',
            'src/pyhashxx.c',
        ]
pyhashxx = Extension('_pyhashxx', sources=sources, depends=headers)

setup(
    name = "pyhashxx",
    version = "0.1.3",
    description = "Python wrapper for xxHash algorithm",
    author = "Ewen Cheslack-Postava",
    author_email = 'me@ewencp.org',
    platforms=["any"],
    license="BSD",
    url = "http://github.com/ewencp/pyhashxx",
    packages = find_packages(),
    ext_modules = [ pyhashxx ],
    test_suite = "tests",
    headers = headers,
)
