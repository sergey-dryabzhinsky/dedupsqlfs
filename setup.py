__author__ = 'sergey'

import os
import sys
import subprocess
from distutils.core import setup
from distutils.extension import Extension


CYTHON_BUILD = 0
if "--cython-build" in sys.argv:
    # Support compiling with Cython
    CYTHON_BUILD = 1
    sys.argv.remove("--cython-build")

    from Cython.Distutils import build_ext
    from Cython.Build import cythonize
else:
    from distutils.command.build_ext import build_ext

args = sys.argv[1:]

def scandir(dir, files=[]):
    """
    scan the 'dvedit' directory for extension files, converting
    them to extension names in dotted notation

    @param dir:
    @param files:
    @return:
    """
    d = os.path.dirname(dir)
    if d == "tests":
        return files
    for file in os.listdir(dir):
        path = os.path.join(dir, file)
        if os.path.isfile(path) and path.endswith(".py") and path.find("__.py") == -1 and path.find('migrations') == -1:
            files.append(path.replace(os.path.sep, ".")[:-3])
        elif os.path.isdir(path):
            scandir(path, files)
    return files

# generate an Extension object from its dotted name
def makeExtension(extName):
    extPath = extName.replace(".", os.path.sep)+".py"
    return Extension(extName, [extPath])

def makeExtensionLib(extName):
    extPath = extName.replace(".", os.path.sep)+".py"
    extName = extName.replace('lib-dynload.', '')
    return Extension(extName, [extPath])

def cleanAllExtension(extName):

    if os.path.isfile(extName):
        extDir = os.path.dirname(extName)
    else:
        extName = 'lib-dynload/' + extName
    if os.path.isfile(extName):
        extDir = os.path.dirname(extName)
    else:
        return

    oldPath = os.getcwd()
    os.chdir(extDir)

    # Just in case the build directory was created by accident,
    # note that shell=True should be OK here because the command is constant.
    subprocess.Popen("rm -rvf __pycache__", shell=True, executable="/bin/sh")
    subprocess.Popen("rm -rvf build", shell=True, executable="/bin/sh")
    subprocess.Popen("rm -rvf *.c", shell=True, executable="/bin/sh")
    subprocess.Popen("rm -rvf *.so", shell=True, executable="/bin/sh")

    os.chdir(oldPath)

def cleanBuildExtension(extName):
    if os.path.isfile(extName):
        extDir = os.path.dirname(extName)
    else:
        extName = 'lib-dynload/' + extName
    if os.path.isfile(extName):
        extDir = os.path.dirname(extName)
    else:
        return

    oldPath = os.getcwd()
    os.chdir(extDir)

    # Just in case the build directory was created by accident,
    # note that shell=True should be OK here because the command is constant.
    subprocess.Popen("rm -rvf __pycache__", shell=True, executable="/bin/sh")
    subprocess.Popen("rm -rvf build", shell=True, executable="/bin/sh")
    subprocess.Popen("rm -rvf *.c", shell=True, executable="/bin/sh")

    os.chdir(oldPath)

def cleanPyExtension(extName):
    if os.path.isfile(extName):
        extDir = os.path.dirname(extName)
    else:
        extName = 'lib-dynload/' + extName
    if os.path.isfile(extName):
        extDir = os.path.dirname(extName)
    else:
        return

    oldPath = os.getcwd()
    os.chdir(extDir)

    # Just in case the build directory was created by accident,
    # note that shell=True should be OK here because the command is constant.
    subprocess.Popen("rm -rvf __pycache__", shell=True, executable="/bin/sh")
    subprocess.Popen("rm -rvf build", shell=True, executable="/bin/sh")
    subprocess.Popen("rm -rvf *.c", shell=True, executable="/bin/sh")
    os.chdir(oldPath)

    if os.path.isfile(extName):
        subprocess.Popen("rm -rvf '%s'" % extName, shell=True, executable="/bin/sh")


def stripExtension(extName):
    if not os.path.isfile(extName):
        extName = 'lib-dynload/' + extName
    if not os.path.isfile(extName):
        return
    subprocess.Popen("strip -sv " + extName, shell=True, executable="/bin/sh")


# get the list of extensions
extNamesD = []
scandir("dedupsqlfs", extNamesD)
extNamesL = []
scandir("lib-dynload/_pymysql", extNamesL)

extNames = []
extNames.extend(extNamesD)
extNames.extend(extNamesL)

# Make a `cleanall` rule to get rid of intermediate and library files
if "cleanall" in args:
    print("Deleting cython files...")

    for n in extNames:
        cleanAllExtension(n.replace(".", os.path.sep) + ".py")
    subprocess.Popen("rm -rvf build _pymysql", shell=True, executable="/bin/sh")

    subprocess.Popen("find lib-dynload/_pymysql -type f -iname '*.c' -exec rm -vf '{}' \;", shell=True, executable="/bin/sh")
    subprocess.Popen("find lib-dynload/_pymysql -type f -iname '*.so' -exec rm -vf '{}' \;", shell=True, executable="/bin/sh")

    sys.exit(0)

# Make a `cleanpy` rule to get rid of old raw python files
if "cleanpy" in args:
    print("Deleting python files...")

    for n in extNames:
        cleanPyExtension(n.replace(".", os.path.sep) + ".py")
    subprocess.Popen("rm -rvf build _pymysql", shell=True, executable="/bin/sh")

    subprocess.Popen("find lib-dynload/_pymysql -type f -iname '*.c' -exec rm -vf '{}' \;", shell=True, executable="/bin/sh")
    subprocess.Popen("find lib-dynload/_pymysql -type f -iname '*.py' -exec rm -vf '{}' \;", shell=True, executable="/bin/sh")

    sys.exit(0)

# Make a `cleanbuild` rule to get rid of cython build artefacts
if "cleanbuild" in args:
    print("Deleting cython build files...")

    for n in extNames:
        cleanBuildExtension(n.replace(".", os.path.sep) + ".py")
    subprocess.Popen("rm -rvf build _pymysql", shell=True, executable="/bin/sh")

    subprocess.Popen("find lib-dynload/_pymysql -type f -iname '*.c' -exec rm -vf '{}' \;", shell=True, executable="/bin/sh")

    sys.exit(0)

# Make a `stripall` rule to get rid of old raw python files
if "stripall" in args:
    print("Strip debug from cython files...")

    for n in extNames:
        stripExtension(n.replace(".", os.path.sep) + "*.so")

    subprocess.Popen("find lib-dynload/_pymysql -type f -iname '*.so' -exec strip -vs '{}' \;", shell=True, executable="/bin/sh")

    sys.exit(0)


# We want to always use build_ext --inplace
if args.count("build_ext") > 0 and args.count("--inplace") == 0:
    sys.argv.insert(sys.argv.index("build_ext")+1, "--inplace")


classifiers=[
    'License :: OSI Approved :: MIT License',
    'Intended Audience :: Developers',
    'Intended Audience :: System Administrators',
    'Development Status :: 4 - Beta',
    'Operating System :: POSIX',
    'Programming Language :: C',
    'Programming Language :: C++',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
    'Programming Language :: Python :: 3.10',
    'Programming Language :: Python :: 3.11',
]


# and build up the set of Extension objects
if CYTHON_BUILD:
    dynloaddir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), "lib-dynload"))
    sys.path.insert(0, dynloaddir)

    extensions = []
    extensions.extend([makeExtension(name) for name in extNamesD])
    extensions.extend([makeExtensionLib(name) for name in extNamesL])
    setup(
        ext_modules = cythonize(extensions, language_level=3),
        name="dedupsqlfs",
        version="1.2.953-dev",
        packages=["dedupsqlfs",],
        cmdclass = {'build_ext': build_ext}, requires=['llfuse'],
        classifiers=classifiers
    )
    subprocess.Popen("cp -vfr _pymysql lib-dynload/", shell=True, executable="/bin/sh")
    subprocess.Popen("rm -vfr _pymysql", shell=True, executable="/bin/sh")
else:
    extensions = [makeExtension(name) for name in extNamesD]
    setup(
        ext_modules = extensions,
        name="dedupsqlfs",
        version="1.2.953-dev",
        packages=["dedupsqlfs",],
        cmdclass = {'build_ext': build_ext}, requires=['llfuse'],
        classifiers=classifiers
    )
