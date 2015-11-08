__author__ = 'sergey'

import os
import sys
import subprocess
from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext
from Cython.Build import cythonize

args = sys.argv[1:]

# scan the 'dvedit' directory for extension files, converting
# them to extension names in dotted notation
def scandir(dir, files=[]):
    d = os.path.dirname(dir)
    if d == "tests":
        return files
    for file in os.listdir(dir):
        path = os.path.join(dir, file)
        if os.path.isfile(path) and path.endswith(".py") and path.find("__.py") == -1:
            files.append(path.replace(os.path.sep, ".")[:-3])
        elif os.path.isdir(path):
            scandir(path, files)
    return files

# generate an Extension object from its dotted name
def makeExtension(extName):
    extPath = extName.replace(".", os.path.sep)+".py"
    return Extension(extName, [extPath])

def cleanAllExtension(extName):
    extDir = os.path.dirname(extName)

    oldPath = os.getcwd()
    os.chdir(extDir)

    # Just in case the build directory was created by accident,
    # note that shell=True should be OK here because the command is constant.
    subprocess.Popen("rm -rf __pycache__", shell=True, executable="/bin/bash")
    subprocess.Popen("rm -rf build", shell=True, executable="/bin/bash")
    subprocess.Popen("rm -rf *.c", shell=True, executable="/bin/bash")
    subprocess.Popen("rm -rf *.so", shell=True, executable="/bin/bash")

    os.chdir(oldPath)

def cleanPyExtension(extName):
    extDir = os.path.dirname(extName)

    oldPath = os.getcwd()
    os.chdir(extDir)

    # Just in case the build directory was created by accident,
    # note that shell=True should be OK here because the command is constant.
    subprocess.Popen("rm -rf __pycache__", shell=True, executable="/bin/sh")
    subprocess.Popen("rm -rf build", shell=True, executable="/bin/sh")
    subprocess.Popen("rm -rf *.c", shell=True, executable="/bin/sh")
    os.chdir(oldPath)

    if os.path.isfile(extName):
        os.unlink(extName)


def stripExtension(extName):
    subprocess.Popen("strip -s " + extName, shell=True, executable="/bin/sh")


# get the list of extensions
extNames = scandir("dedupsqlfs")

# Make a `cleanall` rule to get rid of intermediate and library files
if "cleanall" in args:
    print("Deleting cython files...")

    for n in extNames:
        cleanAllExtension(n.replace(".", os.path.sep) + ".py")
    subprocess.Popen("rm -rf build", shell=True, executable="/bin/bash")

    sys.exit(0)

# Make a `cleanpy` rule to get rid of old raw python files
if "cleanpy" in args:
    print("Deleting python files...")

    for n in extNames:
        cleanPyExtension(n.replace(".", os.path.sep) + ".py")
    subprocess.Popen("rm -rf build", shell=True, executable="/bin/bash")

    sys.exit(0)

# Make a `stripall` rule to get rid of old raw python files
if "stripall" in args:
    print("Strip debug from cython files...")

    for n in extNames:
        stripExtension(n.replace(".", os.path.sep) + "*.so")

    sys.exit(0)


# We want to always use build_ext --inplace
if args.count("build_ext") > 0 and args.count("--inplace") == 0:
    sys.argv.insert(sys.argv.index("build_ext")+1, "--inplace")


# and build up the set of Extension objects
extensions = [makeExtension(name) for name in extNames]

setup(
    ext_modules = cythonize(extensions),
    name="dedupsqlfs",
    packages=["dedupsqlfs",],
    cmdclass = {'build_ext': build_ext},
)
