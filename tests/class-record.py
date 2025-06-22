#/usr/bin/env python3
# -*- coding: utf8 -*-

import sys
import os
import time
import random
import struct
import io
import timeit

dirname = "dedupsqlfs"

# Figure out the directy which is the prefix
# path-of-current-file/..
curpath = os.path.abspath( sys.argv[0] )
if os.path.islink(curpath):
    curpath = os.readlink(curpath)
currentdir = os.path.dirname( curpath )
basedir = os.path.abspath( os.path.join( currentdir, "..") )
print(basedir)
dynloaddir = os.path.abspath( os.path.join( basedir, "lib-dynload" ) )
print(dynloaddir)

ddsqlfsdir = os.path.abspath( os.path.join( basedir, dirname ) )

if os.path.isdir(dynloaddir):
    sys.path.insert( 0, dynloaddir )
sys.path.insert( 0, ddsqlfsdir )
sys.path.insert( 0, basedir )

from dedupsqlfs.get_memory_usage import get_real_memory_usage
from dedupsqlfs.my_formats import format_size

try:
    from _recordclass import module as recordclass, loaded
    from recordclass import make_dataclass
except Exception as e:
    def make_dataclass(classname, properteies, defaults):
        class cp(object):
          x=0
          y=0
        return cp
    pass

if make_dataclass is not None:
    RPoint = make_dataclass(
        "RPoint",
        [("x", float,), ("y", float,)],
        defaults=(0.0, 0.0,)
    )

n_objects = 100000

def usual_object():
  class CPoint:
    x=0
    y=0
    def __init__(self, x=0, y=0):
      self.x=x
      self.y=y
  pass

def rec_object():
  RPoint(0, 0)
  pass

memory_usage1 = 0
def test_speed_usual():
  global memory_usage1
  memory_usage_1 = get_real_memory_usage()
  t=timeit.timeit(usual_object,number=n_objects)
  memory_usage_2 = get_real_memory_usage()
  memory_usage1 = memory_usage_2 - memory_usage_1
  print("All done in %s seconds" %t)
  pass


memory_usage2 = 0
def test_speed_rec():
  global memory_usage2
  memory_usage_1 = get_real_memory_usage()
  t=timeit.timeit(rec_object,number=n_objects)
  memory_usage_2 = get_real_memory_usage()
  memory_usage2 = memory_usage_2 - memory_usage_1
  print("All done in %s seconds" %t)
  pass

def test_memory_usual():
  print("Memory wasted %s Mb by simple classes" % (memory_usage1/1024/1024))
  pass

def test_memory_rec():
  print("Memory wasted %s Mb by classes via recordclass" % (memory_usage2/1024/1024))
  pass

if len(sys.argv) >= 1:
    print("Test speed of creation %s usual objects" % n_objects)
    test_speed_usual()
    print("Test speed of creation %s rec objects" % n_objects)
    test_speed_rec()
#else:
    print("Test memory consumption of creation %s simple objects" % n_objects)
    test_memory_usual()
    print("Test memory consumption of creation %s rec objects" % n_objects)
    test_memory_rec()
print("Done")

