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
basedir = os.path.abspath( os.path.join( currentdir, "..", ".." ) )

dynloaddir = os.path.abspath( os.path.join( basedir, "lib-dynload" ) )

ddsqlfsdir = os.path.abspath( os.path.join( basedir, dirname ) )

sys.path.insert( 0, dynloaddir )
sys.path.insert( 0, ddsqlfsdir )
sys.path.insert( 0, basedir )

from dedupsqlfs.get_memory_usage import get_memory_usage
from dedupsqlfs.my_formats import format_size

n_objects = 100000

def usual_object():
  class CPoint:
    x=0
    y=0
    def __init__(self, x=0, y=0):
      self.x=x
      self.y=y
  pass

def slots_object():
  class SPoint:
    __slots__= 'x, y'
    def __init__(self, x=0, y=0):
      self.x=x
      self.y=y
  pass

memory_usage1 = 0
def test_speed_usual():
  global memory_usage1
  memory_usage_1 = get_memory_usage()
  t=timeit.timeit(usual_object,number=n_objects)
  memory_usage_2 = get_memory_usage()
  memory_usage1 = memory_usage_2 - memory_usage_1
  print("All done in %s seconds" %t)
  pass


memory_usage2 = 0
def test_speed_slots():
  global memory_usage2
  memory_usage_1 = get_memory_usage()
  t=timeit.timeit(slots_object,number=n_objects)
  memory_usage_2 = get_memory_usage()
  memory_usage2 = memory_usage_2 - memory_usage_1
  print("All done in %s seconds" %t)
  pass

def test_memory_usual():
  t=timeit.timeit(usual_object,number=n_objects)
  print("Memory wasted %s by simple classes" % memory_usage1)
  pass

def test_memory_slots():
  t=timeit.timeit(slots_object,number=n_objects)
  memory_usage = get_memory_usage()
  print("Memory wasted %s by classes wth slots" % memory_usage2)
  pass

if len(sys.argv) >= 1:
    print("Test speed of creation %s usual objects" % n_objects)
    test_speed_usual()
    print("Test speed of creation %s slots objects" % n_objects)
    test_speed_slots()
else:
    print("Test memory consumption of creation %s simple objects" % n_objects)
    test_memory_usual()
    print("Test memory consumption of creation %s slots objects" % n_objects)
    test_memory_slots()
print("Done")

