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

sys.path.insert( 0, dynloaddir )
sys.path.insert( 0, basedir )

n_objects = 100000

def usual_object():
  class CPoint:
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

def test_speed_usual():
  t=timeit.timeit('usual_object',number=n_objects)
  print("All done in %s seconds" %t)
  pass

def test_speed_slots():
  t=timeit.timeit('slots_object',number=n_objects)
  print("All done in %s seconds" %t)
  pass

test_memory():
  pass

if len(sys.argv) > 1:
    print("Test speed of creation %s usual objects" % n_objects)
    test_speed_usual()
    print("Test speed of creation %s slots objects" % n_objects)
    test_speed_slots()
else:
    print("Test memory consumption of creation %s objects" % n_objects)
    test_memory()
print("Done")

