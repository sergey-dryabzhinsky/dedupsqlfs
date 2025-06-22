from collections import namedtuple
from recordclass import dataobject, datastruct, make_dictclass
from recordclass import litetuple, mutabletuple
from timeit import timeit
import sys
import gc

_PY311 = sys.version_info[:2] >= (3, 11)

PointNT = namedtuple("PointNT", "x y z")
nan = float('nan')

class PyPoint:

    def __init__(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z

    def norm2(self):
        return self.x*self.x + self.y*self.y + self.z*self.z

class PointSlots:
    __slots__ = 'x', 'y', 'z'

    def __init__(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z

    def norm2(self):
        return self.x*self.x + self.y*self.y + self.z*self.z

class PointStruct(datastruct, sequence=True):
    x:int
    y:int
    z:int

    def norm2(self):
        return self.x*self.x + self.y*self.y + self.z*self.z

if _PY311:

    class Point(dataobject, sequence=True):
        x:int
        y:int
        z:int

        def norm2(self):
            return self.x*self.x + self.y*self.y + self.z*self.z

    class PointGC(dataobject, sequence=True, gc=True, immutable_type=True):
        x:int
        y:int
        z:int

        def norm2(self):
            return self.x*self.x + self.y*self.y + self.z*self.z
else:
    class Point(dataobject, sequence=True):
        x:int
        y:int
        z:int

        def norm2(self):
            return self.x*self.x + self.y*self.y + self.z*self.z

    class PointGC(dataobject, sequence=True, gc=True):
        x:int
        y:int
        z:int

        def norm2(self):
            return self.x*self.x + self.y*self.y + self.z*self.z

class PointMap(dataobject, mapping=True, fast_new=True):
    x:int
    y:int
    z:int

    def norm2(self):
        return self.x*self.x + self.y*self.y + self.z*self.z


# PointDict = make_dictclass("PointDict", "x y z", fast_new=True)

results = {'id':[], 'size':[], 'new':[],
           'getattr':[], 'setattr':[],
           'getitem':[], 'setitem':[],
           'getkey':[], 'setkey':[],
           'iterate':[], 'copy':[],
          }

results['id'].extend(
    ['litetuple', 'mutabletuple', 'tuple', 'namedtuple', 'class+slots', 'datastruct', 'dataobject',
     'dataobject+gc', 'dict', 'dataobject+map', 'class'])

classes = (litetuple, mutabletuple, tuple, PointNT, PointSlots,
           PointStruct, Point, PointGC, dict, PointMap, PyPoint)

N = 300_000

numbers = 10

def test_new():
    print("new")
    def test(cls):
        for i in range(N):
            ob = cls(i, i, i)
            prev = ob

    def test_tuple():
        for i in range(N):
            ob = tuple((i, i, i))
            prev = ob

    def test_dict():
        for i in range(N):
            ob = {'x':i, 'y':i, 'z':i}
            prev = ob

    for cls in classes:
        gc.collect()
        if cls is dict:
            res = timeit("test_dict()", number=numbers, globals={'test':test, 'test_dict':test_dict})
        elif cls is tuple:
            res = timeit("test_tuple()", number=numbers, globals={'test':test, 'test_tuple':test_tuple})
        else:
            res = timeit("test(cls)", number=numbers, globals={'cls':cls, 'test':test})
        # res = "%.2f" % res
        results['new'].append(res)

def test_copy():
    print("copy")
    def test(cls):
        ob = cls(0,0,0)
        for i in range(N):
            ob1 = ob.__copy__()

    def test2(cls):
        ob = cls(0,0,0)
        for i in range(N):
            ob1 = ob[:]

    def test_tuple():
        ob = (0,0,0)
        for i in range(N):
            ob1 = ob[:]

    def test_dict():
        ob = {'x':0,'y':0,'z':0}
        for i in range(N):
            ob1 = ob.copy()

    for cls in classes:
        gc.collect()
        if cls is dict:
            res = timeit("test_dict()", number=numbers, globals={'test':test, 'test_dict':test_dict})
        elif cls is tuple:
            res = timeit("test_tuple()", number=numbers, globals={'test':test, 'test_tuple':test_tuple})
        elif cls is PointNT:
            res = timeit("test2(cls)", number=numbers, globals={'cls':cls, 'test2':test2})
        elif cls in (PointSlots, PyPoint):
            res = nan
        else:
            res = timeit("test(cls)", number=numbers, globals={'cls':cls, 'test':test})
        results['copy'].append(res)

def test_getattr():
    print("getattr")
    def test(cls):
        p = cls(0,0,0)
        for i in range(N):
            x = p.x
            y = p.y
            z = p.z

    for cls in classes:
        gc.collect()
        if cls in (litetuple,mutabletuple,tuple,dict):
            res = nan
        else:
            res = timeit("test(cls)", number=numbers, globals={'cls':cls, 'test':test})
        results['getattr'].append(res)

def test_getkey():
    print("getkey")

    def test_dict():
        p = {'x':0, 'y':0, 'z':0}
        for i in range(N):
            x = p['x']
            y = p['y']
            z = p['z']

    def test_dictclass(cls):
        p = cls(0,0,0)
        for i in range(N):
            x = p['x']
            y = p['y']
            z = p['z']

    for cls in classes:
        gc.collect()
        if cls is dict:
            res = timeit("test_dict()", number=numbers, globals={'test_dict':test_dict})
        elif cls in (PointMap,):
            res = timeit("test_dictclass(cls)", number=numbers, globals={'cls':cls, 'test_dictclass':test_dictclass})
        else:
            res = nan
        results['getkey'].append(res)

def test_getitem():
    print("getitem")
    def test(cls):
        p = cls(0,0,0)
        for i in range(N):
            x = p[0]
            y = p[1]
            z = p[2]

    def test_tuple():
        p = (0,0,0)
        for i in range(N):
            x = p[0]
            y = p[1]
            z = p[2]

    for cls in classes:
        gc.collect()
        if cls in (dict, PointSlots, PointMap, PyPoint):
            res = nan
        elif cls is tuple:
            res = timeit("test_tuple()", number=numbers, globals={'test':test, 'test_tuple':test_tuple})
        else:
            res = timeit("test(cls)", number=numbers, globals={'cls':cls, 'test':test})
        results['getitem'].append(res)

def test_setattr():
    print("setattr")
    def test(cls):
        p = cls(0,0,0)
        for i in range(N):
            p.x = 1
            p.y = 2
            p.z = 3

    for cls in classes:
        gc.collect()
        if cls in (litetuple, mutabletuple, tuple, PointNT, dict):
            res = nan
        else:
            res = timeit("test(cls)", number=numbers, globals={'cls':cls, 'test':test, 'tuple':tuple, 'PointNT':PointNT})
        results['setattr'].append(res)

def test_setkey():
    print("setkey")
    def test_dict():
        p = {'x':0, 'y':0, 'z':0}
        for i in range(N):
            p['x'] = 1
            p['y'] = 2
            p['z'] = 3

    def test_dictclass(cls):
        p = cls(0,0,0)
        for i in range(N):
            p['x'] = 1
            p['y'] = 2
            p['z'] = 3

    for cls in classes:
        gc.collect()
        if cls in (PointMap,):
            res = timeit("test_dictclass(cls)", number=numbers, globals={'cls':cls, 'test_dictclass':test_dictclass})
        elif cls is dict:
            res = timeit("test_dict()", number=numbers, globals={'test_dict':test_dict})
        else:
            res = nan
        results['setkey'].append(res)

def test_setitem():
    print("setitem")
    def test(cls):
        p = cls(0,0,0)
        for i in range(N):
            p[0] = 1
            p[1] = 2
            p[2] = 2

    for cls in classes:
        gc.collect()
        if cls in (litetuple, dict, tuple, PointNT, PointSlots, PointMap, PyPoint):
            res = nan
        else:
            res = timeit("test(cls)", number=numbers, globals={'cls':cls, 'test':test})
        results['setitem'].append(res)

def test_getmethod():
    print("getmethod")
    def test(cls):
        p = cls(0,0,0)
        for i in range(N):
            a = p.norm2

    for cls in classes:
        gc.collect()
        if cls in (litetuple, mutabletuple, dict, tuple, PointNT,):
            res = nan
        else:
            res = timeit("test(cls)", number=numbers, globals={'cls':cls, 'test':test})
        results['getmethod'].append(res)

def test_iterate():
    print("iterate")
    def test(cls):
        p = cls(0,0,0)
        for i in range(N):
            for x in iter(p):
                s = x

    def test_tuple():
        p = (0,0,0)
        for i in range(N):
            for x in iter(p):
                s = x

    def test_dict():
        p = {'x':0, 'y':0, 'z':0}
        for i in range(N):
            for x in iter(p):
                s = x

    for cls in classes:
        gc.collect()
        if cls is dict:
            res = timeit("test_dict()", number=numbers, globals={'test':test, 'test_dict':test_dict})
        elif cls is tuple:
            res = timeit("test_tuple()", number=numbers, globals={'test':test, 'test_tuple':test_tuple})
        elif cls in (PointSlots, PyPoint):
            res = nan
        else:
            res = timeit("test(cls)", number=numbers, globals={'cls':cls, 'test':test})
        results['iterate'].append(res)

def test_all(relative=True):
    from math import isnan
    from prettytable import PrettyTable

    test_new()
    test_getattr()
    test_setattr()
    test_getitem()
    test_setitem()
    test_getkey()
    test_setkey()
    # test_getmethod()
    test_iterate()
    test_copy()

    results['size'].extend([
      sys.getsizeof(litetuple(0,0,0)),
      sys.getsizeof(mutabletuple(0,0,0)),
      sys.getsizeof((0,0,0)),
      sys.getsizeof(PointNT(0,0,0)),
      sys.getsizeof(PointSlots(0,0,0)),
      sys.getsizeof(PointStruct(0,0,0)),
      sys.getsizeof(Point(0,0,0)),
      sys.getsizeof(PointGC(0,0,0)),
      sys.getsizeof({'x':0,'y':0, 'z':0}),
      sys.getsizeof(PointMap(0,0,0)),
      sys.getsizeof(PyPoint(0,0,0)),
      # sys.getsizeof(PointDict(0,0,0)),
    ])

    for key in results.keys():
        if key in ('id', 'size'):
            continue
        results[key] = [(round(x,2) if not isnan(x) else ' ')  for x in results[key]]

    pt = PrettyTable()
    pt.field_names = list(results.keys())
    for k in range(len(results['id'])):
        pt.add_row([results[key][k] for key in pt.field_names])

    pt.align = 'l'

    return pt

pt = test_all()
print(pt)
# print(pt.get_html_string())

