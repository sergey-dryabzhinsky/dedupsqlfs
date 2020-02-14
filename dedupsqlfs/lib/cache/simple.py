# -*- coding: utf8 -*-
"""
@author Sergey Dryabzhinsky
"""

from time import time
from dedupsqlfs.lib.cache import TimedCache

"""
@2020-01-17 New cache item interface

CacheItem:
    c_time: float, timestamp, then added, updated, set to 0 if expired
    c_value: int|str|object, some data

CompressionSizesValue:
    size_c: int, compressed data size
    size_w: int, written data size

"""

class CacheItem:
    __slots__ = 'c_time', 'c_value'

    def __init__(self, c_time=0.0, c_value=None):
        self.c_time = c_time
        self.c_value = c_value

class CompressionSizesValue:
    __slots__ = 'size_c', 'size_w'

    def __init__(self, size_c=0, size_w=0):
        self.size_c = size_c
        self.size_w = size_w

try:
    from recordclass import make_dataclass

    CacheItem = make_dataclass(
        "CacheItem",
        [("c_time", float,), ("c_value", object,)],
        defaults=(0.0, None,)
    )
    CompressionSizesValue = make_dataclass(
        "CompressionSizesValue",
        [("size_c", int,), ("size_w", int,)],
        defaults=(0, 0,)
    )
except:
    pass

class CacheTTLseconds(TimedCache):
    """
    Simple cache storage
    
    {
        key (int | str) : CacheItem, ...
    }
    
    """

    _max_ttl = 30

    _storage = None

    def __init__(self):
        self._storage = {}
        super().__init__()

    def __len__(self):
        return len(self._storage)

    def set_max_ttl(self, seconds):
        self._max_ttl = seconds
        return self

    def set(self, key, value):
        self.startTimer()
        self._storage[ key ] = CacheItem(time(), value)
        self.stopTimer("set")
        return self

    def get(self, key, default=None):
        self.startTimer()
        # not setted
        now = time()

        item = self._storage.get(key)
        if item is None:
            item = CacheItem(0, default)
        val = item.c_value
        t = item.c_time

        if now - t > self._max_ttl:
            return val

        # update time only if value was set
        if key in self._storage:
            self._storage[ key ].c_time = now

        self.stopTimer("get")
        return val

    def unset(self, key):
        self.startTimer()
        if key in self._storage:
            del self._storage[ key ]
        self.stopTimer("unset")
        return self

    def clear(self):
        self.startTimer()
        now = time()
        count = 0
        for key in set(self._storage.keys()):
            item = self._storage[key]
            if now - item.c_time > self._max_ttl:
                del self._storage[key]
                count += 1
        self.stopTimer("clear")
        return count
