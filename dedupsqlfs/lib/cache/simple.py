# -*- coding: utf8 -*-

from time import time

__author__ = 'sergey'

class CacheTTLseconds(object):
    """
    Simple cache storage
    
    {
        key (int | str) : [
            timestamp (float),      - then added, updated, set to 0 if expired
            values (int | str)      - some data
        ], ...
    }
    
    """

    OFFSET_TIME = 0
    OFFSET_VALUE = 1

    _max_ttl = 300

    _storage = None

    def __init__(self):
        self._storage = {}
        pass

    def __len__(self):
        return len(self._storage)

    def set_max_ttl(self, seconds):
        self._max_ttl = seconds
        return self

    def set(self, key, value):
        self._storage[ key ] = [time(), value]
        return self

    def get(self, key, default=None):
        # not setted
        val = self._storage.get(key, [0, default])[self.OFFSET_VALUE]
        now = time()

        # update time only if value was set
        if key in self._storage:
            self._storage[ key ][self.OFFSET_TIME] = now

        return val

    def unset(self, key):
        if key in self._storage:
            del self._storage[ key ]
        return self

    def clear(self):
        now = time()
        count = 0
        for key, item in tuple(self._storage.items()):
            if now - item[self.OFFSET_TIME] > self._max_ttl:
                del self._storage[key]
                count += 1
        return count
