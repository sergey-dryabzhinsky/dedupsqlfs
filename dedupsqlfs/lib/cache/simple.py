# -*- coding: utf8 -*-

from time import time

__author__ = 'sergey'

class CacheTTLseconds(object):

    _max_ttl = 300

    _keys = None
    _values = None

    def __init__(self):
        self._keys = {}
        self._values = {}
        pass

    def __len__(self):
        return len(self._keys)

    def set_max_ttl(self, seconds):
        self._max_ttl = seconds
        return self

    def set(self, key, value):
        self._keys[ key ] = time()
        self._values[ key ] = value
        return self

    def get(self, key, default=None):
        # not setted
        val = self._values.get(key, default)
        now = time()
        # expired
        t = self._keys.get( key, 0 )
        if t + self._max_ttl < now:
            return val

        self._keys[ key ] = now

        return val

    def unset(self, key):
        if key in self._keys:
            del self._keys[ key ]
            del self._values[ key ]
        return self

    def clear(self):
        now = time()
        count = 0
        for key, t in tuple(self._keys.items()):
            if t + self._max_ttl < now:
                self.unset(key)
                count += 1
        return count
