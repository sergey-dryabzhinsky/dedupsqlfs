# -*- coding: utf8 -*-
"""
@author Sergey Dryabzhinsky
"""

from time import time

"""
@2020-01-17 New cache item interface

CacheItem:
    c_time: float, timestamp, then added, updated, set to 0 if expired
    c_data: dict, inode raw row
    c_written: bool, data was written, updated
    c_toflush: bool, data must be flushed as soos as possible, but not expired

"""

class CacheItem:
    __slots__ = 'c_time', 'c_data', 'c_written', 'c_toflush'

    def __init__(self, c_time=0.0, c_data=None, c_written=False, c_toflush=False):
        self.c_time = c_time
        self.c_data = c_data
        self.c_written = c_written
        self.c_toflush = c_toflush

try:
    from recordclass import dataobject
    class CacheItem(dataobject):
        c_time: float = 0.0
        c_data: object = None
        c_written: bool = False
        c_toflush: bool = False
except:
    pass

class InodesTime(object):
    """
    Cache storage for inode raw attributes

    {
        inode (int) : CacheItem, ...
    }

    Just to not lookup in SQLdb

    """

    # Maximum seconds after that cache is expired for writed inodes
    _max_ttl = 5

    _inodes = None

    def __init__(self):
        self._inodes = {}
        pass

    def __len__(self):
        s = 0
        for inode in self._inodes:
            s += len(self._inodes[inode])
        return s

    def set_max_ttl(self, seconds):
        self._max_ttl = seconds
        return self

    def set(self, inode, data, writed=False):
        """
        @type   inode: int
        @type   data: dict
        @type   writed: bool
        """

        new = False
        if inode not in self._inodes:
            c = CacheItem()
            c.c_time = 0
            c.c_data = data
            c.c_written = writed
            c.c_toflush = writed
            self._inodes[ inode ] = c
            new = True

        inode_data = self._inodes[inode]

        # If time not set to 0 (expired)
        if inode_data.c_time:
            new = True

        if new:
            inode_data.c_time = time()
        inode_data.c_data = data

        if writed:
            inode_data.c_written = True
            inode_data.c_toflush = True

        return self

    def get(self, inode, default=None):

        now = time()

        inode_data = self._inodes.get(inode)
        if inode_data is None:
            inode_data = CacheItem()
            inode_data.c_time = 0
            inode_data.c_data = default
            inode_data.c_written = False
            inode_data.c_toflush = False

        if now - inode_data.c_time <= self._max_ttl:
            # update last request time
            inode_data.c_time = now

        return inode_data.c_data


    def expired(self):
        """
        Gather inode data to be written:

        1. Which targeted to FLUSH explicit

        2. Which was written and expited by time

        @return: tuple(int, dict{ inode: data})
        """
        now = time()

        write_inodes = {}
        readed_inodes = 0

        for inode in set(self._inodes.keys()):

            inode_data = self._inodes[inode]

            # Get data to FLUSH (and if requested written attrs)
            if inode_data.c_toflush:
                write_inodes[inode] = inode_data.c_data.copy()
                inode_data.c_toflush = False

            if now - inode_data.c_time > self._max_ttl:
                if inode_data.c_written:
                    if not write_inodes.get(inode):
                        write_inodes[inode] = inode_data.c_data.copy()
                else:
                    readed_inodes += 1

                del self._inodes[inode]

        return (readed_inodes, write_inodes,)


    def flush(self, inode):
        """
        Do not remove but set flush flag
        """
        if inode in self._inodes:
            self._inodes[ inode ].c_toflush = True
        return self


    def expire(self, inode):
        """
        Do not remove but expire
        """
        if inode in self._inodes:
            self._inodes[ inode ].c_time = 0
        return self


    def clear(self):
        write_inodes = {}

        for inode in set(self._inodes.keys()):

            inode_data = self._inodes[inode]

            del self._inodes[inode]

            if not inode_data.c_written:
                continue

            write_inodes[inode] = inode_data.c_data.copy()

        self._inodes = {}
        return write_inodes
