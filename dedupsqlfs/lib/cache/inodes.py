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

make_dataclass = None

try:
    # Our lib-dynload module
    from _recordclass import loaded
    if loaded:
        from _recordclass import module as recordclass
except:
    pass

try:
    from recordclass import make_dataclass
except:
    pass

if make_dataclass is not None:
    CacheItem = make_dataclass(
        "CacheItem",
        [("c_time", float,), ("c_data", object,), ("c_written", bool,), ("c_toflush", bool,)],
        defaults=(0.0, None, False, False)
    )

class InodesTime(TimedCache):
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
        super().__init__()

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
        self.startTimer()

        new = False
        if inode not in self._inodes:
            self._inodes[ inode ] = CacheItem(0, data, writed, writed)
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

        self.stopTimer("set")
        return self

    def get(self, inode, default=None):
        self.startTimer()

        now = time()

        inode_data = self._inodes.get(inode)
        if inode_data is None:
            inode_data = CacheItem(0, default)

        if now - inode_data.c_time <= self._max_ttl:
            # update last request time
            inode_data.c_time = now

        self.stopTimer("get")
        return inode_data.c_data


    def expired(self):
        """
        Gather inode data to be written:

        1. Which targeted to FLUSH explicit

        2. Which was written and expited by time

        @return: tuple(int, dict{ inode: data})
        """
        self.startTimer()

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

        self.stopTimer("expired")
        return (readed_inodes, write_inodes,)


    def flush(self, inode):
        """
        Do not remove but set flush flag
        """
        self.startTimer()
        if inode in self._inodes:
            self._inodes[ inode ].c_toflush = True
        self.stopTimer("flush")
        return self


    def expire(self, inode):
        """
        Do not remove but expire
        """
        self.startTimer()
        if inode in self._inodes:
            self._inodes[ inode ].c_time = 0
        self.stopTimer("expire")
        return self


    def clear(self):
        self.startTimer()
        write_inodes = {}

        for inode in set(self._inodes.keys()):

            inode_data = self._inodes[inode]

            del self._inodes[inode]

            if not inode_data.c_written:
                continue

            write_inodes[inode] = inode_data.c_data.copy()

        self._inodes = {}
        self.stopTimer("clear")
        return write_inodes
