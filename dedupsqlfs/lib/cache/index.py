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
    c_item: dict, block index row

"""

class CacheItem:
    __slots__ = 'c_time', 'c_item'

    def __init__(self, c_time=0.0, c_item=None):
        self.c_time = c_time
        self.c_item = c_item

make_dataclass = None
RC_VERSION = "0.0.0"
import sys
try:
    # Our lib-dynload module
    from _recordclass import loaded
    print(loaded)
    if loaded:
        from _recordclass import module as recordclass
        from _recordclass import module
        RC_VERSION = module.__version__
except Exception as e:
#    print(e)
#    print(sys.path)
    pass

try:
    from recordclass import make_dataclass
except:
    pass

if make_dataclass is not None:
    CacheItem = make_dataclass(
        "CacheItem",
        [("c_time", float,), ("c_item", object,)],
        defaults=(0.0, None,)
    )

class IndexTime(TimedCache):
    """
    Cache storage for inode-block index
    
    {
        inode (int) : {
            block_number (int) : CacheItem, ...
        }, ...
    }
    
    Just to not lookup in SQLdb
    """

    # Maximum seconds after that cache is expired for writed blocks
    _max_ttl = 10

    _inodes = None

    def __init__(self):
        self._inodes = {}
        super().__init__()

    def __len__(self):
        """
        For clear() count
        @return: int
        """
        self.startTimer()
        s = 0
        for inode in self._inodes:
            s += len(self._inodes[inode])
        self.stopTimer("__len__")
        return s

    def setMaxTtl(self, seconds):
        self._max_ttl = seconds
        return self

    def set(self, inode, block_number, item):
        """
        @type   inode: int
        @type   block_number: int
        @type   item: object
        """
        self.startTimer()

        new = False
        if inode not in self._inodes:
            self._inodes[ inode ] = {}
            new = True

        inode_data = self._inodes[inode]

        if block_number not in inode_data:
            inode_data[ block_number ] = CacheItem(0, item)
            new = True

        hash_data = inode_data[block_number]

        # If time not set to 0 (expired)
        if hash_data.c_time:
            new = True

        if new:
            hash_data.c_time = time()
        hash_data.c_item = item

        self.stopTimer("set")
        return self

    def get(self, inode, block_number, default=None):
        self.startTimer()

        now = time()

        inode_data = self._inodes.get(inode, {})

        hash_data = inode_data.get(block_number)
        if hash_data is None:
            hash_data = CacheItem(0, default)

        if now - hash_data.c_time <= self._max_ttl:
            # update last request time
            hash_data.c_time = now

        self.stopTimer("get")
        return hash_data.c_item

    def expireBlock(self, inode, block_number):
        self.startTimer()

        removed = False

        if inode in self._inodes:
            inode_data = self._inodes.get(inode, {})

            if block_number in inode_data:
                block_data = inode_data[block_number]
                block_data.c_time = 0
                removed = True

            if not inode_data:
                removed = True

        self.stopTimer("expireBlock")
        return removed

    def expire(self, inode):
        self.startTimer()
        if inode in self._inodes:
            inode_data = self._inodes[inode]
            for bn in inode_data.keys():
                inode_data[bn].c_time = 0
        self.stopTimer("expire")
        return

    def expired(self):
        self.startTimer()
        now = time()

        old_inodes = 0

        for inode in set(self._inodes.keys()):

            inode_data = self._inodes[inode]

            for bn in set(inode_data.keys()):
                block_data = inode_data[bn]

                if now - block_data.c_time > self._max_ttl:
                    old_inodes += 1

                    del inode_data[bn]

            if not inode_data and inode in self._inodes:
                del self._inodes[inode]

        self.stopTimer("expired")
        return old_inodes

    def clear(self):
        count = len(self)
        self.startTimer()
        self._inodes = {}
        self.stopTimer("clear")
        return count
