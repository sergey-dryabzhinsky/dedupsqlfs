# -*- coding: utf8 -*-
"""
@author Sergey Dryabzhinsky
"""

from time import time

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

try:
    from recordclass import dataobject
    class CacheItem(dataobject):
        c_time: float = 0.0
        c_item: object = None
except:
    pass

class IndexTime(object):
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
        pass

    def __len__(self):
        """
        For clear() count
        @return: int
        """
        s = 0
        for inode in self._inodes:
            s += len(self._inodes[inode])
        return s

    def setMaxTtl(self, seconds):
        self._max_ttl = seconds
        return self

    def set(self, inode, block_number, item):
        """
        @type   inode: int
        @type   block_number: int
        @type   item: int
        """

        new = False
        if inode not in self._inodes:
            self._inodes[ inode ] = {}
            new = True

        inode_data = self._inodes[inode]

        if block_number not in inode_data:
            c = CacheItem()
            c.c_time = 0
            c.c_item = item
            inode_data[ block_number ] = c
            new = True

        hash_data = inode_data[block_number]

        # If time not set to 0 (expired)
        if hash_data.c_time:
            new = True

        if new:
            hash_data.c_time = time()
        hash_data.c_item = item

        return self

    def get(self, inode, block_number, default=None):

        now = time()

        inode_data = self._inodes.get(inode, {})

        hash_data = inode_data.get(block_number)
        if hash_data is None:
            hash_data = CacheItem()
            hash_data.c_time = 0
            hash_data.c_item = default

        if now - hash_data.c_time <= self._max_ttl:
            # update last request time
            hash_data.c_time = now

        return hash_data.c_item

    def expireBlock(self, inode, block_number):

        removed = False

        if inode in self._inodes:
            inode_data = self._inodes.get(inode, {})

            if block_number in inode_data:
                block_data = inode_data[block_number]
                block_data.c_time = 0
                removed = True

            if not inode_data:
                removed = True

        return removed

    def expire(self, inode):
        if inode in self._inodes:
            inode_data = self._inodes[inode]
            for bn in inode_data.keys():
                inode_data[bn].c_time = 0
        return

    def expired(self):
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

        return old_inodes

    def clear(self):
        count = len(self)
        self._inodes = {}
        return count
