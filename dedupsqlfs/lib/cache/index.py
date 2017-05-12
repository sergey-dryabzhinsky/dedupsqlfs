# -*- coding: utf8 -*-
"""
@author Sergey Dryabzhinsky
"""

from time import time

class IndexTime(object):
    """
    Cache storage for inode-block index
    
    {
        inode (int) : {
            block_number (int) : [
                timestamp (float),      - then added, updated, set to 0 if expired
                hash_id (int)           - block hash ID
            ], ...
        }, ...
    }
    
    Just to not lookup in SQLdb
    """

    OFFSET_TIME = 0
    OFFSET_HASH = 1

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

    def set(self, inode, block_number, hash_id):
        """
        @type   inode: int
        @type   block_number: int
        @type   hash_id: int
        """

        new = False
        if inode not in self._inodes:
            self._inodes[ inode ] = {}
            new = True

        inode_data = self._inodes[inode]

        if block_number not in inode_data:
            inode_data[ block_number ] = [0, hash_id,]
            new = True

        hash_data = inode_data[block_number]

        # If time not set to 0 (expired)
        if hash_data[self.OFFSET_TIME]:
            new = True

        if new:
            hash_data[self.OFFSET_TIME] = time()
        hash_data[self.OFFSET_HASH] = hash_id

        return self

    def get(self, inode, block_number, default=None):

        now = time()

        inode_data = self._inodes.get(inode, {})

        hash_data = inode_data.get(block_number, [0, default])

        val = hash_data[self.OFFSET_HASH]

        t = hash_data[self.OFFSET_TIME]
        if now - t > self._max_ttl:
            return val

        # update last request time
        hash_data[self.OFFSET_TIME] = now

        return val

    def expireBlock(self, inode, block_number):

        removed = False

        if inode in self._inodes:
            inode_data = self._inodes.get(inode, {})

            if block_number in inode_data:
                block_data = inode_data[block_number]
                block_data[self.OFFSET_TIME] = 0
                removed = True

            if not inode_data:
                removed = True

        return removed

    def expire(self, inode):
        inode = str(inode)
        if inode in self._inodes:
            inode_data = self._inodes[inode]
            for bn in inode_data.keys():
                inode_data[bn][self.OFFSET_TIME] = 0
            self._inodes[inode] = inode_data
        return

    def expired(self):
        now = time()

        old_inodes = 0

        for inode in tuple(self._inodes.keys()):

            inode_data = self._inodes[inode]

            for bn in tuple(inode_data.keys()):
                block_data = inode_data[bn]

                t = block_data[self.OFFSET_TIME]
                if now - t > self._max_ttl:
                    old_inodes += 1

                    del inode_data[bn]

            if not inode_data and inode in self._inodes:
                del self._inodes[inode]

        return old_inodes

    def clear(self):
        count = len(self)
        self._inodes = {}
        return count
