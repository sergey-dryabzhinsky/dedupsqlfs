# -*- coding: utf8 -*-

from time import time

__author__ = 'sergey'

class IndexTime(object):

    # Maximum seconds after that cache is expired for writed blocks
    _max_ttl = 10

    _inodes = None

    def __init__(self):
        self._inodes = {}
        pass

    def __len__(self):
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

        inode = str(inode)
        block_number = str(block_number)

        if inode not in self._inodes:
            self._inodes[ inode ] = {}

        inode_data = self._inodes[inode]

        if block_number not in inode_data:
            inode_data[ block_number ] = {}

        hash_data = inode_data[block_number]

        hash_data["time"] = time()
        hash_data["hash"] = hash_id

        return self

    def get(self, inode, block_number, default=None):

        inode = str(inode)
        block_number = str(block_number)

        now = time()

        inode_data = self._inodes.get(inode, {})

        hash_data = inode_data.get(block_number, {
            "time" : now
        })

        val = hash_data.get("hash", default)

        # update last request time
        hash_data["time"] = time()

        return val

    def unset(self, inode, block_number):

        inode = str(inode)
        block_number = str(block_number)

        removed = False

        if inode in self._inodes:
            inode_data = self._inodes.get(inode, {})

            if block_number in inode_data:
                block_data = inode_data[block_number]
                block_data["time"] = 0
                removed = True

            if not inode_data:
                removed = True

        return removed

    def forget(self, inode):
        inode = str(inode)
        if inode in self._inodes:
            inode_data = self._inodes[inode]
            for bn in tuple(inode_data.keys()):
                inode_data[bn]["time"] = 0
            self._inodes[inode] = inode_data
        return

    def expired(self):
        now = time()

        old_inodes = 0

        for inode in tuple(self._inodes.keys()):

            inode_data = self._inodes[inode]

            for bn in tuple(inode_data.keys()):
                block_data = inode_data[bn]

                t = block_data["time"]

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
