# -*- coding: utf8 -*-

from time import time
import heapq

__author__ = 'sergey'

class StorageTTLseconds(object):

    # Maximum seconds after that cache is expired for writed blocks
    _max_write_ttl = 5
    # Maximum cache size in bytes for block that writed recently
    _max_write_cache_size = 256*1024*1024
    # Maximum records in cache that writed recently
    _max_write_count = 0
    # Maximum seconds after that cache is expired for readed blocks
    _max_read_ttl = 10
    # Maximum cache size in bytes for block that accessed recently
    _max_read_cache_size = 256*1024*1024
    # Maximum records in cache that accessed recently
    _max_read_count = 0
    # Blocks count treshhold in %
    _max_count_trsh = 10

    _inodes = None
    _block_size = 128*1024

    _count_writed = 0
    _count_readed = 0

    def __init__(self):
        self._inodes = {}
        self._max_write_count = int(self._max_write_cache_size / self._block_size)
        self._max_read_count = int(self._max_read_cache_size / self._block_size)
        pass

    def __len__(self):
        s = 0
        for inode in self._inodes:
            s += len(self._inodes[inode])
        return s

    def setBlockSize(self, in_bytes):
        self._block_size = in_bytes
        self._max_write_count = int(self._max_write_cache_size / self._block_size)
        self._max_read_count = int(self._max_read_cache_size / self._block_size)
        return self

    def setMaxWriteCacheSize(self, in_bytes):
        self._max_write_cache_size = in_bytes
        self._max_write_count = int(self._max_write_cache_size / self._block_size)
        return self

    def setMaxReadCacheSize(self, in_bytes):
        self._max_read_cache_size = in_bytes
        self._max_read_count = int(self._max_read_cache_size / self._block_size)
        return self

    def setMaxWriteTtl(self, seconds):
        self._max_write_ttl = seconds
        return self

    def setMaxReadTtl(self, seconds):
        self._max_read_ttl = seconds
        return self

    def set(self, inode, block_number, block, writed=False):
        """
        @type   inode: int
        @type   block_number: int
        @type   block: BytesIO
        @type   writed: bool
        """

        inode = str(inode)
        block_number = str(block_number)

        new = False
        if inode not in self._inodes:
            self._inodes[ inode ] = {}
            new = True

        inode_data = self._inodes[inode]

        if block_number not in inode_data:
            inode_data[ block_number ] = {}
            new = True

        block_data = inode_data[block_number]

        block_data["time"] = time()
        block_data["block"] = block

        if writed:
            if new:
                self._count_writed += 1
            else:
                if not block_data["w"]:
                    self._count_writed += 1
                    self._count_readed -= 1

            block_data["w"] = True
        else:
            if new:
                self._count_readed += 1

            block_data["w"] = block_data.get("w", False)

        #inode_data[ block_number ] = block_data
        #self._inodes[ inode ] = inode_data

        return self

    def get(self, inode, block_number, default=None):

        inode = str(inode)
        block_number = str(block_number)


        inode_data = self._inodes.get(inode, {})

        block_data = inode_data.get(block_number, {})

        if not block_data:
            return default

        now = time()
        t = block_data["time"]
        if now - t > self._max_write_ttl:
            return default

        val = block_data.get("block", default)

        # update last request time
        block_data["time"] = time()

        return val

    def isWritedCacheFull(self):
        return 100.0 * self._count_writed / self._max_write_count - 100.0 >= self._max_count_trsh

    def isReadCacheFull(self):
        return 100.0 * self._count_readed / self._max_read_count - 100.0 >= self._max_count_trsh

    def expired(self, writed=False):
        now = time()

        if writed:
            old_inodes = {}
        else:
            old_inodes = 0

        for inode in tuple(self._inodes.keys()):

            inode_data = self._inodes[inode]

            for bn in tuple(inode_data.keys()):
                block_data = inode_data[bn]

                t = block_data["time"]
                if block_data["w"] != writed:
                    continue

                if now - t > self._max_write_ttl:
                    if writed:
                        old_inode_data = old_inodes.get(inode, {})
                        old_inode_data[bn] = block_data.copy()
                        old_inodes[inode] = old_inode_data
                    else:
                        old_inodes += 1
                    del inode_data[bn]
                    if writed:
                        self._count_writed -= 1
                    else:
                        self._count_readed -= 1

            if not inode_data and inode in self._inodes:
                del self._inodes[inode]

        return old_inodes

    def expireByCount(self, writed=False):
        now = time()

        # 1. Fill heap

        heap = []

        for inode in tuple(self._inodes.keys()):

            inode_data = self._inodes[inode]

            for bn in tuple(inode_data.keys()):
                block_data = inode_data[bn]

                t = block_data["time"]
                if block_data["w"] != writed:
                    continue

                heapq.heappush(heap, (int((now - t)*10**6), inode, bn,))

        # 2. Sort heap

        if writed:
            mostrecent = heapq.nsmallest(self._max_write_count, heap)
        else:
            mostrecent = heapq.nsmallest(self._max_read_count, heap)

        # 3. Convert data

        heap_inodes = {}
        for dt, inode, bn in mostrecent:
            if inode not in heap_inodes:
                heap_inodes[ inode ] = ()
            heap_inodes[ inode ] += (bn,)

        del heap
        del mostrecent

        # 4. Expire cache, filter by new data

        if writed:
            old_inodes = {}
        else:
            old_inodes = 0

        for inode in tuple(self._inodes.keys()):

            inode = str(inode)

            inode_data = self._inodes[inode]

            for bn in tuple(inode_data.keys()):

                bn = str(bn)

                block_data = inode_data[bn]

                if block_data["w"] != writed:
                    continue

                # Expire inodes that in heap or by block numbers
                if inode not in heap_inodes or\
                    bn not in heap_inodes[ inode ]:

                    if writed:
                        old_inode_data = old_inodes.get(inode, {})
                        old_inode_data[bn] = block_data.copy()
                        old_inodes[inode] = old_inode_data
                    else:
                        old_inodes += 1
                    del inode_data[bn]
                    if writed:
                        self._count_writed -= 1
                    else:
                        self._count_readed -= 1

            if not inode_data and inode in self._inodes:
                del self._inodes[inode]

        return old_inodes

    def clear(self):
        old_inodes = self._inodes.copy()
        self._inodes = {}
        self._count_readed = 0
        self._count_writed = 0
        return old_inodes
