# -*- coding: utf8 -*-

from time import time
import heapq

__author__ = 'sergey'

class StorageTTLseconds(object):

    # Maximum seconds after that cache is expired for writed blocks
    _max_write_ttl = 5

    # Maximum cache size in bytes for block that writed recently
    _max_write_cache_size = 256*1024*1024
    _cur_write_cache_size = 0

    # Maximum seconds after that cache is expired for readed blocks
    _max_read_ttl = 10

    # Maximum cache size in bytes for block that accessed recently
    _max_read_cache_size = 256*1024*1024
    _cur_read_cache_size = 0

    # Expired maximum cache size in %
    _max_size_trsh = 10

    _inodes = None
    _block_size = 128*1024

    def __init__(self):
        self._inodes = {}
        pass

    def __len__(self):
        s = 0
        for inode in self._inodes:
            s += len(self._inodes[inode])
        return s

    def setBlockSize(self, in_bytes):
        self._block_size = in_bytes
        return self

    def setMaxWriteCacheSize(self, in_bytes):
        self._max_write_cache_size = in_bytes
        return self

    def setMaxReadCacheSize(self, in_bytes):
        self._max_read_cache_size = in_bytes
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
            inode_data[ block_number ] = {
                "size" : 0,
                "w" : writed
            }
            new = True

        block_data = inode_data[block_number]

        blockSize = len(block.getvalue())

        if not new:
            # Not new block
            oldBlockSize = block_data["size"]
            if writed:
                # If it now writed
                if not block_data["w"]:
                    # But not was
                    self._cur_read_cache_size -= oldBlockSize
                else:
                    self._cur_write_cache_size -= oldBlockSize
            else:
                self._cur_read_cache_size -= oldBlockSize

        if writed:
            self._cur_write_cache_size += blockSize
        else:
            self._cur_read_cache_size += blockSize

        block_data["time"] = time()
        block_data["block"] = block
        block_data["size"] = blockSize

        if writed:
            block_data["w"] = True

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

    def getCachedSize(self, writed=False):
        size = 0
        for inode in self._inodes.keys():
            for block_data in self._inodes[inode].values():
                if block_data["w"] != writed:
                    continue

                block_data["block"].seek(0, 2)
                size += block_data["block"].tell()
                block_data["block"].seek(0, 0)
        return size


    def isWritedCacheFull(self):
        filled = 100.0 * self._cur_write_cache_size / self._max_write_cache_size
        max_fill = 100 + self._max_size_trsh
        return filled > max_fill

    def isReadCacheFull(self):
        filled = 100.0 * self._cur_read_cache_size / self._max_read_cache_size
        max_fill = 100 + self._max_size_trsh
        return filled > max_fill

    def expired(self, writed=False):
        now = time()

        if writed:
            old_inodes = {}
        else:
            old_inodes = 0

        for inode in tuple(self._inodes.keys()):

            inode_data = self._inodes[inode]
            updated = False

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

                        self._cur_write_cache_size -= block_data["size"]
                    else:
                        old_inodes += 1

                        self._cur_read_cache_size -= block_data["size"]

                    del inode_data[bn]
                    updated = True

            if not inode_data and inode in self._inodes:
                del self._inodes[inode]
            elif updated:
                self._inodes[inode] = inode_data

        return old_inodes

    def expireByCount(self, writed=False):
        now = time()

        # 1. Fill heap

        heap = []

        inodesKeys = tuple(self._inodes.keys())

        for inode in inodesKeys:

            inode_data = self._inodes[inode]

            for bn in inode_data.keys():
                block_data = inode_data[bn]

                t = block_data["time"]
                if block_data["w"] != writed:
                    continue

                heapq.heappush(
                    heap, (
                        int((now - t)*10**6),
                        inode,
                        bn,
                        len(block_data["block"].getvalue())
                    )
                )

        # 2. Sort heap

        if writed:
            currentSize = self._cur_write_cache_size
            maxSize = self._max_write_cache_size
        else:
            currentSize = self._cur_read_cache_size
            maxSize = self._max_read_cache_size

        needMaxSize = maxSize * (100 - self._max_size_trsh) / 100

        nget = int((currentSize - needMaxSize) / self._block_size)

        mostrecent = []
        while True:
            needSize = currentSize
            mostrecent = heapq.nsmallest(nget, heap)
            for dt, inode, bn, bsize in mostrecent:
                needSize -= bsize
            if needSize <= needMaxSize:
                break
            nget += 1

        # 3. Convert data

        heap_inodes = {}
        for dt, inode, bn, bsize in mostrecent:
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

        for inode in inodesKeys:

            inode_data = self._inodes[inode]

            for bn in tuple(inode_data.keys()):

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

                        self._cur_write_cache_size -= block_data["size"]
                    else:
                        old_inodes += 1

                        self._cur_read_cache_size -= block_data["size"]

                    del inode_data[bn]

            if not inode_data and inode in self._inodes:
                del self._inodes[inode]

        return old_inodes

    def clear(self):
        old_inodes = self._inodes.copy()
        self._inodes = {}
        return old_inodes
