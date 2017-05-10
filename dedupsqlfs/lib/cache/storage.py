# -*- coding: utf8 -*-

from time import time
import heapq

__author__ = 'sergey'

class StorageTimeSize(object):

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
    _max_size_trsh = 5

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
                "size" : 0,         # Written size
                "w" : writed,       # Recently written
                "f" : writed        # Force flush
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

        # If time not set to 0 (expired)
        if block_data.get("time", 0):
            new = True

        if new:
            block_data["time"] = time()
        block_data["block"] = block
        block_data["size"] = blockSize

        if writed:
            block_data["w"] = True
            block_data["f"] = True

        return self

    def get(self, inode, block_number, default=None):

        inode = str(inode)
        block_number = str(block_number)

        now = time()

        inode_data = self._inodes.get(inode, {})

        block_data = inode_data.get(block_number, {
            "time" : 0          # Don't create empty item with good time
        })

        if not block_data:
            return default

        val = block_data.get("block", default)

        t = block_data["time"]
        if now - t > self._max_write_ttl:
            return val

        # update last request time
        block_data["time"] = now

        return val

    def getCachedSize(self, writed=False):
        size = 0
        for inode in self._inodes.keys():
            for block_data in self._inodes[inode].values():
                if block_data["w"] != writed:
                    continue

                size += len(block_data["block"].getvalue())
        return size


    def isWritedCacheFull(self):
        if self._max_write_cache_size < 0:
            return False
        filled = 100.0 * self._cur_write_cache_size / self._max_write_cache_size
        max_fill = 100 + self._max_size_trsh
        return filled > max_fill

    def isReadCacheFull(self):
        if self._max_read_cache_size < 0:
            return False
        filled = 100.0 * self._cur_read_cache_size / self._max_read_cache_size
        max_fill = 100 + self._max_size_trsh
        return filled > max_fill

    def forget(self, inode):
        """
        Delete inode info from cache if it don't have writed blocks anyhow
        If have - do expire blocks
        
        @param inode: 
        @return: bool 
        """
        inode = str(inode)
        canDel = True
        if inode in self._inodes:
            inode_data = self._inodes[inode]
            for bn in set(inode_data.keys()):
                block_data = inode_data[bn]
                inode_data[bn]["time"] = 0
                if block_data["w"] is True:
                    canDel = False
            if canDel:
                del self._inodes[inode]
        return canDel

    def expire(self, inode):
        """
        Expire inode data
        It will be removed from cache on expred() call
        
        @param inode: 
        @return: 
        """
        inode = str(inode)
        if inode in self._inodes:
            inode_data = self._inodes[inode]
            for bn in set(inode_data.keys()):
                inode_data[bn]["time"] = 0
        return

    def flush(self, inode):
        """
        Force inode blocks to flush on disk
        
        @param inode: 
        @return: 
        """
        inode = str(inode)
        if inode in self._inodes:
            inode_data = self._inodes[inode]
            for bn in set(inode_data.keys()):
                inode_data[bn]["f"] = True
        return

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

                # Get data to FLUSH (and if requested written blocks)
                if block_data["f"] and writed:
                    old_inode_data = old_inodes.get(inode, {})
                    old_inode_data[bn] = block_data.copy()
                    old_inodes[inode] = old_inode_data
                    block_data["f"] = False

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

            if not inode_data and inode in self._inodes:
                del self._inodes[inode]

        return old_inodes


    def expireByCount(self, writed=False):
        """
        Expired inodes data by in-memory bytes size of cache
        
        @param writed: 
        @return: dict or int 
        """

        now = time()

        # 1. Fill heap

        heap = []

        inodesKeys = set(self._inodes.keys())

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

        needMaxSize = int(maxSize * (100.0 - self._max_size_trsh) / 100.0)

        nget = int((currentSize - needMaxSize) / self._block_size)

        mostrecent = []
        while True:
            needSize = currentSize
            mostrecent = heapq.nlargest(nget, heap)
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

            for bn in set(inode_data.keys()):

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
