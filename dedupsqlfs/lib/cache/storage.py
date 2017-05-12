# -*- coding: utf8 -*-
"""
@author Sergey Dryabzhinsky
"""

from time import time
import heapq

class StorageTimeSize(object):
    """
    Cache storage for inode-block index

    {
        inode (int) : {
            block_number (int) : [
                timestamp (float),      - then added, updated, set to 0 if expired
                block (BytesIO),        - block uncompressed data stream-like object
                size (int),             - size of block data
                written (bool)          - data was written, updated
                toflush (bool)          - data must be flushed as soos as possible, but not expired
            ], ...
        }, ...
    }

    Just to not lookup in SQLdb
    """

    OFFSET_TIME = 0
    OFFSET_BLOCK = 1
    OFFSET_SIZE = 2
    OFFSET_WRITTEN = 3
    OFFSET_TOFLUSH = 4

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

        new = False
        if inode not in self._inodes:
            self._inodes[ inode ] = {}
            new = True

        inode_data = self._inodes[inode]

        if block_number not in inode_data:
            inode_data[ block_number ] = [
                0, block, 0, writed, writed
            ]
            new = True

        block_data = inode_data[block_number]

        blockSize = len(block.getvalue())

        if not new:
            # Not new block
            oldBlockSize = block_data[self.OFFSET_SIZE]
            if writed:
                # If it now writed
                if not block_data[self.OFFSET_WRITTEN]:
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
        if block_data[self.OFFSET_TIME]:
            new = True

        if new:
            block_data[self.OFFSET_TIME] = time()
        block_data[self.OFFSET_BLOCK] = block
        block_data[self.OFFSET_SIZE] = blockSize

        if writed:
            block_data[self.OFFSET_WRITTEN] = True
            block_data[self.OFFSET_TOFLUSH] = True

        return self

    def get(self, inode, block_number, default=None):

        now = time()

        inode_data = self._inodes.get(inode, {})

        block_data = inode_data.get(block_number, [
                0, default, 0, False, False
            ])

        if not block_data:
            return default

        val = block_data[self.OFFSET_BLOCK]

        t = block_data[self.OFFSET_TIME]
        if now - t > self._max_write_ttl:
            return val

        # update last request time
        block_data[self.OFFSET_TIME] = now

        return val

    def getCachedSize(self, writed=False):
        size = 0
        for inode in self._inodes.keys():
            for block_data in self._inodes[inode].values():
                if block_data[self.OFFSET_WRITTEN] != writed:
                    continue

                size += len(block_data[self.OFFSET_BLOCK].getvalue())
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
        @type inode: int
        
        @return: bool 
        """
        canDel = True
        if inode in self._inodes:
            inode_data = self._inodes[inode]
            for bn in tuple(inode_data.keys()):
                block_data = inode_data[bn]
                inode_data[bn][self.OFFSET_TIME] = 0
                if block_data[self.OFFSET_WRITTEN] is True:
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
        if inode in self._inodes:
            inode_data = self._inodes[inode]
            for bn in tuple(inode_data.keys()):
                inode_data[bn][self.OFFSET_TIME] = 0
        return

    def flush(self, inode):
        """
        Force inode blocks to flush on disk
        
        @param inode: 
        @return: 
        """
        if inode in self._inodes:
            inode_data = self._inodes[inode]
            for bn in tuple(inode_data.keys()):
                inode_data[bn][self.OFFSET_TOFLUSH] = True
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
                if block_data[self.OFFSET_TOFLUSH] and writed:
                    old_inode_data = old_inodes.get(inode, {})
                    old_inode_data[bn] = block_data.copy()
                    old_inodes[inode] = old_inode_data
                    block_data[self.OFFSET_TOFLUSH] = False

                if block_data[self.OFFSET_WRITTEN] != writed:
                    continue

                t = block_data[self.OFFSET_TIME]
                if now - t > self._max_write_ttl:
                    if writed:
                        old_inode_data = old_inodes.get(inode, {})
                        old_inode_data[bn] = block_data.copy()
                        old_inodes[inode] = old_inode_data

                        self._cur_write_cache_size -= block_data[self.OFFSET_SIZE]
                    else:
                        old_inodes += 1

                        self._cur_read_cache_size -= block_data[self.OFFSET_SIZE]

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

        inodesKeys = tuple(self._inodes.keys())

        for inode in inodesKeys:

            inode_data = self._inodes[inode]

            for bn in inode_data.keys():
                block_data = inode_data[bn]

                if block_data[self.OFFSET_WRITTEN] != writed:
                    continue

                t = block_data[self.OFFSET_TIME]
                heapq.heappush(
                    heap, (
                        int((now - t)*10**6),
                        inode,
                        bn,
                        len(block_data[self.OFFSET_BLOCK].getvalue())
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

        mostoldest = []
        while True:
            needSize = currentSize
            mostoldest = heapq.nlargest(nget, heap)
            for dt, inode, bn, bsize in mostoldest:
                needSize -= bsize
            if needSize <= needMaxSize:
                break
            nget += 1

        # 3. Convert data

        heap_inodes = {}
        for dt, inode, bn, bsize in mostoldest:
            if inode not in heap_inodes:
                heap_inodes[ inode ] = ()
            heap_inodes[ inode ] += (bn,)

        del heap
        del mostoldest

        # 4. Expire cache, filter by new data

        if writed:
            oversize_inodes = {}
        else:
            oversize_inodes = 0

        for inode in tuple(heap_inodes.keys()):

            inode_data = self._inodes[inode]

            for bn in heap_inodes[inode]:

                block_data = inode_data[bn]

                if block_data[self.OFFSET_WRITTEN] != writed:
                    continue

                if writed:
                    osi_inode_data = oversize_inodes.get(inode, {})
                    osi_inode_data[bn] = block_data.copy()
                    oversize_inodes[inode] = osi_inode_data

                    self._cur_write_cache_size -= block_data[self.OFFSET_SIZE]
                else:
                    oversize_inodes += 1

                    self._cur_read_cache_size -= block_data[self.OFFSET_SIZE]

                del inode_data[bn]

            if not inode_data and inode in self._inodes:
                del self._inodes[inode]

        return oversize_inodes


    def clear(self):
        """
        Return all not written and not flushed blocks
        which must be written and flushed
        
        @return: dict 
        """

        old_inodes = {}

        for inode in tuple(self._inodes.keys()):

            inode_data = self._inodes[inode]

            for bn in tuple(inode_data.keys()):
                block_data = inode_data[bn]

                # Get data to FLUSH (and if requested written blocks)
                if block_data[self.OFFSET_TOFLUSH]:
                    old_inode_data = old_inodes.get(inode, {})
                    old_inode_data[bn] = block_data.copy()
                    old_inodes[inode] = old_inode_data
                    block_data[self.OFFSET_TOFLUSH] = False

                if not block_data[self.OFFSET_WRITTEN]:
                    continue

                old_inode_data = old_inodes.get(inode, {})
                old_inode_data[bn] = block_data.copy()
                old_inodes[inode] = old_inode_data

        self._inodes = {}
        return old_inodes
