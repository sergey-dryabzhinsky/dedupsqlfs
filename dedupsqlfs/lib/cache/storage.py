# -*- coding: utf8 -*-
"""
@author Sergey Dryabzhinsky
"""

from time import time
import heapq
import copy

from dedupsqlfs.lib.cache import TimedCache

"""
@2020-01-17 New cache item interface

CacheItem:
    c_time: float, timestamp, then added, updated, set to 0 if expired
    c_block: BytesIO, block uncompressed data stream-like object
    c_size: int, size of block data
    c_written: bool, data was written, updated
    c_toflush: bool, data must be flushed as soos as possible, but not expired

"""

class CacheItem:
    __slots__ = 'c_time', 'c_block', 'c_size', 'c_written', 'c_toflush'

    def __init__(self, c_time=0.0, c_block=None, c_size=0, c_written=False, c_toflush=False):
        self.c_time = c_time
        self.c_block = c_block
        self.c_size = c_size
        self.c_written = c_written
        self.c_toflush = c_toflush

make_dataclass = None
RC_VERSION = "0.0.0"

try:
    # Our lib-dynload module
    from _recordclass import loaded
    if loaded:
        from _recordclass import module as recordclass
        from _recordclass import __version__ as RC_VERSION
except:
    pass

try:
    from recordclass import make_dataclass
except:
    pass

if make_dataclass is not None:
    CacheItem = make_dataclass(
        "CacheItem",
        [("c_time", float,), ("c_block", object,), ("c_size", int,), ("c_written", bool,), ("c_toflush", bool,)],
        defaults=(0.0, None, 0, False, False,)
    )

class StorageTimeSize(TimedCache):
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
    _max_write_ttl = 10

    # Maximum cache size in bytes for block that writed recently
    _max_write_cache_size = 512*1024*1024
    _cur_write_cache_size = 0

    # Maximum seconds after that cache is expired for readed blocks
    _max_read_ttl = 10

    # Maximum cache size in bytes for block that accessed recently
    _max_read_cache_size = 512*1024*1024
    _cur_read_cache_size = 0

    # Expired maximum cache size in %
    _max_size_trsh = 2

    _inodes = None
    _block_size = 128*1024


    def __init__(self):
        self._inodes = {}
        super().__init__()

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
        self.startTimer()

        new = False
        if inode not in self._inodes:
            self._inodes[ inode ] = {}
            new = True

        inode_data = self._inodes[inode]

        if block_number not in inode_data:
            inode_data[ block_number ] = CacheItem(0, block, 0, writed, writed)
            new = True

        block_data = inode_data[block_number]

        blockSize = len(block.getbuffer())

        if not new:
            # Not new block
            oldBlockSize = block_data.c_size
            if writed:
                # If it now writed
                if not block_data.c_written:
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
        if block_data.c_time:
            new = True

        if new:
            block_data.c_time = time()
        block_data.c_block = block
        block_data.c_size = blockSize

        if writed:
            block_data.c_written = True
#            block_data.c_toflush = True

        self.stopTimer("set")
        return self

    def get(self, inode, block_number, default=None):
        self.startTimer()

        now = time()

        inode_data = self._inodes.get(inode, {})

        block_data = inode_data.get(block_number)
        if block_data is None:
            block_data = CacheItem(0, default)

        val = block_data.c_block

        t = block_data.c_time
        if block_data.c_written:
            if now - t > self._max_write_ttl:
                return val
        else:
            if now - t > self._max_read_ttl:
                return val

        # update last request time
        block_data.c_time = now

        self.stopTimer("get")
        return val

    def getCachedSize(self, writed=False):
        self.startTimer()
        size = 0
        for inode in self._inodes.keys():
            for block_data in self._inodes[inode].values():
                if block_data.c_written != writed:
                    continue

                size += len(block_data.c_block.getbuffer())
        self.stopTimer("getCachedSize")
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
        self.startTimer()
        canDel = True
        if inode in self._inodes:
            inode_data = self._inodes[inode]
            for bn in inode_data.keys():
                block_data = inode_data[bn]
                block_data.c_time = 0
                if block_data.c_written:
                    canDel = False
                if block_data.c_toflush:
                    canDel = False
            if canDel:
                del self._inodes[inode]
        self.stopTimer("forget")
        return canDel

    def expire(self, inode):
        """
        Expire inode data
        It will be removed from cache on expred() call
        
        @param inode: 
        @return: 
        """
        self.startTimer()
        if inode in self._inodes:
            inode_data = self._inodes[inode]
            for bn in inode_data.keys():
                inode_data[bn].c_time = 0
        self.stopTimer("expire")
        return

    def flush(self, inode):
        """
        Force inode blocks to flush on disk
        
        @param inode: 
        @return: 
        """
        self.startTimer()
        if inode in self._inodes:
            inode_data = self._inodes[inode]
            for bn in inode_data.keys():
                inode_data[bn].c_toflush = True
        self.stopTimer("flush")
        return

    def expired(self):
        self.startTimer()
        now = time()

        write_inodes = {}
        read_inodes = 0

        for inode in tuple(self._inodes.keys()):

            inode_data = self._inodes[inode]

            for bn in tuple(inode_data.keys()):
                block_data = inode_data[bn]

                # Get data to FLUSH (and if requested written blocks)
                if block_data.c_toflush:

                    if inode not in write_inodes:
                        write_inodes[inode] = {}

                    write_inodes[inode][bn] = copy.copy(block_data)

                    block_data.c_toflush = False

                t = block_data.c_time
                if now - t > self._max_write_ttl:
                    if block_data.c_written:
                        if inode not in write_inodes:
                            write_inodes[inode] = {}

                        write_inodes[inode][bn] = copy.copy(block_data)

                        self._cur_write_cache_size -= block_data.c_size
                    else:
                        read_inodes += 1

                        self._cur_read_cache_size -= block_data.c_size

                    del inode_data[bn]

            if not inode_data and inode in self._inodes:
                del self._inodes[inode]

        self.stopTimer("expired")
        return (read_inodes, write_inodes,)


    def expireByCount(self, writed=False):
        """
        Expired inodes data by in-memory bytes size of cache
        
        @param writed: 
        @return: dict or int 
        """
        self.startTimer()

        now = time()

        # 1. Fill heap

        heap = []

        inodesKeys = tuple(self._inodes.keys())

        for inode in inodesKeys:

            inode_data = self._inodes[inode]

            for bn in inode_data.keys():
                block_data = inode_data[bn]

                if block_data.c_written != writed:
                    continue

                t = block_data.c_time
                heapq.heappush(
                    heap, (
                        int((now - t)*10**6),
                        inode,
                        bn,
                        len(block_data.c_block.getbuffer())
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

        for inode in heap_inodes.keys():

            inode_data = self._inodes[inode]

            for bn in heap_inodes[inode]:

                block_data = inode_data[bn]

                if block_data.c_written != writed:
                    continue

                if writed:
                    if inode not in oversize_inodes:
                        oversize_inodes[inode] = {}

                    oversize_inodes[inode][bn] = copy.copy(block_data)

                    self._cur_write_cache_size -= block_data.c_size
                else:
                    oversize_inodes += 1

                    self._cur_read_cache_size -= block_data.c_size

                del inode_data[bn]

            if not inode_data and inode in self._inodes:
                del self._inodes[inode]

        self.stopTimer("expireByCount")
        return oversize_inodes


    def clear(self):
        """
        Return all not written and not flushed blocks
        which must be written and flushed
        
        @return: dict 
        """
        self.startTimer()

        old_inodes = {}

        for inode in self._inodes.keys():

            inode_data = self._inodes[inode]

            for bn in inode_data.keys():
                block_data = inode_data[bn]

                # Get data to FLUSH (and if requested written blocks)
                if block_data.c_toflush:

                    if inode not in old_inodes:
                        old_inodes[inode] = {}

                    old_inodes[inode][bn] = copy.copy(block_data)

                    block_data.c_toflush = False

                if not block_data.c_written:
                    continue

                if inode not in old_inodes:
                    old_inodes[inode] = {}

                old_inodes[inode][bn] = copy.copy(block_data)

        self._inodes = {}
        self.stopTimer("clear")
        return old_inodes
