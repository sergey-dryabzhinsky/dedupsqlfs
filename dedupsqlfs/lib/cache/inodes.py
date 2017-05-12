# -*- coding: utf8 -*-
"""
@author Sergey Dryabzhinsky
"""

from time import time

class InodesTime(object):
    """
    Cache storage for inode raw attributes

    {
        inode (int) : [
            timestamp (float),      - then added, updated, set to 0 if expired
            data (dict),            - inode raw ROW data from SQLdb
            written (bool)          - data was written, updated
            toflush (bool)          - data must be flushed as soos as possible, but not expired
        ], ...
    }

    Just to not lookup in SQLdb

    """

    OFFSET_TIME = 0
    OFFSET_DATA = 1
    OFFSET_WRITTEN = 2
    OFFSET_TOFLUSH = 3

    # Maximum seconds after that cache is expired for writed inodes
    _max_ttl = 5

    _inodes = None

    def __init__(self):
        self._inodes = {}
        pass

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

        new = False
        if inode not in self._inodes:
            self._inodes[ inode ] = [
                0, data, writed, writed
            ]
            new = True

        inode_data = self._inodes[inode]

        # If time not set to 0 (expired)
        if inode_data[self.OFFSET_TIME]:
            new = True

        if new:
            inode_data[self.OFFSET_TIME] = time()
        inode_data[self.OFFSET_DATA] = data

        if writed:
            inode_data[self.OFFSET_WRITTEN] = True
            inode_data[self.OFFSET_TOFLUSH] = True

        return self

    def get(self, inode, default=None):

        now = time()

        inode_data = self._inodes.get(inode, [
                0, default, False, False
            ])

        val = inode_data[self.OFFSET_DATA]

        t = inode_data[self.OFFSET_TIME]
        if now - t > self._max_ttl:
            return val

        # update last request time
        inode_data[self.OFFSET_TIME] = now

        return val


    def expired(self, writed=False):
        now = time()

        if writed:
            old_inodes = {}
        else:
            old_inodes = 0

        for inode in tuple(self._inodes.keys()):

            inode_data = self._inodes[inode]

            # Get data to FLUSH (and if requested written attrs)
            if inode_data[self.OFFSET_TOFLUSH] and writed:
                old_inodes[inode] = inode_data[self.OFFSET_DATA].copy()
                inode_data[self.OFFSET_TOFLUSH] = False

            if inode_data[self.OFFSET_WRITTEN] != writed:
                continue

            t = inode_data[self.OFFSET_TIME]
            if now - t > self._max_ttl:
                if writed:
                    old_inodes[inode] = inode_data[self.OFFSET_DATA].copy()
                else:
                    old_inodes += 1

                del self._inodes[inode]

        return old_inodes


    def flush(self, inode):
        """
        Do not remove but set flush flag
        """
        if inode in self._inodes:
            self._inodes[ inode ][self.OFFSET_TOFLUSH] = True
        return self


    def expire(self, inode):
        """
        Do not remove but expire
        """
        if inode in self._inodes:
            self._inodes[ inode ][self.OFFSET_TIME] = 0
        return self


    def clear(self):
        old_inodes = {}

        for inode in tuple(self._inodes.keys()):

            inode_data = self._inodes[inode]

            if not inode_data[self.OFFSET_WRITTEN]:
                continue

            old_inodes[inode] = inode_data[self.OFFSET_DATA].copy()

        self._inodes = {}
        return old_inodes
