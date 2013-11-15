# -*- coding: utf8 -*-

from time import time

__author__ = 'sergey'

class InodesTime(object):

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

        inode = str(inode)

        if inode not in self._inodes:
            self._inodes[ inode ] = {}

        inode_data = self._inodes[inode]

        inode_data["time"] = time()
        inode_data["data"] = data

        if writed:
            inode_data["w"] = True

        return self

    def get(self, inode, default=None):

        inode = str(inode)

        inode_data = self._inodes.get(inode, {})

        if not inode_data:
            return default

        now = time()
        t = inode_data["time"]
        if now - t > self._max_ttl:
            return default

        val = inode_data.get("data", default)

        # update last request time
        inode_data["time"] = time()

        return val

    def expired(self, writed=False):
        now = time()

        if writed:
            old_inodes = {}
        else:
            old_inodes = 0

        for inode in tuple(self._inodes.keys()):

            inode_data = self._inodes[inode]

            t = inode_data["time"]
            if inode_data["w"] != writed:
                continue

            if now - t > self._max_ttl:
                if writed:
                    old_inodes[inode] = inode_data
                else:
                    old_inodes += 1

                del self._inodes[inode]

        return old_inodes


    def unset(self, key):
        """
        Do not remove but expire
        """
        if key in self._inodes:
            self._inodes[ key ]["time"] = 0
        return self


    def clear(self):
        old_inodes = self._inodes.copy()
        self._inodes = {}
        return old_inodes
