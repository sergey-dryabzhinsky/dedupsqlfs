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

        new = False
        if inode not in self._inodes:
            self._inodes[ inode ] = {
                "w" : writed,
                "f" : writed,
            }
            new = True

        inode_data = self._inodes[inode]

        # If time not set to 0 (expired)
        if inode_data.get("time", 0):
            new = True

        if new:
            inode_data["time"] = time()
        inode_data["data"] = data

        if writed:
            inode_data["w"] = True
            inode_data["f"] = True

        return self

    def get(self, inode, default=None):

        now = time()

        inode = str(inode)

        inode_data = self._inodes.get(inode, {
            "time" : 0              # Don't create empty item with good time
        })

        val = inode_data.get("data", default)

        t = inode_data["time"]
        if now - t > self._max_ttl:
            return val

        # update last request time
        inode_data["time"] = now

        return val


    def expired(self, writed=False):
        now = time()

        if writed:
            old_inodes = {}
        else:
            old_inodes = 0

        for inode in set(self._inodes.keys()):

            inode_data = self._inodes[inode]

            if inode_data["w"] != writed:
                continue

            t = inode_data["time"]
            if now - t > self._max_ttl:
                if writed:
                    old_inodes[inode] = inode_data["data"].copy()
                else:
                    old_inodes += 1

                del self._inodes[inode]

        return old_inodes


    def toBeFlushed(self):
        """
        Copy list of inodes which need to be flushed to disk
        
        @return: 
        """
        flush_inodes = {}

        for inode in set(self._inodes.keys()):

            inode_data = self._inodes[inode]

            if not inode_data["f"]:
                continue

            flush_inodes[inode] = inode_data["data"].copy()

            inode_data["f"] = False

        return flush_inodes


    def flush(self, inode):
        """
        Do not remove but set flush flag
        """
        key = str(inode)
        if key in self._inodes:
            self._inodes[ key ]["f"] = True
        return self


    def expire(self, inode):
        """
        Do not remove but expire
        """
        key = str(inode)
        if key in self._inodes:
            self._inodes[ key ]["time"] = 0
        return self


    def clear(self):
        old_inodes = {}

        for inode in set(self._inodes.keys()):

            inode_data = self._inodes[inode]

            if not inode_data["w"]:
                continue

            old_inodes[inode] = inode_data["data"].copy()

        self._inodes = {}
        return old_inodes
