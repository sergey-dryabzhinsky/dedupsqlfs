# -*- coding: utf8 -*-

__author__ = 'sergey'

import os
import stat
import time
import math
import llfuse
import errno
from datetime import datetime
from dedupsqlfs.lib import constants

class Subvolume(object):

    _manager = None
    _last_error = None

    def __init__(self, manager):
        """
        @param manager: FUSE wrapper
        @type  manager: dedupsqlfs.fuse.operations.DedupOperations
        """
        self._manager = manager

        self.root_mode = stat.S_IFDIR | 0o755

        pass

    def getManager(self):
        return self._manager

    def getTable(self, name):
        return self.getManager().getTable(name)

    def getLastError(self):
        return self._last_error

    def create(self, name):
        """
        @param name: Subvolume name
        @type  name: bytes

        @return: tree node ID
        @rtype: int|bool
        """
        snap_name = b'@' + name

        try:
            attr = self.getManager().lookup(llfuse.ROOT_INODE, snap_name)
        except llfuse.FUSEError as e:
            if not e.errno == errno.ENOENT:
                raise
            ctx = llfuse.RequestContext()
            ctx.uid = os.getuid()
            ctx.gid = os.getgid()
            attr = self.getManager().mkdir(llfuse.ROOT_INODE, snap_name, self.root_mode, ctx)

        return attr
    # -----------------------------------------------

    def list(self):
        """
        List all subvolumes
        """

        fh = self.getManager().opendir(llfuse.ROOT_INODE)

        print("Subvolumes:")
        print("-"*79)
        print("%-50s\t%-29s" % ("Name", "Created"))

        for name, attr, node in self.getManager().readdir(fh, 0):
            print("%-50s\t%-29s" % (name.decode("utf8"), datetime.fromtimestamp(attr.st_ctime)))

        self.getManager().releasedir(fh)

        print("-"*79)

        return


    pass