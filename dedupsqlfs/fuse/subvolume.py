# -*- coding: utf8 -*-

__author__ = 'sergey'

import os
import stat
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
        @rtype: bool
        """
        subvol_name = b'@' + name

        try:
            self.getManager().lookup(llfuse.ROOT_INODE, subvol_name)
            return False
        except llfuse.FUSEError as e:
            if not e.errno == errno.ENOENT:
                raise
            ctx = llfuse.RequestContext()
            ctx.uid = os.getuid()
            ctx.gid = os.getgid()
            self.getManager().mkdir(llfuse.ROOT_INODE, subvol_name, self.root_mode, ctx)

        return True
    # -----------------------------------------------

    def list(self):
        """
        List all subvolumes
        """

        fh = self.getManager().opendir(llfuse.ROOT_INODE)

        print("Subvolumes:")
        print("-"*79)
        print("%-50s| %-29s" % ("Name", "Created"))
        print("-"*79)

        for name, attr, node in self.getManager().readdir(fh, 0):
            print("%-50s| %-29s" % (name.decode("utf8"), datetime.fromtimestamp(attr.st_ctime)))

        self.getManager().releasedir(fh)

        print("-"*79)

        return


    def remove(self, name):
        """
        @param name: Subvolume name
        @type  name: bytes
        """
        if name == constants.ROOT_SUBVOLUME_NAME:
            self.getManager().getLogger().warn("Can't remove root subvolume!")
            return

        subvol_name = b'@' + name

        directories = [(llfuse.ROOT_INODE, subvol_name)]

        while len(directories)>0:

            parent_ino, directory = directories.pop()

            try:
                attr = self.getManager().lookup(parent_ino, directory)
            except:
                continue

            count = 0
            for rname, rattr, node in self.getManager().readdir(attr.st_ino, 0):
                if stat.S_ISDIR(rattr.st_mode):
                    directories.append((rattr.st_ino, rname,))
                else:
                    self.getManager().unlink(attr.st_ino, rname)
                count += 1

            if not count:
                self.getManager().unlink(parent_ino, directory)
            else:
                directories.insert(0, (parent_ino, directory,))

        return


    pass