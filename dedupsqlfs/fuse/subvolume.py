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

    def getLogger(self, name):
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
            attrs = self.getManager().mkdir(llfuse.ROOT_INODE, subvol_name, self.root_mode, ctx)

            node = self.getTable('tree').find_by_inode(attrs.st_ino)
            self.getTable('subvolume').insert(node['id'], int(attrs.st_ctime))

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

            subvol = self.getTable('subvolume').get(node)

            print("%-50s| %-29s" % (name.decode("utf8"), datetime.fromtimestamp(subvol["created_at"])))

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

        try:
            attr = self.getManager().lookup(llfuse.ROOT_INODE, subvol_name)
            node = self.getTable('tree').find_by_inode(attr.st_ino)
            self.getTable('tree').delete_subvolume(node["id"])

            self.getTable('subvolume').delete(node['id'])
        except:
            self.getManager().getLogger().warn("Can't remove subvolume! Not found!")
            return

        return


    pass