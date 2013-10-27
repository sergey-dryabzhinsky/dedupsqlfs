# -*- coding: utf8 -*-

__author__ = 'sergey'

import os
import stat
import llfuse
import errno
from datetime import datetime
from dedupsqlfs.lib import constants
from dedupsqlfs.my_formats import format_size

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

    def getLogger(self):
        return self.getManager().getLogger()

    def getLastError(self):
        return self._last_error

    # -----------------------------------------------

    def create(self, name):
        """
        @param name: Subvolume name
        @type  name: bytes

        @return: tree node ID
        @rtype: bool
        """

        if not name:
            self.getManager().getLogger().error("Define subvolume name which you need to create!")
            return False

        subvol_name = name
        if not subvol_name.startswith(b'@'):
            subvol_name = b'@' + subvol_name

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

    def list(self):
        """
        List all subvolumes
        """

        fh = self.getManager().opendir(llfuse.ROOT_INODE)

        print("Subvolumes:")
        print("-"*79)
        print("%-56s| %-20s|" % ("Name", "Created"))
        print("-"*79)

        for name, attr, node in self.getManager().readdir(fh, 0):

            subvol = self.getTable('subvolume').get(node)

            print("%-56s| %-20s|" % (name.decode("utf8"), datetime.fromtimestamp(subvol["created_at"])))

        self.getManager().releasedir(fh)

        print("-"*79)

        return


    def remove(self, name):
        """
        @param name: Subvolume name
        @type  name: bytes
        """

        if not name:
            self.getManager().getLogger().error("Select subvolume which you need to delete!")
            return


        subvol_name = name
        if not subvol_name.startswith(b'@'):
            subvol_name = b'@' + subvol_name

        if subvol_name == constants.ROOT_SUBVOLUME_NAME:
            self.getLogger().warn("Can't remove root subvolume!")
            return

        try:
            attr = self.getManager().lookup(llfuse.ROOT_INODE, subvol_name)
            node = self.getTable('tree').find_by_inode(attr.st_ino)
            self.getTable('tree').delete_subvolume(node["id"])

            self.getTable('subvolume').delete(node['id'])
        except:
            self.getLogger().warn("Can't remove subvolume! Not found!")
            return

        return

    def report_usage(self, name):
        """
        @param name: Subvolume name
        @type  name: bytes
        """

        if not name:
            self.getManager().getLogger().error("Select subvolume which you need to process!")
            return


        subvol_name = name
        if not subvol_name.startswith(b'@'):
            subvol_name = b'@' + subvol_name

        try:
            attr = self.getManager().lookup(llfuse.ROOT_INODE, subvol_name)
            node = self.getTable('tree').find_by_inode(attr.st_ino)

            curTree = self.getTable("tree").getCursor()
            curInode = self.getTable("inode").getCursor()

            curTree.execute("SELECT inode_id FROM tree WHERE subvol_id=?", (node['id'],))

            apparent_size = 0
            unique_size = 0
            while True:
                treeItem = curTree.fetchone()
                if not treeItem:
                    break

                curInode.execute("SELECT `size` FROM `inode` WHERE id=?", (treeItem["inode_id"],))
                apparent_size += curInode.fetchone()["size"]

                hashes = self.getTable('inode_hash_block').get_hashes_by_inode(treeItem["inode_id"])
                for indexItem in hashes:
                    cnt = self.getTable('inode_hash_block').get_count_hash(indexItem["hash_id"])
                    if cnt == 1:
                        unique_size += indexItem['block_size']

            self.getLogger().info("Apparent size is %s.",
                             format_size(apparent_size)
            )

            self.getLogger().info("Unique data size is %s.",
                             format_size(unique_size)
            )

        except Exception as e:
            self.getManager().getLogger().warn("Can't process subvolume! %s" % e)

        return


    pass