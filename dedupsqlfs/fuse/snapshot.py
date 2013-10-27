# -*- coding: utf8 -*-

__author__ = 'sergey'

import stat
import llfuse
from datetime import datetime
from dedupsqlfs.fuse.subvolume import Subvolume

class Snapshot(Subvolume):

    def make(self, from_subvol, with_name):
        """
        Copy all tree,inode,index,link data from one subvolume to new
        """

        if not from_subvol:
            self.getManager().getLogger().error("Select subvolume from which you need to create snapshot!")
            return

        if not with_name:
            self.getManager().getLogger().error("Define name for subvolume to which you need to copy %r data!" % from_subvol)
            return

        if not self.create(with_name):
            self.getManager().getLogger().error("Subvolume with name %r already exists! Can't snapshot into it!", with_name)
            return

        subvol_from = from_subvol
        if not from_subvol.startswith(b'@'):
            subvol_from = b'@' + from_subvol

        subvol_to = with_name
        if not with_name.startswith(b'@'):
            subvol_to = b'@' + with_name

        try:
            attr_from = self.getManager().lookup(llfuse.ROOT_INODE, subvol_from)
            root_node_from = node_from = self.getTable('tree').find_by_inode(attr_from.st_ino)

            attr_to = self.getManager().lookup(llfuse.ROOT_INODE, subvol_to)
            root_node_to = node_to = self.getTable('tree').find_by_inode(attr_to.st_ino)

            nodes = []
            for name, attr, node in self.getManager().readdir(node_from["inode_id"], 0):
                nodes.append((node, attr, name, node_to["id"]))

            while len(nodes)>0:

                node_from, attr_from, name_from, parent_to_id = nodes.pop()

                inode_from = self.getTable("inode").get(attr_from.st_ino)

                inode_to = self.getTable("inode").insert(
                    inode_from["nlinks"], inode_from["mode"],
                    inode_from["uid"], inode_from["gid"], inode_from["rdev"], inode_from["size"],
                    inode_from["atime"], inode_from["mtime"], inode_from["ctime"],
                    inode_from["atime_ns"], inode_from["mtime_ns"], inode_from["ctime_ns"]
                )

                treeItem_from = self.getTable("tree").get(node_from)

                self.getTable("tree").selectSubvolume(root_node_to["id"])
                treeNode_to = self.getTable("tree").insert(
                    parent_to_id,
                    treeItem_from["name_id"],
                    inode_to
                )
                self.getTable("tree").selectSubvolume(None)

                linkTarget_from = self.getTable("link").find_by_inode(inode_from["id"])
                if linkTarget_from:
                    self.getTable("link").insert(inode_to, linkTarget_from)

                xattr_from = self.getTable("xattr").find_by_inode(inode_from["id"])
                if xattr_from:
                    self.getTable("xattr").insert(inode_to, xattr_from["data"])

                indexes_from = self.getTable("inode_hash_block").get_by_inode(inode_from["id"])
                if len(indexes_from):
                    for indexItem in indexes_from:
                        self.getTable("inode_hash_block").insert(
                            inode_to,
                            indexItem["block_number"],
                            indexItem["hash_id"],
                            indexItem["block_size"]
                        )

                if stat.S_ISDIR(attr_from.st_mode):
                    for name, attr, node in self.getManager().readdir(inode_from["id"], 0):
                        nodes.append((node, attr, name, treeNode_to))

        except:
            self.getManager().getLogger().warn("Error while copying data!")
            import traceback
            self.getManager().getLogger().error(traceback.format_exc())

        return

    def remove_older_than(self, dateStr):

        oldDate = datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S")

        fh = self.getManager().opendir(llfuse.ROOT_INODE)

        for name, attr, node in self.getManager().readdir(fh, 0):

            subvol = self.getTable('subvolume').get(node)

            subvolDate = datetime.fromtimestamp(subvol["created_at"])
            if subvolDate < oldDate:
                self.getManager().getLogger().info("Remove %r subvolume", name)
                self.remove(name)

        self.getManager().releasedir(fh)

        return

    pass
