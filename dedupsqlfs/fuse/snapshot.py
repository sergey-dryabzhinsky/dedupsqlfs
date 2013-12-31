# -*- coding: utf8 -*-

__author__ = 'sergey'

import stat
import llfuse
from datetime import datetime
from dedupsqlfs.fuse.subvolume import Subvolume
from dedupsqlfs.lib import constants

class Snapshot(Subvolume):

    def make(self, from_subvol, with_name):
        """
        Copy all tree,inode,index,link data from one subvolume to new
        """

        if not from_subvol:
            self.getLogger().error("Select subvolume from which you need to create snapshot!")
            return

        if not with_name:
            self.getLogger().error("Define name for subvolume to which you need to copy %r data!" % from_subvol)
            return

        if not self.create(with_name):
            self.getLogger().error("Subvolume with name %r already exists! Can't snapshot into it!", with_name)
            return

        subvol_from = from_subvol
        if not from_subvol.startswith(b'@'):
            subvol_from = b'@' + from_subvol

        subvol_to = with_name
        if not with_name.startswith(b'@'):
            subvol_to = b'@' + with_name

        self.getLogger().debug("Use subvolume: %r" % subvol_from)
        self.getLogger().debug("Into subvolume: %r" % subvol_to)

        try:
            self.getTable("tree").selectSubvolume(None)

            attr_from = self.getManager().lookup(llfuse.ROOT_INODE, subvol_from)
            root_node_from = node_from = self.getTable('tree').find_by_inode(attr_from.st_ino)

            self.getLogger().debug("-- use subvolume node: %r" % (root_node_from,))

            attr_to = self.getManager().lookup(llfuse.ROOT_INODE, subvol_to)
            root_node_to = node_to = self.getTable('tree').find_by_inode(attr_to.st_ino)

            self.getLogger().debug("-- into subvolume node: %r" % (root_node_to,))

            count_to_do = self.getTable('tree').count_subvolume_nodes(root_node_from["id"])
            count_done = 0
            count_proc = 0
            if count_to_do:
                count_proc = "%6.2f" % (count_done * 100.0 / count_to_do,)

            self.getLogger().info("Progress:")
            self.print_msg("\r%s %%" % count_proc)

            _inode_from = self.getTable("inode").get(attr_from.st_ino)
            del _inode_from["id"]

            self.getTable("inode").update_data(attr_to.st_ino, _inode_from)

            self.getTable("tree").selectSubvolume(root_node_from["id"])

            nodes = []
            for name_from, attr_from, node_from_id in self.getManager().readdir(node_from["inode_id"], 0):
                if attr_from.st_ino == node_from["inode_id"]:
                    continue
                if name_from in (b'.', b'..'):
                    continue
                nodes.append((node_from_id, attr_from, name_from, node_to["id"]))

            self.getLogger().debug("Start copying subvol:")
            while len(nodes)>0:

                node_from_id, attr_from, name_from, parent_node_to_id = nodes.pop()

                v = {}
                for a in attr_from.__slots__:
                    v[a] = getattr(attr_from, a)

                self.getLogger().debug("-- name_from: %r, attr_from: %r, node_from: %s; to parent node: %s" % (
                    name_from, v, node_from_id, parent_node_to_id))

                at, at_ns = self.get_time_tuple(attr_from.st_atime)
                mt, mt_ns = self.get_time_tuple(attr_from.st_mtime)
                ct, ct_ns = self.get_time_tuple(attr_from.st_ctime)
                inode_to_id = self.getTable("inode").insert(
                    attr_from.st_nlink, attr_from.st_mode,
                    attr_from.st_uid, attr_from.st_gid, attr_from.st_rdev, attr_from.st_size,
                    at, mt, ct,
                    at_ns, mt_ns, ct_ns
                )

                treeItem_from = self.getTable("tree").get(node_from_id)

                self.getLogger().debug("---- node from: %r" % (node_from_id,))
                self.getLogger().debug("---- tree item: %r" % (treeItem_from,))

                self.getTable("tree").selectSubvolume(root_node_to["id"])
                treeNode_to_id = self.getTable("tree").insert(
                    parent_node_to_id,
                    treeItem_from["name_id"],
                    inode_to_id
                )
                self.getTable("tree").selectSubvolume(root_node_from["id"])

                linkTarget_from = self.getTable("link").find_by_inode(attr_from.st_ino)
                if linkTarget_from:
                    self.getTable("link").insert(inode_to_id, linkTarget_from)

                xattr_from = self.getTable("xattr").find_by_inode(attr_from.st_ino)
                if xattr_from:
                    self.getTable("xattr").insert(inode_to_id, xattr_from)

                count_indexes_from = self.getTable("inode_hash_block").get_count_by_inode(attr_from.st_ino)
                if count_indexes_from:
                    dp = 1.0 / count_indexes_from
                    for indexItem in self.getTable("inode_hash_block").get_by_inode(attr_from.st_ino):
                        self.getTable("inode_hash_block").insert(
                            inode_to_id,
                            indexItem["block_number"],
                            indexItem["hash_id"]
                        )
                        count_done += dp
                        if count_to_do:
                            proc = "%6.4f" % (count_done * 100.0 / count_to_do,)
                            if proc != count_proc:
                                count_proc = proc
                                if self.getManager().flushCaches():
                                    self.getManager().getManager().commit()
                                self.print_msg("\r%s %%" % count_proc)
                else:
                    count_done += 1

                if stat.S_ISDIR(attr_from.st_mode):
                    for name_from, attr_from, node_from_id in self.getManager().readdir(attr_from.st_ino, 0):
                        if attr_from.st_ino == attr_from.st_ino:
                            continue
                        if name_from in (b'.', b'..'):
                            continue
                        nodes.append((node_from, attr_from, name_from, treeNode_to_id,))

                if count_to_do:
                    proc = "%6.2f" % (count_done * 100.0 / count_to_do,)
                    if proc != count_proc:
                        count_proc = proc
                        if self.getManager().flushCaches():
                            self.getManager().getManager().commit()
                        self.print_msg("\r%s %%" % count_proc)

            self.print_msg("\n")
            if not self.getManager().flushCaches():
                self.getManager().getManager().commit()

            # Copy attrs from subvolume table
            subvol_from = self.getTable("subvolume").get(root_node_from["id"])
            self.getTable("subvolume").mount_time(root_node_to["id"], subvol_from["mounted_at"])
            self.getTable("subvolume").update_time(root_node_to["id"], subvol_from["updated_at"])

            self.getManager().getLogger().info("Done")

        except:
            self.print_msg("\n")
            self.getManager().getLogger().warn("Error while copying data!")
            import traceback
            self.getManager().getLogger().error(traceback.format_exc())

        return

    def remove_older_than(self, dateStr, use_last_update_time=False):

        oldDate = datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S")

        fh = self.getManager().opendir(llfuse.ROOT_INODE)

        for name, attr, node in self.getManager().readdir(fh, 0):

            if name == constants.ROOT_SUBVOLUME_NAME:
                continue

            subvol = self.getTable('subvolume').get(node)

            if not use_last_update_time:
                subvolDate = datetime.fromtimestamp(subvol["created_at"])
            else:
                subvolDate = datetime.fromtimestamp(subvol["updated_at"])

            if subvolDate < oldDate:
                self.getManager().getLogger().info("Remove %r subvolume", name)
                self.remove(name)

        self.getManager().releasedir(fh)

        return

    pass
