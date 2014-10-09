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

        tableTree = self.getTable("tree")
        tableInode = self.getTable("inode")
        tableLink = self.getTable("link")
        tableXattr = self.getTable("xattr")
        tableIndex = self.getTable("inode_hash_block")
        tableSubvol = self.getTable("subvolume")

        try:
            tableTree.selectSubvolume(None)

            root_node_from = node_from = self.getManager().get_tree_node_by_parent_inode_and_name(llfuse.ROOT_INODE, subvol_from)
            attr_from = self.getManager().getattr(root_node_from["inode_id"])

            self.getLogger().debug("-- use subvolume node: %r" % (root_node_from,))

            root_node_to = node_to = self.getManager().get_tree_node_by_parent_inode_and_name(llfuse.ROOT_INODE, subvol_to)
            attr_to = self.getManager().getattr(root_node_to["inode_id"])

            self.getLogger().debug("-- into subvolume node: %r" % (root_node_to,))

            count_to_do = tableTree.count_subvolume_nodes(root_node_from["subvol_id"])
            count_done = 0
            count_proc = 0
            if count_to_do:
                count_proc = "%6.2f" % (count_done * 100.0 / count_to_do,)

            self.print_msg("Progress:\n")
            self.print_msg("\r%s %%  " % count_proc)

            _inode_from = tableInode.get(attr_from.st_ino)
            del _inode_from["id"]

            tableInode.update_data(attr_to.st_ino, _inode_from)

            tableTree.selectSubvolume(None)

            nodes = []
            for name_from, attr_from, node_from_id in self.getManager().readdir(node_from["inode_id"], 0):
                if attr_from.st_ino == node_from["inode_id"]:
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

                tableInode.selectSubvolume(root_node_to["subvol_id"])
                inode_to_id = tableInode.insert(
                    attr_from.st_nlink, attr_from.st_mode,
                    attr_from.st_uid, attr_from.st_gid, attr_from.st_rdev, attr_from.st_size,
                    at, mt, ct,
                    at_ns, mt_ns, ct_ns
                )
                tableInode.selectSubvolume(None)

                treeItem_from = tableTree.get(node_from_id)

                self.getLogger().debug("---- node from: %r" % (node_from_id,))
                self.getLogger().debug("---- tree item: %r" % (treeItem_from,))

                tableTree.selectSubvolume(root_node_to["subvol_id"])
                treeNode_to_id = tableTree.insert(
                    parent_node_to_id,
                    treeItem_from["name_id"],
                    inode_to_id
                )
                tableTree.selectSubvolume(None)

                linkTarget_from = tableLink.find_by_inode(attr_from.st_ino)
                if linkTarget_from:
                    tableLink.insert(inode_to_id, linkTarget_from)

                xattr_from = tableXattr.find_by_inode(attr_from.st_ino)
                if xattr_from:
                    tableXattr.insert(inode_to_id, xattr_from)

                count_indexes_from = tableIndex.get_count_by_inode(attr_from.st_ino)
                if count_indexes_from:
                    dp = 1.0 / count_indexes_from
                    check_count = 0
                    for indexItem in tableIndex.get_by_inode(attr_from.st_ino):

                        tableIndex.selectSubvolume(root_node_to["subvol_id"])
                        tableIndex.insert(
                            inode_to_id,
                            indexItem["block_number"],
                            indexItem["hash_id"]
                        )
                        tableIndex.selectSubvolume(None)

                        check_count += 1
                        count_done += dp
                        if count_to_do:
                            proc = "%6.4f" % (count_done * 100.0 / count_to_do,)
                            if proc != count_proc:
                                count_proc = proc
                                if self.getManager().flushCaches():
                                    self.getManager().getManager().commit()
                                self.print_msg("\r%s %%" % count_proc)
                    if check_count != count_indexes_from:
                        raise OSError("Count inode data blocks don't match to written count! inode=%s, count_db=%s, count_count=%s" % (attr_from.st_ino, count_indexes_from, check_count,))
                else:
                    count_done += 1

                if stat.S_ISDIR(attr_from.st_mode):
                    for name_from, new_attr_from, node_from_id in self.getManager().readdir(attr_from.st_ino, 0):
                        if attr_from.st_ino == new_attr_from.st_ino:
                            continue
                        nodes.append((node_from_id, new_attr_from, name_from, treeNode_to_id,))

                if count_to_do:
                    proc = "%6.2f" % (count_done * 100.0 / count_to_do,)
                    if proc != count_proc:
                        count_proc = proc
                        if self.getManager().flushCaches():
                            self.getManager().getManager().commit()
                        self.print_msg("\r%s %%    " % count_proc)

            self.print_msg("\n")
            if not self.getManager().flushCaches():
                self.getManager().getManager().commit()

            # Copy attrs from subvolume table
            subvol_from = tableSubvol.get(root_node_from["subvol_id"])
            tableSubvol.mount_time(root_node_to["subvol_id"], subvol_from["mounted_at"])
            tableSubvol.update_time(root_node_to["subvol_id"], subvol_from["updated_at"])

            # All snapshots readonly by default
            tableSubvol.readonly(root_node_to["subvol_id"], True)

            self.print_msg("Done\n")

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
                self.print_msg("Remove %r subvolume" % name)
                self.remove(name)

        self.getManager().releasedir(fh)

        return

    pass
