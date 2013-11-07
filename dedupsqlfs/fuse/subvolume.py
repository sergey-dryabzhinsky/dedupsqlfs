# -*- coding: utf8 -*-

__author__ = 'sergey'

import os
import stat
import sys
import llfuse
import errno
from datetime import datetime
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

    def print_msg(self, msg):
        if self.getManager().getOption("verbosity") <= 0:
            return self
        sys.stdout.write(msg)
        sys.stdout.flush()
        return self

    # -----------------------------------------------

    def create(self, name):
        """
        @param name: Subvolume name
        @type  name: bytes

        @return: tree node ID
        @rtype: bool
        """

        if not name:
            self.getLogger().error("Define subvolume name which you need to create!")
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
        self.getLogger().debug("Created tree node: %r" % (node,))

        self.getTable('subvolume').insert(node['id'], int(attrs.st_ctime))

        return True

    def list(self):
        """
        List all subvolumes
        """

        fh = self.getManager().opendir(llfuse.ROOT_INODE)

        print("Subvolumes:")
        print("-"*(46+22+22+22+1))
        print("%-46s| %-20s| %-20s| %-20s|" % ("Name", "Created", "Last mounted", "Last updated"))
        print("-"*(46+22+22+22+1))

        for name, attr, node in self.getManager().readdir(fh, 0):

            self.getLogger().debug("subvolume.list(): name=%r, attr=%r, node=%r" % (name, attr, node,))

            subvol = self.getTable('subvolume').get(node)

            ctime = "---"
            if subvol["created_at"]:
                ctime = datetime.fromtimestamp(subvol["created_at"])

            mtime = "not mounted"
            if subvol["mounted_at"]:
                mtime = datetime.fromtimestamp(subvol["mounted_at"])

            utime = "not updated"
            if subvol["updated_at"]:
                utime = datetime.fromtimestamp(subvol["updated_at"])

            print("%-46s| %-20s| %-20s| %-20s|" % (
                name.decode("utf8"),
                ctime,
                mtime,
                utime,
            ))

        print("-"*(46+22+22+22+1))

        self.getManager().releasedir(fh)

        return


    def remove(self, name):
        """
        @param name: Subvolume name
        @type  name: bytes
        """

        if not name:
            self.getLogger().error("Select subvolume which you need to delete!")
            return

        subvol_name = name
        if not subvol_name.startswith(b'@'):
            subvol_name = b'@' + subvol_name

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
            self.getLogger().error("Select subvolume which you need to process!")
            return

        subvol_name = name
        if not subvol_name.startswith(b'@'):
            subvol_name = b'@' + subvol_name

        try:
            attr = self.getManager().lookup(llfuse.ROOT_INODE, subvol_name)
            node = self.getTable('tree').find_by_inode(attr.st_ino)

            curTree = self.getTable("tree").getCursor()
            curInode = self.getTable("inode").getCursor()

            count_to_do = self.getTable('tree').count_subvolume_inodes(node["id"])
            count_done = 0
            count_proc = 0
            if count_to_do:
                count_proc = "%6.2f" % (count_done * 100.0 / count_to_do,)

            self.getLogger().info("Progress:")
            self.print_msg("\r%s %%" % count_proc)

            apparent_size = 0
            compressed_size = 0
            unique_size = 0

            compMethods = {}

            curTree.execute("SELECT inode_id FROM tree WHERE subvol_id=?", (node['id'],))

            while True:
                treeItem = curTree.fetchone()
                if not treeItem:
                    break

                curInode.execute("SELECT `size` FROM `inode` WHERE id=?", (treeItem["inode_id"],))
                apparent_size += curInode.fetchone()["size"]

                hashes = self.getTable('inode_hash_block').get_hashes_by_inode(treeItem["inode_id"])
                for indexItem in hashes:
                    cnt = self.getTable('inode_hash_block').get_count_hash(indexItem["hash_id"])

                    blockItem = self.getTable("block").get(indexItem["hash_id"])
                    method = self.getManager().getCompressionTypeName(blockItem["compression_type_id"])
                    compMethods[ method ] = compMethods.get(method, 0) + 1

                    if cnt == 1:
                        unique_size += indexItem['block_size']
                        compressed_size += len(blockItem["data"])

                count_done += 1

                if count_to_do:
                    proc = "%6.2f" % (count_done * 100.0 / count_to_do,)
                    if proc != count_proc:
                        count_proc = proc
                        self.print_msg("\r%s %%" % count_proc)

            self.print_msg("\n")

            self.getLogger().info("Apparent size is %s.",
                             format_size(apparent_size)
            )

            self.getLogger().info("Unique data size is %s.",
                             format_size(unique_size)
            )

            if unique_size:
                self.getLogger().info("Compressed data size is %s (%.2f %%).",
                    format_size(compressed_size), compressed_size * 100.0 / unique_size
                )

            self.getLogger().info("Compression by types:")
            count_all = 0
            comp_types = {}

            for method, cnt in compMethods.items():
                count_all += cnt
                comp_types[ cnt ] = method

            keys = list(comp_types.keys())
            keys.sort(reverse=True)

            for key in keys:
                compression = comp_types[key]
                self.getLogger().info(" %8s used by %.2f%% blocks",
                    compression, 100.0 * key / count_all
                )

        except Exception as e:
            self.getLogger().warn("Can't process subvolume! %s" % e)
            import traceback
            self.getLogger().error(traceback.format_exc())

        return


    pass