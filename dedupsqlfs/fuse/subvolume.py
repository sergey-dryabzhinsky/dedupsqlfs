# -*- coding: utf8 -*-

__author__ = 'sergey'

import os
import stat
import sys
import math
import llfuse
import errno
from time import time
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

    def print_out(self, msg):
        sys.stdout.write(msg)
        sys.stdout.flush()
        return self

    def get_time_tuple(self, t):
        t_ns, t_i = math.modf(t)
        t_ns = int(t_ns * 10**9)
        return int(t_i), t_ns

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

        subvol_id = self.getTable('subvolume').insert(int(time()))
        self.getTable('tree').selectSubvolume(subvol_id)
        self.getTable('inode').selectSubvolume(subvol_id)
        self.getTable('inode_hash_block').selectSubvolume(subvol_id)

        ctx = llfuse.RequestContext()
        ctx.uid = os.getuid()
        ctx.gid = os.getgid()
        attrs = self.getManager().mkdir(llfuse.ROOT_INODE, subvol_name, self.root_mode, ctx)

        node = self.getTable('tree').find_by_inode(attrs.st_ino)
        self.getLogger().debug("Created tree node: %r" % (node,))

        return True

    def list(self):
        """
        List all subvolumes
        """

        fh = self.getManager().opendir(llfuse.ROOT_INODE)

        tableInode = self.getTable('inode')
        tableTree = self.getTable('tree')
        tableIndex = self.getTable('inode_hash_block')

        self.print_out("Subvolumes:\n")
        self.print_out("-"*(46+12+14+14+22+22+22+1) + "\n")
        self.print_out("%-46s| %-10s| %-12s| %-12s| %-20s| %-20s| %-20s|\n" % (
            "Name", "ReadOnly", "Data Size", "Real Size", "Created", "Last mounted", "Last updated"))
        self.print_out("-"*(46+12+14+14+22+22+22+1) + "\n")

        for name, attr, node in self.getManager().readdir(fh, 0):

            rootNode = tableTree.find_by_inode(attr.st_ino)

            self.getLogger().debug("subvolume.list(): name=%r, attr=%r, node=%r" % (name, attr, node,))

            subvol = self.getTable('subvolume').get(rootNode['subvol_id'])

            ctime = "---"
            if subvol["created_at"]:
                ctime = datetime.fromtimestamp(subvol["created_at"])

            mtime = "not mounted"
            if subvol["mounted_at"]:
                mtime = datetime.fromtimestamp(subvol["mounted_at"])

            utime = "not updated"
            if subvol["updated_at"]:
                utime = datetime.fromtimestamp(subvol["updated_at"])

            readonly = False
            if subvol["readonly"]:
                readonly = True

            subvolDataSizes = tableIndex.get_sum_sizes_by_subvol(rootNode['subvol_id'])
            apparent_size = tableInode.get_subvolume_size(rootNode['subvol_id'])
            unique_size = subvolDataSizes["writed"]

            self.print_out(u"{0:<46s}| {1:<10s}| {1:<12s}| {1:<12s}| {2:<20s}| {3:<20s}| {4:<20s}|\n".format(
                name.decode("utf8"),
                readonly,
                format_size(apparent_size),
                format_size(unique_size),
                ctime,
                mtime,
                utime,
                ))

        self.print_out("-"*(46+12+14+14+22+22+22+1) + "\n")

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
        except Exception:
            self.getLogger().warn("Can't remove subvolume! Not found!")
            return

        try:
            node = self.getTable('tree').find_by_inode(attr.st_ino)
            self.getTable('tree').delete_subvolume(node["subvol_id"])
            self.getTable('subvolume').delete(node['subvol_id'])
        except Exception as e:
            self.getLogger().warn("Can't remove subvolume!")
            self.getLogger().error("E: %s" % e)
            import traceback
            self.getLogger().error(traceback.format_exc())
            return

        return

    def readonly(self, name, flag=True):
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
        except Exception:
            self.getLogger().warn("Can't remove subvolume! Not found!")
            return

        try:
            node = self.getTable('tree').find_by_inode(attr.st_ino)
            self.getTable('subvolume').readonly(node['subvol_id'], flag)
        except Exception as e:
            self.getLogger().warn("Can't set subvolume readonly flag!")
            self.getLogger().error("E: %s" % e)
            import traceback
            self.getLogger().error(traceback.format_exc())
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
            tableInode = self.getTable('inode')
            tableTree = self.getTable('tree')

            attr = self.getManager().lookup(llfuse.ROOT_INODE, subvol_name)
            rootNode = tableTree.find_by_inode(attr.st_ino)

            compMethods = {}
            hashCT = {}

            tableIndex = self.getTable('inode_hash_block')
            tableHCT = self.getTable('hash_compression_type')

            hashes = tableIndex.get_hashes_by_subvol(rootNode["subvol_id"])

            for hash_id in hashes:
                if hash_id in hashCT:
                    method = hashCT[hash_id]
                else:
                    hctItem = tableHCT.get(hash_id)
                    method = self.getManager().getCompressionTypeName(hctItem["type_id"])
                    hashCT[hash_id] = method

                compMethods[ method ] = compMethods.get(method, 0) + 1

            subvolDataSizes = tableIndex.get_sum_sizes_by_subvol(rootNode['subvol_id'])

            apparent_size = tableInode.get_subvolume_size(rootNode['subvol_id'])
            sparce_size = apparent_size - subvolDataSizes["real"]
            dedup_size = subvolDataSizes["real"] - subvolDataSizes["writed"]
            unique_size = subvolDataSizes["writed"]
            compressed_size = subvolDataSizes["real_comp"]
            compressed_uniq_size = subvolDataSizes["writed_comp"]

            self.print_msg("\n")

            self.print_out("Apparent size is %s.\n" % format_size(apparent_size) )
            self.print_out("Unique data size is %s.\n" % format_size(unique_size) )
            self.print_out("Sparce data size is %s.\n" % format_size(sparce_size) )
            self.print_out("Deduped data size is %s.\n" % format_size(dedup_size) )

            if apparent_size:
                self.print_out("Compressed data size is %s (%.2f %%).\n" % (
                    format_size(compressed_size), compressed_size * 100.0 / apparent_size
                ))
            if unique_size:
                self.print_out("Compressed unique data size is %s (%.2f %%).\n" % (
                    format_size(compressed_uniq_size), compressed_uniq_size * 100.0 / unique_size
                ))

            count_all = 0
            comp_types = {}

            for method, cnt in compMethods.items():
                count_all += cnt
                comp_types[ cnt ] = method

            keys = list(comp_types.keys())
            keys.sort(reverse=True)

            if keys:
                self.print_out("Compression by types:\n")
            for key in keys:
                compression = comp_types[key]
                self.print_out(" %8s used by %.2f%% blocks\n" % (
                    compression, 100.0 * key / count_all
                ))

        except Exception as e:
            self.getLogger().warn("Can't process subvolume! %s" % e)
            import traceback
            self.getLogger().error(traceback.format_exc())

        return


    pass