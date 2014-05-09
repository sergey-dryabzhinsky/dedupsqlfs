# -*- coding: utf8 -*-

__author__ = 'sergey'

import os
import stat
import sys
import math
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

        self.print_out("Subvolumes:\n")
        self.print_out("-"*(46+22+22+22+1) + "\n")
        self.print_out("%-46s| %-20s| %-20s| %-20s|\n" % ("Name", "Created", "Last mounted", "Last updated"))
        self.print_out("-"*(46+22+22+22+1) + "\n")

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

            self.print_out("%-46s| %-20s| %-20s| %-20s|\n" % (
                name.decode("utf8"),
                ctime,
                mtime,
                utime,
            ))

        self.print_out("-"*(46+22+22+22+1) + "\n")

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
            self.getTable('tree').delete_subvolume(node["id"])
            self.getTable('subvolume').delete(node['id'])
        except Exception as e:
            self.getLogger().warn("Can't remove subvolume!")
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
            node = tableTree.find_by_inode(attr.st_ino)

            count_to_do = tableTree.count_subvolume_inodes(node["id"])
            count_done = 0
            count_proc = 0
            if count_to_do:
                count_proc = "%6.2f" % (count_done * 100.0 / count_to_do,)

            self.print_msg("Progress:\n")
            self.print_msg("\r%s %%" % count_proc)

            apparent_size = 0
            sparce_size = 0
            compressed_size = 0
            unique_size = 0
            dedup_size = 0

            compMethods = {}

            hashCount = {}
            hashCT = {}
            hashBS = {}

            curTree = tableTree.getCursorForSelectNodeInodes(node['id'])

            tableOption = self.getTable('option')
            tableIndex = self.getTable('inode_hash_block')
            tableHCT = self.getTable('hash_compression_type')
            tableHBS = self.getTable('hash_block_size')

            blockSize = int(tableOption.get("block_size"))

            curIndex = tableIndex.getCursor()

            while True:
                treeItem = curTree.fetchone()
                if not treeItem:
                    break

                inode_size = tableInode.get_size_by_id_nlinks(treeItem["inode_id"])
                apparent_size += inode_size

                hashes = ()
                if inode_size:
                    hashes = set(( str(item["hash_id"]) for item in tableIndex.get_hashes_by_inode(treeItem["inode_id"])))

                stored_blocks = len(hashes)

                if stored_blocks:
                    inode_blocks = int(math.ceil(1.0 * inode_size / blockSize))
                    sparce_size += (inode_blocks - stored_blocks) * blockSize

                    curIndex.execute("SELECT COUNT(`hash_id`) AS `cnt`, `hash_id` FROM `inode_hash_block` WHERE `hash_id` IN ("+
                                    ",".join(hashes)
                                     +") GROUP BY `hash_id`")
                    while True:
                        indexItems = curIndex.fetchmany(1024)
                        if not indexItems:
                            break

                        hids = ()
                        cnts = {}
                        for item in indexItems:
                            if item["cnt"] > 1:
                                cnt = tableIndex.get_count_hash_by_inode(item["hash_id"], treeItem["inode_id"])
                                hids += (item["hash_id"],)
                                cnts[ item["hash_id"] ] = cnt

                        if hids:
                            rsizes = tableHBS.get_real_sizes(hids)
                            for item in rsizes:
                                dedup_size += cnts[ item["hash_id"] ] * item["real_size"]


                for hash_id in hashes:

                    if hash_id in hashCount:
                        cnt = hashCount[hash_id]
                    else:
                        cnt = tableIndex.get_count_hash_by_inode(hash_id, treeItem["inode_id"])
                        hashCount[hash_id] = cnt

                    if hash_id in hashCT:
                        method = hashCT[hash_id]
                    else:
                        hctItem = tableHCT.get(hash_id)
                        method = self.getManager().getCompressionTypeName(hctItem["type_id"])
                        hashCT[hash_id] = method

                    compMethods[ method ] = compMethods.get(method, 0) + 1

                    if cnt == 1:

                        if hash_id in hashBS:
                            hbsItem = hashBS[ hash_id ]
                        else:
                            hbsItem = tableHBS.get(hash_id)
                            hashBS[ hash_id ] = hbsItem

                        unique_size += hbsItem['real_size']
                        compressed_size += hbsItem['comp_size']

                count_done += 1

                if count_to_do:
                    proc = "%6.2f" % (count_done * 100.0 / count_to_do,)
                    if proc != count_proc:
                        count_proc = proc
                        self.print_msg("\r%s %%" % count_proc)

            self.print_msg("\n")

            self.print_out("Apparent size is %s.\n" % format_size(apparent_size) )
            self.print_out("Unique data size is %s.\n" % format_size(unique_size) )
            self.print_out("Sparce data size is %s.\n" % format_size(sparce_size) )
            self.print_out("Deduped data size is %s.\n" % format_size(dedup_size) )

            if unique_size:
                self.print_out("Compressed data size is %s (%.2f %%).\n" % (
                    format_size(compressed_size), compressed_size * 100.0 / unique_size
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