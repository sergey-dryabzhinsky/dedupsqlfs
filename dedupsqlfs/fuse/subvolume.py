# -*- coding: utf8 -*-

__author__ = 'sergey'

import os
import stat
import sys
import math
from datetime import datetime
from dedupsqlfs.my_formats import format_size
from dedupsqlfs.lib.constants import ROOT_SUBVOLUME_NAME, COMPRESSION_PROGS_NONE
import json

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
        return self.getManager().getManager().getTable(name)

    def getLogger(self):
        return self.getManager().getLogger()

    def getLastError(self):
        return self._last_error

    def print_msg(self, msg):
        """
        Print message only if verbosity ON
        @param msg:
        @return:
        """
        if self.getManager().getOption("verbosity") <= 0:
            return self
        sys.stdout.write(msg)
        sys.stdout.flush()
        return self

    def print_out(self, msg):
        """
        Forced message print out
        @param msg:
        @return:
        """
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
        @rtype: bool|dict
        """

        if not name:
            self.getLogger().error("Define subvolume name which you need to create!")
            return False

        tableSubvol = self.getTable('subvolume')
        subvolItem = tableSubvol.find(name)
        if subvolItem:
            self.getLogger().warning("Subvolume with name %r already exists!", name)
            return subvolItem

        newt_ns, newt_s = self.getManager().newctime64_32()

        subvol_id = self.getTable('subvolume').insert(name, newt_s, None, newt_s)
        subvolItem = self.getTable('subvolume').get(subvol_id)

        tableName = self.getTable("name")
        tableTree = self.getTable('tree_%d' % subvolItem["id"])
        tableInode = self.getTable('inode_%d' % subvolItem["id"])

        uid, gid = os.getuid(), os.getgid()
        nameRoot = b''

        name_id = tableName.find(nameRoot)
        if not name_id:
            name_id = tableName.insert(nameRoot)
        # Directory size: name-row-size + inode-row-size + tree-row-size
        sz = tableName.getRowSize(nameRoot) + tableInode.getRowSize() + tableTree.getRowSize()
        inode_id = tableInode.insert(2, self.root_mode, uid, gid, 0, sz, newt_ns, newt_ns, newt_ns)
        tableTree.insert(None, name_id, inode_id)

        self.getManager().getManager().commit()
        self.getManager().getManager().close()

        return subvolItem

    def list(self, with_stats=False):
        """
        List all subvolumes
        """

        tableSubvol = self.getTable('subvolume')

        checkTree = self.getManager().getOption('check_tree_inodes')

        self.print_out("Subvolumes:\n")

        nameMaxLen = 1
        for subvol_id in tableSubvol.get_ids():
            subvol = tableSubvol.get(subvol_id)
            nl = len(subvol["name"].decode())
            if nl > nameMaxLen:
                nameMaxLen = nl

        nameMaxLen += 1

        if not with_stats:
            self.print_out("-"*(nameMaxLen+11+16+22+22+22+1) + "\n")
            self.print_out((("%%-%d" % nameMaxLen) + "s| %-9s| %-14s| %-20s| %-20s| %-20s|\n") % (
                "Name", "ReadOnly", "Apparent Size", "Created", "Last mounted", "Last updated"))
            self.print_out("-"*(nameMaxLen+11+16+22+22+22+1) + "\n")
        else:
            self.print_out("-"*(nameMaxLen+11+16+14+18+16+13+22+22+22+1) + "\n")
            self.print_out((("%%-%d" % nameMaxLen) + "s| %-9s| %-14s| %-12s| %-16s| %-14s| %-11s| %-20s| %-20s| %-20s|\n") % (
                "Name", "ReadOnly",
                "Apparent Size", "Unique Size", "Compressed Size", "Dedupped Size", "Diff Size",
                "Created", "Last mounted", "Last updated"))
            self.print_out("-"*(nameMaxLen+11+16+14+18+16+13+22+22+22+1) + "\n")

        for subvol_id in tableSubvol.get_ids('created_at'):

            subvol = tableSubvol.get(subvol_id)

            apparent_size = 0
            if not with_stats:
                if checkTree:
                    apparent_size = self.get_apparent_size(subvol)
                else:
                    apparent_size = self.get_apparent_size_fast(subvol["name"])
            else:
                usage = self.get_usage(subvol["name"])

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

            if not with_stats:
                self.print_out((("%%-%d" % nameMaxLen) + "s| %-9r| %-14s| %-20s| %-20s| %-20s|\n") % (
                    subvol["name"].decode(),
                    readonly,
                    format_size(apparent_size),
                    ctime,
                    mtime,
                    utime,
                    ))
            else:

                diffStats = self.get_root_diff(subvol["name"])
                if not diffStats:
                    diffStats = {"diffRealSize":0}

                self.print_out((("%%-%d" % nameMaxLen) + "s| %-9s| %-14s| %-12s| %-16s| %-14s| %-11s| %-20s| %-20s| %-20s|\n") % (
                    subvol["name"].decode(),
                    readonly,
                    format_size(usage["apparentSize"]),
                    format_size(usage["uniqueSize"]),
                    format_size(usage["compressedSize"]),
                    format_size(usage["dedupSize"]),
                    format_size(diffStats["diffRealSize"]),
                    ctime,
                    mtime,
                    utime,
                    ))

        if not with_stats:
            self.print_out("-"*(nameMaxLen+11+16+22+22+22+1) + "\n")
        else:
            self.print_out("-"*(nameMaxLen+11+16+14+18+16+13+22+22+22+1) + "\n")

        self.getManager().getManager().close()

        return


    def prepareTreeNameIds(self):
        """
        List all subvolumes
        @rtype: set
        """

        tableSubvol = self.getTable('subvolume')

        nameIds = set()
        pageSize = 10000

        for subvol_id in tableSubvol.get_ids():

            subvol = tableSubvol.get(subvol_id)

            tableTree = self.getTable("tree_%d" % subvol["id"])

            curTree = tableTree.getCursor()
            curTree.execute("SELECT DISTINCT name_id FROM `%s`" % tableTree.getName())

            while True:

                items = curTree.fetchmany(pageSize)

                if not len(items):
                    break

                for node in items:
                    nameIds.add(node["name_id"])

            curTree.close()
            tableTree.close()

        return nameIds

    def prepareIndexHashIds(self):
        """
        List all subvolumes
        @rtype: set
        """

        tableSubvol = self.getTable('subvolume')

        hashIds = set()
        pageSize = 10000

        for subvol_id in tableSubvol.get_ids():

            subvol = tableSubvol.get(subvol_id)

            tableIndex = self.getTable("inode_hash_block_%s" % subvol["id"])
            tableTree = self.getTable('tree_%s' % subvol["id"])

            curIndex = tableIndex.getCursor()
            curIndex.execute("SELECT DISTINCT hash_id,inode_id FROM `%s`" % tableIndex.getName())

            while True:

                items = curIndex.fetchmany(pageSize)
                if not len(items):
                    break

                inode_ids = ",".join(set(str(item["inode_id"]) for item in items))

                inodes_in_tree = set(tableTree.get_inodes_by_inodes_intgen(inode_ids))

                for item in items:

                    # Check if FS tree has inode

                    inode_id = item["inode_id"]
                    if inode_id not in inodes_in_tree:
                        continue

                    hash_id = item["hash_id"]
                    hashIds.add(hash_id)

            curIndex.close()
            tableIndex.close()
            tableTree.close()

        return hashIds


    def prepareIndexHashIdCount(self):
        """
        List all subvolumes
        """

        tableSubvol = self.getTable('subvolume')

        pageSize = 20000

        checkTree = self.getManager().getOption('check_tree_inodes')

        hashCount = {}

        for subvol_id in tableSubvol.get_ids():

            subvol = tableSubvol.get(subvol_id)

            tableIndex = self.getTable("inode_hash_block_%s" % subvol["id"])

            if checkTree:
                tableTree = self.getTable('tree_%d' % subvol["id"])

            # DEBUG
            # self.print_out("-- debug: %s, walk index table %r - begin\n" % (datetime.now(), subvol["hash"]))

            curIndex = tableIndex.getCursor()
            curIndex.execute("SELECT hash_id,inode_id FROM `%s`" % tableIndex.getName())

            nodesInodes = {}

            while True:

                items = curIndex.fetchmany(pageSize)
                if not len(items):
                    break

                inodes_in_tree = set()
                if checkTree:
                    inode_ids = ",".join(set(str(item["inode_id"]) for item in items))
                    inodes_in_tree = set(tableTree.get_inodes_by_inodes_intgen(inode_ids))

                for item in items:

                    # Check if FS tree has inode
                    if checkTree:
                        inode_id = item["inode_id"]
                        if inode_id in nodesInodes:
                            if not nodesInodes[inode_id]:
                                continue
                        else:
                            if inode_id not in inodes_in_tree:
                                nodesInodes[inode_id] = False
                                continue
                            else:
                                nodesInodes[inode_id] = True

                    hash_id = item["hash_id"]
                    hashCount[ hash_id ] = hashCount.get( hash_id, 0 ) + 1

            curIndex.close()
            tableIndex.close()
            if checkTree:
                tableTree.close()

            # DEBUG
            # self.print_out("-- debug: %s, walk index table %r - end\n" % (datetime.now(), subvol["hash"]))

        return hashCount


    def remove(self, name):
        """
        @param name: Subvolume name
        @type  name: bytes
        @return: int - freed space in bytes
        """

        if not name:
            self.getLogger().error("Select subvolume which you need to delete!")
            return

        tableSubvol = self.getTable('subvolume')

        subvolItem = tableSubvol.find(name)
        if not subvolItem:
            self.getLogger().error("Subvolume with name %r not found!", name)
            return False

        self.print_msg("Remove %r snapshot ... " % name)

        freedSpace = 0
        try:
            for tname in (
                'tree_%d' % subvolItem["id"],
                'inode_%d' % subvolItem["id"],
                'inode_hash_block_%d' % subvolItem["id"],
                'inode_option_%d' % subvolItem["id"],
                'link_%d' % subvolItem["id"],
                'xattr_%d' % subvolItem["id"],
            ):
                space = self.getTable(tname).getFileSize()
                self.getTable(tname).drop()
                freedSpace += space

            tableSubvol.delete(subvolItem["id"])
        except Exception as e:
            self.getLogger().warn("Can't remove subvolume and related tables!")
            self.getLogger().error("E: %s", e)
            import traceback
            self.getLogger().error(traceback.format_exc())

        self.print_msg("freed space: %s\n" % format_size(freedSpace))

        self.getManager().getManager().commit()
        self.getManager().getManager().close()

        return freedSpace

    def readonly(self, name, flag=True):
        """
        @param name: Subvolume name
        @type  name: bytes

        @return: success
        @rtype: bool
        """

        if not name:
            self.getLogger().error("Select subvolume which you need to delete!")
            return False

        tableSubvol = self.getTable('subvolume')

        subvol_id = tableSubvol.find(name)
        if not subvol_id:
            self.getLogger().error("Subvolume with name %r not found!", name)
            return False

        changed = self.getTable('subvolume').readonly(subvol_id, flag)

        self.getManager().getManager().commit()
        self.getManager().getManager().close()

        return changed > 0

    def get_apparent_size(self, subvolItem):
        if subvolItem["stats_at"] and subvolItem["stats"]:
            stats_at = int(subvolItem["stats_at"])
            updated_at = int(subvolItem["updated_at"])
            if updated_at <= stats_at:
                # No updates since last stats calculated
                stats = json.loads(subvolItem["stats"])
                return stats["apparentSize"]

        tableTree = self.getTable('tree_%d' % subvolItem["id"])
        tableInode = self.getTable('inode_%d' % subvolItem["id"])

        allInodes = tableTree.get_all_inodes_set()
        apparentSize = tableInode.get_sizes_by_inodes(allInodes)

        tableTree.close()
        tableInode.close()

        return apparentSize

    def get_apparent_size_fast(self, name):

        tableSubvol = self.getTable('subvolume')

        subvolItem = tableSubvol.find(name)
        if not subvolItem:
            self.getLogger().error("Subvolume with name %r not found!", name)
            return 0

        if subvolItem["stats_at"] and subvolItem["stats"]:
            stats_at = int(subvolItem["stats_at"])
            updated_at = int(subvolItem["updated_at"])
            if updated_at <= stats_at:
                # No updates since last stats calculated
                stats = json.loads(subvolItem["stats"])
                return stats["apparentSize"]

        tableInode = self.getTable('inode_%d' % subvolItem["id"])
        sz = tableInode.get_sizes()
        tableInode.close()
        return sz

    def get_usage(self, name, hashTypes=False):
        """
        @param name: Subvolume name
        @type  name: bytes

        @rtype: dict|false
        """

        if not name:
            self.getLogger().error("Select subvolume which you need to process!")
            return False

        tableSubvol = self.getTable('subvolume')

        subvolItem = tableSubvol.find(name)
        if not subvolItem:
            self.getLogger().error("Subvolume with name %r not found!", name)
            return False

        if subvolItem["stats_at"] and subvolItem["stats"]:
            stats_at = int(subvolItem["stats_at"])
            updated_at = int(subvolItem["updated_at"])
            if updated_at <= stats_at:
                # No updates since last stats calculated
                return json.loads(subvolItem["stats"])

        compMethods = {}
        hashCT = {}
        hashSZ = {}

        checkTree = self.getManager().getOption('check_tree_inodes')

        tableHCT = self.getTable('hash_compression_type')
        tableHS = self.getTable('hash_sizes')
        tableIndex = self.getTable('inode_hash_block_%d' % subvolItem["id"])
        tableTree = self.getTable('tree_%d' % subvolItem["id"])
        tableInode = self.getTable('inode_%d' % subvolItem["id"])

        nodesInodes = set()
        if checkTree:
            nodesInodes = tableTree.get_all_inodes_set()

        dataSize = 0
        compressedSize = 0
        uniqueSize = 0
        compressedUniqueSize = 0
        apparentSize = 0

        for item in tableIndex.get_hash_inode_ids():

            # Check if FS tree has inode
            inode_id = int(item["inode_id"])
            getSize = True
            if checkTree:
                if inode_id not in nodesInodes:
                    getSize = False
            else:
                if inode_id in nodesInodes:
                    getSize = False
                nodesInodes.add(inode_id)
            if getSize:
                apparentSize += tableInode.get_size(inode_id)

            hash_id = item["hash_id"]

            if hashTypes:

                if hash_id in hashCT:
                    method = hashCT[hash_id]
                else:
                    hctItem = tableHCT.get(hash_id)
                    if not hctItem:
                        self.getLogger().error("Hash compression type not found! hash_id=%r" % hash_id)
                        continue
                    method = self.getManager().getCompressionTypeName(hctItem["type_id"])
                    hashCT[hash_id] = method

                compMethods[ method ] = compMethods.get(method, 0) + 1

            if hash_id in hashSZ:
                hszItem = hashSZ[hash_id]
            else:
                hszItem = tableHS.get(hash_id)
                hashSZ[hash_id] = hszItem
                uniqueSize += hszItem["writed_size"]
                compressedUniqueSize += hszItem["compressed_size"]

            dataSize += hszItem["writed_size"]
            compressedSize += hszItem["compressed_size"]

        sparseSize = apparentSize - dataSize
        dedupSize = dataSize - uniqueSize

        count_all = 0
        comp_types = {}

        if hashTypes:
            for method, cnt in compMethods.items():
                count_all += cnt
                comp_types[ cnt ] = method

        tableInode.close()
        tableTree.close()
        tableIndex.close()

        stats = {
            "apparentSize": apparentSize,
            "dataSize": dataSize,
            "dedupSize": dedupSize,
            "sparseSize": sparseSize,
            "uniqueSize": uniqueSize,
            "compressedSize": compressedSize,
            "compressedUniqueSize": compressedUniqueSize,
            "compressionTypes": comp_types,
            "compressionTypesAll": count_all
        }

        tableSubvol.stats_time(subvolItem["id"])
        tableSubvol.set_stats(subvolItem["id"], json.dumps(stats))

        tableSubvol.commit()

        return stats

    def get_root_diff(self, name):
        """
        @param name: Subvolume name
        @type  name: bytes

        @rtype: dict|false
        """

        if not name:
            self.getLogger().error("Select subvolume which you need to process!")
            return False

        tableSubvol = self.getTable('subvolume')

        subvolRootItem = tableSubvol.find(ROOT_SUBVOLUME_NAME)
        if not subvolRootItem:
            self.getLogger().error("Subvolume with name %r not found!", ROOT_SUBVOLUME_NAME)
            return False

        if name == ROOT_SUBVOLUME_NAME:
            stats = {
                "diffRealSize": 0,
                "diffBlocksCount": 0
            }
            tableSubvol.root_diff_time(subvolRootItem["id"])
            tableSubvol.set_root_diff(subvolRootItem["id"], json.dumps(stats))
            tableSubvol.commit()
            return stats


        subvolItem = tableSubvol.find(name)
        if not subvolItem:
            self.getLogger().error("Subvolume with name %r not found!", name)
            return False

        if subvolItem["root_diff_at"] and subvolItem["root_diff"]:
            root_diff_at = int(subvolItem["root_diff_at"])
            updated_at = int(subvolRootItem["updated_at"])
            if updated_at <= root_diff_at:
                # No updates since last stats calculated
                return json.loads(subvolItem["root_diff"])

        tableRootIndex = self.getTable('inode_hash_block_%d' % subvolRootItem["id"])
        tableIndex = self.getTable('inode_hash_block_%d' % subvolItem["id"])

        rootUniqHashes = tableRootIndex.get_uniq_hashes()
        subvolUniqHashes = tableIndex.get_uniq_hashes()

        diffUniqHashes = subvolUniqHashes - rootUniqHashes

        diffBlocksCount = tableIndex.count_hashes_by_hashes(diffUniqHashes)
        diffRealSize = tableIndex.count_realsize_by_hashes(diffUniqHashes)

        tableIndex.close()

        stats = {
            "diffRealSize": diffRealSize,
            "diffBlocksCount": diffBlocksCount
        }

        tableSubvol.root_diff_time(subvolItem["id"])
        tableSubvol.set_root_diff(subvolItem["id"], json.dumps(stats))

        tableSubvol.commit()

        return stats


    def clean_stats(self, name):
        """
        Called only if garbage collector found garbage on subvol

        @param name:
        @return:
        """

        if not name:
            self.getLogger().error("Select subvolume which you need to process!")
            return False

        tableSubvol = self.getTable('subvolume')

        subvolItem = tableSubvol.find(name)
        if not subvolItem:
            self.getLogger().error("Subvolume with name %r not found!", name)
            return False

        tableSubvol.stats_time(subvolItem["id"], 0)
        tableSubvol.set_stats(subvolItem["id"], None)

        tableSubvol.root_diff_time(subvolItem["id"], 0)
        tableSubvol.set_root_diff(subvolItem["id"], None)

        tableSubvol.commit()

        return

    def clean_non_root_subvol_diff_stats(self):
        """
        Called only if garbage collector found garbage on subvol

        @param name:
        @return:
        """

        tableSubvol = self.getTable('subvolume')

        for subvol_id in tableSubvol.get_ids():

            subvol = tableSubvol.get(subvol_id)

            if subvol['name'] == ROOT_SUBVOLUME_NAME:
                continue

            tableSubvol.root_diff_time(subvol["id"], 0)
            tableSubvol.set_root_diff(subvol["id"], None)

        tableSubvol.commit()

        return

    def report_usage(self, name):
        """
        @param name: Subvolume name
        @type  name: bytes
        """
        usage = self.get_usage(name, True)
        if not usage:
            return

        diff = self.get_root_diff(name)

        self.print_msg("\n")

        self.print_out("Apparent size is %s.\n" % format_size(usage["apparentSize"]) )
        self.print_out("Unique data size is %s.\n" % format_size(usage["uniqueSize"]) )
        self.print_out("Sparse data size is %s.\n" % format_size(usage["sparseSize"]) )
        self.print_out("Deduped data size is %s.\n" % format_size(usage["dedupSize"]) )

        if usage["apparentSize"]:
            self.print_out("Compressed data size is %s (%.2f %%).\n" % (
                format_size(usage["compressedSize"]), usage["compressedSize"] * 100.0 / usage["apparentSize"]
            ))
        if usage["uniqueSize"]:
            self.print_out("Compressed unique data size is %s (%.2f %%).\n" % (
                format_size(usage["compressedUniqueSize"]), usage["compressedUniqueSize"] * 100.0 / usage["uniqueSize"]
            ))

        if diff and diff["diffRealSize"]:
            self.print_out("Difference between @root: %s.\n" % format_size(diff["diffRealSize"]))

        count_all = usage["compressionTypesAll"]
        comp_types = usage["compressionTypes"]

        keys = list(comp_types.keys())
        keys.sort(reverse=True)

        if keys:
            self.print_out("Compression by types:\n")
        for key in keys:
            compression = comp_types[key]
            self.print_out(" %8s used by %.2f%% blocks\n" % (
                compression, 100.0 * int(key) / count_all
            ))

        return


    def decompress_all_non_root_tables(self):
        """
        Decompress all subvolumes sqlite table files
        """

        manager = self.getManager().getManager()
        if manager.TYPE != 'sqlite':
            return

        tableSubvol = self.getTable('subvolume')

        tableNames = ('inode_hash_block', 'tree', 'inode', 'inode_option', 'link', 'xattr',)

        for subvol_id in tableSubvol.get_ids('created_at'):

            subvol = tableSubvol.get(subvol_id)

            if subvol['name'] == ROOT_SUBVOLUME_NAME:
                continue

            for tn in tableNames:
                """
                @var table: L{dedupsqlfs.db.sqlite.table._base.Table}
                """
                table = self.getTable(tn + '_%d' % subvol["id"])
                table.setCompressed(False)
                table.close(nocompress=True)

        manager.close()

        return


    def compress_all_non_root_tables(self):
        """
        Compress all subvolumes sqlite table files
        """

        manager = self.getManager().getManager()
        if manager.TYPE != 'sqlite':
            return

        tableSubvol = self.getTable('subvolume')

        tableNames = ('inode_hash_block', 'tree', 'inode', 'inode_option', 'link', 'xattr',)

        prog = self.getManager().getOption("sqlite_compression_prog")

        for subvol_id in tableSubvol.get_ids('created_at'):

            subvol = tableSubvol.get(subvol_id)

            if subvol['name'] == ROOT_SUBVOLUME_NAME:
                continue

            for tn in tableNames:
                """
                @var table: L{dedupsqlfs.db.sqlite.table._base.Table}
                """
                table = self.getTable(tn + '_%d' % subvol["id"])

                compress = False

                if os.path.getsize(table.getDbFilePath()) > 1024 * 1024:
                    compress = True
                    table.setCompressed()
                    if prog in (None, COMPRESSION_PROGS_NONE,):
                        compress = False
                    else:
                        table.setCompressionProg(prog)
                table.close(compress is False)

        manager.close()

        return

    def count_today_created_subvols(self, count_readonly=False):
        """
        Count subvolumes/snapshots by create time
        If create DATE equals TODAY DATE

        @param count_readonly: Snapshots is readonly
        """

        manager = self.getManager().getManager()

        tableSubvol = self.getTable('subvolume')

        today = datetime.now().date()

        count = 0

        for subvol_id in tableSubvol.get_ids('created_at'):

            subvol = tableSubvol.get(subvol_id)

            if subvol['name'] == ROOT_SUBVOLUME_NAME:
                continue

            if not subvol['readonly'] and count_readonly:
                continue

            subDay = datetime.fromtimestamp(subvol['created_at']).date()

            if subDay == today:
                count += 1

        manager.close()

        self.print_out("%s\n" % count)

        return

    pass
