# -*- coding: utf8 -*-

# Imports. {{{1

import sys

# Try to load the required modules from Python's standard library.
try:
    import errno
    import os
    import stat
    import time
    import traceback
except ImportError as e:
    msg = "Error: Failed to load one of the required Python modules! (%s)\n"
    sys.stderr.write(msg % str(e))
    sys.exit(1)

# Try to load the Python FUSE binding.
try:
    import llfuse as fuse
    from llfuse import FUSEError
except ImportError:
    sys.stderr.write("Error: The Python FUSE binding isn't installed!\n" + \
        "If you're on Ubuntu try running `sudo apt-get install python-fuse'.\n")
    sys.exit(1)

# Local modules that are mostly useful for debugging.
from dedupsqlfs.my_formats import format_size
from dedupsqlfs.lib import constants
from dedupsqlfs.log import logging, DEBUG_VERBOSE
from dedupsqlfs.fuse.compress.mp import MultiProcCompressTool

# Storage for options and DB interface
# Implements FUSE interface
class DedupFS(object): # {{{1

    def __init__(self, operations, mountpoint, options, *args, **kw):  # {{{2
        """
        @type operations: dedupsqlfs.fuse.operations.DedupOperations
        """

        self.options = dict(vars(options))

        self._compressors = {}
        self._readonly = False

        self.mountpoint = mountpoint
        self.operations = operations
        self.operations.setApplication(self)

        self.__init_logging()

        kwargs = kw
        kwargs.update(self.parseFuseOptions(self.getOption("mountoption")))

        self._opts = []
        for key, value in kwargs.items():
            if value in (True, False,):
                self._opts.append(key)
            else:
                self._opts.append("%s=%s" % (key, value,))

        self.getLogger().debug("DedupFS options: %r" % (self.options,))
        self.getLogger().debug("DedupFS mount options: %r" % (self._opts,))
        self.getLogger().debug("DedupFS mountpoint: %r" % (self.mountpoint,))

        self._compressTool = MultiProcCompressTool()

        pass


    def main(self):
        self._fixCompressionOptions()

        self._compressTool.init()

        try:
            fuse.init(self.operations, self.mountpoint, self._opts)
            fuse.main(single=True)
        except:
            fuse.close(unmount=False)
            self._compressTool.stop()
            raise

        fuse.close()
        self._compressTool.stop()

    def parseFuseOptions(self, mountoptions):
        if not mountoptions:
            mountoptions = []
        args = {}
        for optstr in mountoptions:
            opts = optstr.split(",")
            for optstr in opts:
                opt = optstr.split("=")
                if len(opt) > 1:
                    args[opt[0]] = opt[1]
                else:
                    args[opt[0]] = True
        return args

    def getOption(self, key):
        return self.options.get(key)

    def setOption(self, key, value):
        self.options[key] = value
        return self

    def getLogger(self):
        return self.logger

    def setReadonly(self, flag=True):
        self._readonly = flag == True
        return self

    def isReadonly(self):
        return self._readonly

    def hasFsStorageOnPath(self, basePath):
        has = True
        from dedupsqlfs.db.sqlite.manager import DbManager as SqliteManager
        manager = SqliteManager(dbname=self.getOption("name"))
        manager.setBasepath(basePath)
        if not manager.isSupportedStorage():
            from dedupsqlfs.db.mysql.manager import DbManager as MysqlManager
            manager = MysqlManager(dbname=self.getOption("name"))
            manager.setBasepath(os.path.expanduser(self.getOption("data")))
            if not manager.isSupportedStorage():
                has = False
        return has


    def appendCompression(self, name):
        self._compressTool.appendCompression(name)
        return self

    def _fixCompressionOptions(self):

        method = self.getOption("compression_method")
        if method and method.find("=") != -1:
            method, level = method.split("=")
            self._compressTool.setOption("compression_method", method)
            self._compressTool.getCompressor(method).setCustomCompressionLevel(level)
        else:
            self._compressTool.setOption("compression_method", method)

        methods = self.getOption("compression_custom")
        if methods and type(methods) in (tuple, list,):
            methods = list(methods)
            for i in range(len(methods)):
                method = methods[i]
                if method and method.find("=") != -1:
                    method, level = method.split("=")
                    methods[i] = method
                    self._compressTool.getCompressor(method).setCustomCompressionLevel(level)
            self._compressTool.setOption("compression_custom", methods)

        self._compressTool.setOption("compression_minimal_size", self.getOption("compression_minimal_size"))
        self._compressTool.setOption("compression_level", self.getOption("compression_level"))
        self._compressTool.setOption("compression_forced", self.getOption("compression_forced"))

        return


    def getCompressTool(self):
        return self._compressTool

    def compressData(self, dataBlocks):
        return self._compressTool.compressData(dataBlocks)

    def decompressData(self, method, compressedBlock):
        return self._compressTool.decompressData(method, compressedBlock)


    def setDataDirectory(self, data_dir_path):
        self.operations.getManager().setBasepath(data_dir_path)
        return self

    def saveCompressionMethods(self, methods=None):
        if methods and type(methods) is list:
            manager = self.operations.getManager()
            table = manager.getTable("compression_type")
            for m in methods:

                if m and m.find("=") != -1:
                    m, level = m.split("=")

                m_id = table.find(m)
                if not m_id:
                    table.insert(m)
            manager.commit()
        return self

    # Miscellaneous methods: {{{2

    def __init_logging(self): # {{{3
        # Initialize a Logger() object to handle logging.
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.ERROR)
        self.logger.addHandler(logging.StreamHandler(sys.stderr))
        # Configure logging of messages to a file.
        if self.getOption("log_file"):
            handler = logging.StreamHandler(open(self.getOption("log_file"), 'a'))
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.getLogger().addHandler(handler)
        # Convert verbosity argument to logging level?
        if self.getOption("verbosity") > 0:
            if self.getOption("verbosity") <= 1:
                self.getLogger().setLevel(logging.INFO)
            elif self.getOption("verbosity") <= 2:
                self.getLogger().setLevel(logging.DEBUG)
            elif self.getOption("verbosity") <= 3:
                self.getLogger().setLevel(DEBUG_VERBOSE)
            else:
                self.getLogger().setLevel(logging.NOTSET)

    def report_disk_usage(self): # {{{3

        from dedupsqlfs.fuse.subvolume import Subvolume

        subv = Subvolume(self.operations)

        manager = self.operations.getManager()

        self.getLogger().info("--" * 39)

        tableSubvol = manager.getTable("subvolume")

        disk_usage = 0
        disk_usage += manager.getTable("hash").getFileSize()
        disk_usage += manager.getTable("hash_compression_type").getFileSize()
        disk_usage += manager.getTable("hash_sizes").getFileSize()
        disk_usage += manager.getTable("compression_type").getFileSize()
        disk_usage += manager.getTable("name").getFileSize()
        disk_usage += manager.getTable("name_pattern_option").getFileSize()
        disk_usage += manager.getTable("option").getFileSize()
        disk_usage += manager.getTable("subvolume").getFileSize()
        disk_usage += manager.getTable("block").getFileSize()

        apparentSize = 0
        dataSize = 0
        uniqueSize = 0
        compressedSize = 0
        compressedUniqueSize = 0
        compMethods = {}
        hashCT = {}
        hashSZ = {}

        for subvol_id in tableSubvol.get_ids():

            subvol = tableSubvol.get(subvol_id)
            apparentSize += subv.get_apparent_size(subvol)

            disk_usage += manager.getTable("inode_" + subvol["hash"]).getFileSize()
            disk_usage += manager.getTable("inode_option_" + subvol["hash"]).getFileSize()
            disk_usage += manager.getTable("inode_hash_block_" + subvol["hash"]).getFileSize()
            disk_usage += manager.getTable("link_" + subvol["hash"]).getFileSize()
            disk_usage += manager.getTable("xattr_" + subvol["hash"]).getFileSize()
            disk_usage += manager.getTable("tree_" + subvol["hash"]).getFileSize()


        tableHCT = manager.getTable('hash_compression_type')
        tableHS = manager.getTable('hash_sizes')

        subv.prepareIndexHashIdCount()
        tableTmp = manager.getTable('tmp_id_count')

        curH = tableTmp.getCursor()
        curH.execute("SELECT * FROM `%s`" % tableTmp.getName())

        for item in iter(curH.fetchone, None):

            hash_id = item["id"]
            hash_cnt = item["cnt"]

            if hash_id in hashCT:
                method = hashCT[hash_id]
            else:
                hctItem = tableHCT.get(hash_id)
                method = self.operations.getCompressionTypeName(hctItem["type_id"])
                hashCT[hash_id] = method

            compMethods[ method ] = compMethods.get(method, 0) + 1

            if hash_id in hashSZ:
                hszItem = hashSZ[hash_id]
            else:
                hszItem = tableHS.get(hash_id)
                hashSZ[hash_id] = hszItem
                uniqueSize += hszItem["writed_size"]
                compressedUniqueSize += hszItem["compressed_size"]

            dataSize += hszItem["writed_size"]*hash_cnt
            compressedSize += hszItem["compressed_size"]*hash_cnt

        tableTmp.drop()

        sparseSize = apparentSize - dataSize
        dedupSize = dataSize - uniqueSize

        count_all = 0
        comp_types = {}

        for method, cnt in compMethods.items():
            count_all += cnt
            comp_types[ cnt ] = method

        if apparentSize:
            self.getLogger().info("Apparent size is %s, unique %s.",
                             format_size(apparentSize), format_size(uniqueSize)
            )

            self.getLogger().info("Deduped size is %s, ratio is %.2f%%.",
                             format_size(dedupSize),
                             100.0 * dedupSize / apparentSize)

            self.getLogger().info("Sparse size is %s, ratio is %.2f%%.",
                             format_size(sparseSize),
                             100.0 * sparseSize / apparentSize)

            self.getLogger().info("Databases take up %s, ratio is %.2f%%.",
                             format_size(disk_usage),
                             100.0 * disk_usage / uniqueSize)
            self.getLogger().info("Compressed data take up %s:\n- unique %s,\n- saved apparent space is %.2f%%,\n- use of database space: %.2f%%).",
                             format_size(compressedSize), format_size(compressedUniqueSize),
                             100.0 * (apparentSize - compressedSize) / apparentSize,
                             100.0 * compressedUniqueSize / disk_usage,
            )
            self.getLogger().info("Meta data and indexes take up %s:\n- ratio over apparent is %.2f%%,\n- use of database space: %.2f%%).",
                             format_size(disk_usage - compressedUniqueSize),
                             100.0 * (disk_usage - compressedUniqueSize) / uniqueSize,
                             100.0 * (disk_usage - compressedUniqueSize) / disk_usage,
            )

        else:
            self.getLogger().info("Apparent size is %s.",
                             format_size(apparentSize)
            )
            self.getLogger().info("Compressed size is %s.",
                             format_size(compressedSize)
            )
            self.getLogger().info("Databases take up %s.",
                             format_size(disk_usage)
            )

        if compressedSize:
            self.getLogger().info("--" * 39)
            self.getLogger().info("Compression by types:")

            keys = list(comp_types.keys())
            keys.sort(reverse=True)

            for key in keys:
                compression = comp_types[key]
                self.getLogger().info(" %8s used by %.2f%% blocks",
                    compression, 100.0 * key / count_all
                )

        return

# vim: ts=4 sw=4 et
