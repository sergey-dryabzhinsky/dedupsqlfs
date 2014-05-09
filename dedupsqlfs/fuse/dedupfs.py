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

        self._fixCompressionOptions()

        pass


    def main(self):
        try:
            fuse.init(self.operations, self.mountpoint, self._opts)
            fuse.main(single=True)
        except:
            fuse.close(unmount=False)
            raise
        fuse.close()

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

        level = None
        if name and name.find("=") != -1:
            name, level = name.split("=")

        if name == "none":
            from dedupsqlfs.compression.none import NoneCompression
            self._compressors[name] = NoneCompression()
        elif name == "zlib":
            from dedupsqlfs.compression.zlib import ZlibCompression
            self._compressors[name] = ZlibCompression()
        elif name == "bz2":
            from dedupsqlfs.compression.bz2 import Bz2Compression
            self._compressors[name] = Bz2Compression()
        elif name == "lzma":
            from dedupsqlfs.compression.lzma import LzmaCompression
            self._compressors[name] = LzmaCompression()
        elif name == "lzo":
            from dedupsqlfs.compression.lzo import LzoCompression
            self._compressors[name] = LzoCompression()
        elif name == "lz4":
            from dedupsqlfs.compression.lz4 import Lz4Compression
            self._compressors[name] = Lz4Compression()
        elif name == "snappy":
            from dedupsqlfs.compression.snappy import SnappyCompression
            self._compressors[name] = SnappyCompression()
        else:
            raise ValueError("Unknown compression method: %r" % (name,))

        self._compressors[name].setDefaultCompressionLevel(level)

        return self

    def _fixCompressionOptions(self):

        method = self.getOption("compression_method")
        if method and method.find("=") != -1:
            method, level = method.split("=")
            self.setOption("compression_method", method)

        methods = self.getOption("compression_custom")
        if methods and type(methods) in (tuple, list,):
            methods = list(methods)
            for i in range(len(methods)):
                method = methods[i]
                if method and method.find("=") != -1:
                    method, level = method.split("=")
                    methods[i] = method
            self.setOption("compression_custom", methods)

        return

    def getCompressor(self, name):
        level = None
        if name and name.find("=") != -1:
            name, level = name.split("=")
        if name in self._compressors:
            comp = self._compressors[name]
            comp.setDefaultCompressionLevel(level)
            return comp
        else:
            raise ValueError("Unknown compression method: %r" % (name,))

    def compressData(self, data):
        """
        Compress data and returns back

        @return tuple (compressed data (bytes), compresion method (string) )
        """
        method = self.getOption("compression_method")
        forced = self.getOption("compression_forced")
        level = self.getOption("compression_level")

        cdata = data
        data_length = len(data)
        cmethod = constants.COMPRESSION_TYPE_NONE

        if data_length <= self.getOption("compression_minimal_size") and not forced:
            return cdata, cmethod

        if method != constants.COMPRESSION_TYPE_NONE:
            if method not in (constants.COMPRESSION_TYPE_BEST, constants.COMPRESSION_TYPE_CUSTOM,):
                comp = self.getCompressor(method)
                if comp.isDataMayBeCompressed(data):
                    cdata = comp.compressData(data, level)
                    cmethod = method
                    if data_length <= len(cdata) and not forced:
                        cdata = data
                        cmethod = constants.COMPRESSION_TYPE_NONE
            else:
                min_len = data_length * 2
                # BEST
                methods = self._compressors.keys()
                if method == constants.COMPRESSION_TYPE_CUSTOM:
                    methods = self.getOption("compression_custom")
                for m in methods:
                    comp = self.getCompressor(m)
                    if comp.isDataMayBeCompressed(data):
                        _cdata = comp.compressData(data, level)
                        cdata_length = len(_cdata)
                        if min_len > cdata_length:
                            min_len = cdata_length
                            cdata = _cdata
                            cmethod = m

                if data_length <= min_len and not forced:
                    cdata = data
                    cmethod = constants.COMPRESSION_TYPE_NONE

        return cdata, cmethod

    def decompressData(self, method, data):
        """
        deCompress data and returns back

        @return bytes
        """
        comp = self._compressors[ method ]
        return comp.decompressData(data)

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
        manager = self.operations.getManager()
        disk_usage = manager.getFileSize()

        indexTable = manager.getTable("inode_hash_block")
        hbsTable = manager.getTable("hash_block_size")

        apparent_size, compressed_size, sparce_size = self.operations.getDataSize(use_subvol=False)
        apparent_size_u, compressed_size_u = self.operations.getDataSize(use_subvol=False, unique=True)

        self.getLogger().info("--" * 79)

        #print("disk_usage: %r" % disk_usage)
        #print("apparent_size: %r" % apparent_size)

        if apparent_size:
            self.getLogger().info("Apparent size is %s (unique %s).",
                             format_size(apparent_size), format_size(apparent_size_u)
            )

            curIndex = indexTable.getCursor()

            curIndex.execute("SELECT COUNT(`hash_id`) AS `cnt`, `hash_id` FROM `inode_hash_block` GROUP BY `hash_id`")

            dedup_size = 0
            while True:
                indexItems = curIndex.fetchmany(1024)
                if not indexItems:
                    break

                hids = ()
                cnts = {}
                for item in indexItems:
                    if item["cnt"] > 1:
                        hids += (item["hash_id"],)
                        cnts[ item["hash_id"] ] = item["cnt"]-1

                if hids:
                    rsizes = hbsTable.get_real_sizes(hids)
                    for item in rsizes:
                        cnt = cnts[ item["hash_id"] ]
                        dedup_size += cnt * item["real_size"]

            self.getLogger().info("Deduped size is %s (ratio is %.2f%%).",
                             format_size(dedup_size),
                             100.0 * dedup_size / apparent_size)

            self.getLogger().info("Sparce size is %s (ratio is %.2f%%).",
                             format_size(sparce_size),
                             100.0 * sparce_size / apparent_size)

            self.getLogger().info("Databases take up %s (ratio is %.2f%%).",
                             format_size(disk_usage),
                             100.0 * disk_usage / apparent_size_u)
            self.getLogger().info("Compressed data take up %s (unique %s, ratioA is %.2f%%, ratioD is %.2f%%).",
                             format_size(compressed_size), format_size(compressed_size_u),
                             100.0 * (apparent_size - compressed_size) / apparent_size,
                             100.0 * compressed_size_u / disk_usage,
            )
            self.getLogger().info("Meta data and indexes take up %s (ratioA is %.2f%%, rationD is %.2f%%).",
                             format_size(disk_usage - compressed_size_u),
                             100.0 * (disk_usage - compressed_size_u) / apparent_size_u,
                             100.0 * (disk_usage - compressed_size_u) / disk_usage,
            )

        else:
            self.getLogger().info("Apparent size is %s.",
                             format_size(apparent_size)
            )
            self.getLogger().info("Compressed size is %s.",
                             format_size(compressed_size)
            )
            self.getLogger().info("Databases take up %s.",
                             format_size(disk_usage)
            )

        if compressed_size:
            self.getLogger().info("--" * 79)
            self.getLogger().info("Compression by types:")
            count_all = 0
            comp_types = {}

            hctTable = manager.getTable("hash_compression_type")

            for item in hctTable.count_compression_type():
                count_all += item["cnt"]
                comp_types[ item["cnt"] ] = self.operations.getCompressionTypeName( item["type_id"] )

            keys = list(comp_types.keys())
            keys.sort(reverse=True)

            for key in keys:
                compression = comp_types[key]
                self.getLogger().info(" %8s used by %.2f%% blocks",
                    compression, 100.0 * key / count_all
                )

        return

# vim: ts=4 sw=4 et
