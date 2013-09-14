# -*- coding: utf8 -*-

# Imports. {{{1

import sys

# Try to load the required modules from Python's standard library.
try:
    import errno
    import logging
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
    from dedupsqlfs.lib import constants
except ImportError:
    sys.stderr.write("Error: The Python FUSE binding isn't installed!\n" + \
        "If you're on Ubuntu try running `sudo apt-get install python-fuse'.\n")
    sys.exit(1)

# Local modules that are mostly useful for debugging.
from dedupsqlfs.my_formats import format_size

# Storage for options and DB interface
# Implements FUSE interface
class DedupFS(object): # {{{1

    def __init__(self, operations, mountpoint, options, *args, **kw):  # {{{2
        """
        @type operations: dedupsqlfs.fuse.operations.DedupOperations
        """

        self.options = vars(options)

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

    def getLogger(self):
        return self.logger

    def setReadonly(self, flag=True):
        self._readonly = flag == True
        return self

    def isReadonly(self):
        return self._readonly

    def appendCompression(self, name, func_comp, func_decomp):
        self._compressors[name] = (func_comp, func_decomp)
        return self

    def compressData(self, data):
        """
        Compress data and returns back

        @return tuple (compressed data (bytes), compresion method (string) )
        """
        method = self.getOption("compression_method")
        forced = self.getOption("compression_forced")
        cdata = data
        data_length = len(data)
        cmethod = constants.COMPRESSION_TYPE_NONE

        if data_length <= self.getOption("compression_minimal_size") and not forced:
            return cdata, cmethod

        if method != constants.COMPRESSION_TYPE_NONE:
            if method not in (constants.COMPRESSION_TYPE_BEST, constants.COMPRESSION_TYPE_CUSTOM):
                func_comp = self._compressors[ method ][0]
                cdata = func_comp(data)
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
                    func_comp = self._compressors[ m ][0]
                    _cdata = func_comp(data)
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
        func_decomp = self._compressors[ method ][1]
        dcdata = func_decomp(data)
        return dcdata

    def setDataDirectory(self, data_dir_path):
        self.operations.getManager().setBasepath(data_dir_path)
        return self

    def saveCompressionMethods(self, methods=None):
        if methods and type(methods) is list:
            manager = self.operations.getManager()
            table = manager.getTable("compression_type")
            for m in methods:
                m_id = table.find(m)
                if not m_id:
                    table.insert(m)
            manager.commit()
        return self

    def createSubvolume(self, name):
        """
        @todo
        """
        return self

    # Miscellaneous methods: {{{2

    def __init_logging(self): # {{{3
        # Initialize a Logger() object to handle logging.
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.ERROR)
        self.logger.addHandler(logging.StreamHandler(sys.stderr))
        # Configure logging of messages to a file.
        if self.getOption("log_file"):
            handler = logging.StreamHandler(open(self.getOption("log_file"), 'w'))
            self.getLogger().addHandler(handler)
        # Convert verbosity argument to logging level?
        if self.getOption("verbosity") > 0:
            if self.getOption("verbosity") <= 1:
                self.getLogger().setLevel(logging.INFO)
            elif self.getOption("verbosity") <= 2:
                self.getLogger().setLevel(logging.DEBUG)
            else:
                self.getLogger().setLevel(logging.NOTSET)

    def report_disk_usage(self): # {{{3
        manager = self.operations.getManager()
        disk_usage = manager.getFileSize()

        indexTable = manager.getTable("inode_hash_block")

        apparent_size = self.operations.getApparentSize()

        self.getLogger().info("--" * 79)

        if apparent_size:
            self.getLogger().info("Apparent size is %s.",
                             format_size(apparent_size)
            )
            self.getLogger().info("Databases take up %s (ratio is %.2f%%).",
                             format_size(disk_usage),
                             100.0 * disk_usage / apparent_size
            )

            curIndex = indexTable.getCursor()

            curIndex.execute("SELECT COUNT(hash_id)-1 AS cnt,block_size FROM `inode_hash_block` GROUP BY hash_id HAVING cnt>0")

            dedup_size = 0
            while True:
                indexItem = curIndex.fetchone()
                if not indexItem:
                    break
                dedup_size += indexItem["cnt"] * indexItem["block_size"]

            self.getLogger().info("Deduped size is %s (ratio is %.2f%%).",
                             format_size(dedup_size),
                             100.0 * dedup_size / apparent_size)
            self.getLogger().info("Compressed size is %s (ratio is %.2f%%).",
                             format_size(apparent_size - dedup_size),
                             100.0 - 100.0 * disk_usage / (apparent_size - dedup_size))
        else:
            self.getLogger().info("Apparent size is %s.",
                             format_size(apparent_size)
            )
            self.getLogger().info("Databases take up %s.",
                             format_size(disk_usage)
            )

        self.getLogger().info("--" * 79)
        self.getLogger().info("Compression by types:")
        count_all = 0
        comp_types = {}
        for item in indexTable.count_compression_type():
            count_all += item["cnt"]
            comp_types[ item["cnt"] ] = self.operations.getCompressionTypeName( item["compression_type_id"] )

        keys = list(comp_types.keys())
        keys.sort(reverse=True)

        for key in keys:
            compression = comp_types[key]
            self.getLogger().info(" %8s used by %.2f%% blocks",
                compression, 100.0 * key / count_all
            )

        return

# vim: ts=4 sw=4 et
