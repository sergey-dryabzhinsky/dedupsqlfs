# -*- coding: utf8 -*-
"""
Base class for compression tool
Some base methods, properties...
Used in single or multi process compression classes
"""

__author__ = 'sergey'

from time import time
from dedupsqlfs.lib import constants

class Task(object):
    """
    @ivar key: int|str          - task primary key
    @ivar data: bytes           - data to compress
    """

    key = None

    data = None

    pass

class Result(object):
    """
    @ivar key: int|str          - task primary key
    @ivar method: str           - compression method used
    @ivar cdata: bytes          - compressed data
    """

    key = None

    method = None
    cdata = None

    pass

class BaseCompressTool(object):

    _compressors = None
    """
    @ivar _compressors: Dictionary of compression methods: { method: L{dedupsqlfs.compression.BaseCompression} }
    @type _compressors: dict
    """

    _options = None
    """
    @ivar _options: Application options
    @type _options: dict
    """

    _methods = None
    """
    @ivar _methods: Avalable to use Methods
    @type _methods: set
    """

    _logger = None
    """
    @ivar _logger: Logger
    @type _logger: logger
    """

    time_spent_compressing = 0

    def __init__(self):
        self._compressors = {}
        self._options = {}
        self._methods = set()
        pass

    def checkCpuLimit(self):
        return 1

    def init(self, logger):
        self.time_spent_compressing = 0

        methods = set(self.getOption("compression"))
        if constants.COMPRESSION_TYPE_FAST in methods:
            methods = set(self._compressors.keys())
        if constants.COMPRESSION_TYPE_BEST in methods:
            methods = set(self._compressors.keys())
        if constants.COMPRESSION_TYPE_DEFAULT in methods:
            methods = set(self._compressors.keys())
        self._methods = methods
        if constants.COMPRESSION_TYPE_NONE in self._methods:
            self._methods.remove(constants.COMPRESSION_TYPE_NONE)

        self._logger = logger

        self.getLogger().debug("BaseCompressTool::init - selected methods = %r", self._methods)
        return self

    def stop(self):
        return self

    def getLogger(self):
        return self._logger

    def setOption(self, key, value):
        self._options[key] = value
        return self

    def getOption(self, key, default=None):
        return self._options.get(key, default)

    def appendCompression(self, name):

        level = None
        if name and name.find(":") != -1:
            name, level = name.split(":")

        if name == "none":
            from dedupsqlfs.compression.none import NoneCompression
            self._compressors[name] = NoneCompression()
        elif name == "zlib":
            from dedupsqlfs.compression.zlib import ZlibCompression
            self._compressors[name] = ZlibCompression()
        elif name == "deflate":
            from dedupsqlfs.compression.deflate import DeflateCompression
            self._compressors[name] = DeflateCompression()
        elif name == "brotli":
            from dedupsqlfs.compression.brotli import BrotliCompression
            self._compressors[name] = BrotliCompression()
        elif name == "bz2":
            from dedupsqlfs.compression.bz2 import Bz2Compression
            self._compressors[name] = Bz2Compression()
        elif name == "xz":
            from dedupsqlfs.compression.lzma import LzmaCompression
            self._compressors[name] = LzmaCompression()
        elif name == "lzo":
            from dedupsqlfs.compression.lzo import LzoCompression
            self._compressors[name] = LzoCompression()
        elif name == "lz4":
            from dedupsqlfs.compression.lz4 import Lz4Compression
            self._compressors[name] = Lz4Compression()
        elif name == "lz4r07":
            from dedupsqlfs.compression.lz4r07 import Lz4r07Compression
            self._compressors[name] = Lz4r07Compression()
        elif name == "lz4h":
            from dedupsqlfs.compression.lz4h import Lz4hCompression
            self._compressors[name] = Lz4hCompression()
        elif name == "snappy":
            from dedupsqlfs.compression.snappy import SnappyCompression
            self._compressors[name] = SnappyCompression()
        elif name == "quicklz":
            from dedupsqlfs.compression.quicklz import QuickLzCompression
            self._compressors[name] = QuickLzCompression()
        elif name == "quicklzf":
            from dedupsqlfs.compression.quicklzf import QuickLzFCompression
            self._compressors[name] = QuickLzFCompression()
        elif name == "quicklzm":
            from dedupsqlfs.compression.quicklzm import QuickLzMCompression
            self._compressors[name] = QuickLzMCompression()
        elif name == "quicklzb":
            from dedupsqlfs.compression.quicklzb import QuickLzBCompression
            self._compressors[name] = QuickLzBCompression()
        elif name == "zstd":
            from dedupsqlfs.compression.zstd import ZstdCompression
            self._compressors[name] = ZstdCompression()
        else:
            raise ValueError("Unknown compression method! %r" % name)

        self._compressors[name].setCustomCompressionLevel(level)

        return self


    def getCompressor(self, name):
        if name in self._compressors:
            comp = self._compressors[name]
            return comp
        else:
            raise ValueError("Unknown compression method: %r" % (name,))


    def isMethodSelected(self, name):
        return name in self._methods

    def _compressData(self, data):
        """
        Compress data and returns back

        @return tuple (compressed data (bytes), compresion method (string) )
        """

        forced = self.getOption("compression_forced")
        level = self.getOption("compression_level")

        cdata = data
        data_length = len(data)
        cmethod = constants.COMPRESSION_TYPE_NONE

        minRatio = self.getOption("compression_minimal_ratio", 0.05)

        minSize = self.getOption("compression_minimal_size")
        if minSize < 0:
            # AutoSize?
            minSize = 0

        if data_length <= minSize and not forced:
            return cdata, cmethod

        cdata_length = data_length
        min_len = data_length

        self.getLogger().debug("BaseCompressTool::_compressData - data length = %r", data_length)

        for m in self._methods:
            comp = self._compressors[ m ]
            self.getLogger().debug("BaseCompressTool::_compressData - try method = %r", m)
            if comp.isDataMayBeCompressed(data, data_length):
                # Prefer custom level options
                useLevel = comp.getCustomCompressionLevel()
                if not useLevel:
                    useLevel = level
                self.getLogger().debug("BaseCompressTool::_compressData - try level %r", useLevel)
                _cdata = comp.compressData(data, useLevel)
                cdata_length = len(_cdata)
                self.getLogger().debug("BaseCompressTool::_compressData - cdata length = %r", cdata_length)
                if min_len > cdata_length:
                    self.getLogger().debug("BaseCompressTool::_compressData - good try - compressed data is less than before")
                    min_len = cdata_length
                    cdata = _cdata
                    cmethod = m

        cratio = (data_length - cdata_length) * 1.0 / data_length

        self.getLogger().debug("BaseCompressTool::_compressData - RESULT - cratio = %.3f, minRatio = %.3f, cmethod = %r", cratio, minRatio, cmethod)

        if data_length <= min_len and not forced:
            cdata = data
            cmethod = constants.COMPRESSION_TYPE_NONE
        elif cratio < minRatio and not forced:
            cdata = data
            cmethod = constants.COMPRESSION_TYPE_NONE

        return cdata, cmethod

    def compressData(self, dataToCompress):
        """
        Compress data and returns back

        @param dataToCompress: dict { hash id: bytes data }

        @return tuple ( hash id, (compressed data (bytes), compresion method (string) ) )
        """

        start_time = time()

        isNoneOnly = not self._methods or (len(self._methods) == 1 and constants.COMPRESSION_TYPE_NONE in self._methods)

        for hash_id, data in dataToCompress.items():
            # Clean pass - no compress at all
            if isNoneOnly:
                yield hash_id, (data, constants.COMPRESSION_TYPE_NONE,)
            else:
                yield hash_id, self._compressData(data)

        self.time_spent_compressing = time() - start_time

        return

    def decompressData(self, method, data):
        """
        deCompress data and returns back

        @return bytes
        """
        if method != constants.COMPRESSION_TYPE_NONE:
            comp = self._compressors[ method ]
            return comp.decompressData(data)
        return data

    def isDeprecated(self, method):
        """
        Is (de)compression method deprecated and should not be used

        @return bool
        """
        comp = self._compressors[method]
        return comp.isDeprecated()

    pass
