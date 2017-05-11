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

    _selected = None
    """
    @ivar _selected: is Method selected cache
    @type _selected: dict
    """

    time_spent_compressing = 0

    def __init__(self):
        self._compressors = {}
        self._options = {}
        self._selected = {}
        pass

    def checkCpuLimit(self):
        return 1

    def init(self):
        self.time_spent_compressing = 0
        return self

    def stop(self):
        return self

    def setOption(self, key, value):
        self._options[key] = value
        return self

    def getOption(self, key, default=None):
        return self._options.get(key, default)

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
        elif name == "zstd036":
            from dedupsqlfs.compression.zstd036 import Zstd036Compression
            self._compressors[name] = Zstd036Compression()
        elif name == "zstd047":
            from dedupsqlfs.compression.zstd047 import Zstd047Compression
            self._compressors[name] = Zstd047Compression()
        elif name == "zstd061":
            from dedupsqlfs.compression.zstd061 import Zstd061Compression
            self._compressors[name] = Zstd061Compression()
        elif name == "zstd001":
            from dedupsqlfs.compression.zstd001 import Zstd001Compression
            self._compressors[name] = Zstd001Compression()
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

        selected = self._selected.get(name)
        if selected is not None:
            return selected

        selected = False

        method = self.getOption("compression_method")

        if method not in (constants.COMPRESSION_TYPE_BEST, constants.COMPRESSION_TYPE_CUSTOM,):
            if method == name:
                selected = True
        else:
            methods = self._compressors.keys()
            if method == constants.COMPRESSION_TYPE_CUSTOM:
                methods = tuple(self.getOption("compression_custom"))
                methods += (constants.COMPRESSION_TYPE_NONE,)
            for m in methods:
                if m == name:
                    selected = True

        self._selected[name] = selected

        return selected

    def _compressData(self, data):
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
                comp = self._compressors[ method ]
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
                    comp = self._compressors[ m ]
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

    def compressData(self, dataToCompress):
        """
        Compress data and returns back

        @param dataToCompress: dict { hash id: bytes data }

        @return dict { hash id: (compressed data (bytes), compresion method (string) ) }
        """

        start_time = time()

        for hash_id, data in dataToCompress.items():
            yield  hash_id, self._compressData(data)

        self.time_spent_compressing = time() - start_time

        return

    def decompressData(self, method, data):
        """
        deCompress data and returns back

        @return bytes
        """
        comp = self._compressors[ method ]
        return comp.decompressData(data)

    def isDeprecated(self, method):
        """
        Is (de)compression method deprecated and should not be used

        @return bool
        """
        comp = self._compressors[method]
        return comp.isDeprecated()

    pass
