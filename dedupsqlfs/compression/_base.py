# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Base class for compression helper
"""

import sys
from dedupsqlfs.lib import constants

class BaseCompression:

    _method_name = None

    _minimal_size = 128

    _has_comp_level_options = False

    _custom_comp_level = None

    _module = None
    _func_comp = None
    _func_decomp = None

    _deprecated = False

    def __init__(self):
        if self._method_name is None:
            raise AttributeError("Attribute _method_name must be set!")
        self._init_module()
        pass

    def _init_module(self):
        if not self._module:
            if self._method_name in sys.modules:
                del sys.modules[ self._method_name ]
            self._module = __import__(self._method_name)
            self._func_comp = getattr(self._module, "compress")
            self._func_decomp = getattr(self._module, "decompress")
        return

    def _get_comp_func(self):
        return self._func_comp

    def _get_decomp_func(self):
        return self._func_decomp

    def hasCompressionLevelOptions(self):
        return self._has_comp_level_options

    def setCustomCompressionLevel(self, level=None):
        self._custom_comp_level = level
        return self

    def getFastCompressionOptions(self):
        return False

    def getNormCompressionOptions(self):
        return False

    def getBestCompressionOptions(self):
        return False

    def getDefaultCompressionOptions(self):
        return False

    def getCustomCompressionOptions(self):
        return False

    def getCompressionLevelOptions(self, level=None):
        """
        @rtype: dict or tuple or bool
        """
        if level == constants.COMPRESSION_LEVEL_FAST:
            opts = self.getFastCompressionOptions()
        elif level == constants.COMPRESSION_LEVEL_NORM:
            opts = self.getNormCompressionOptions()
        elif level == constants.COMPRESSION_LEVEL_BEST:
            opts = self.getBestCompressionOptions()
        elif level == constants.COMPRESSION_LEVEL_DEFAULT:
            opts = self.getDefaultCompressionOptions()
        else:
            opts = self.getCustomCompressionOptions()
        return opts

    def isDataMayBeCompressed(self, data):
        data_len = len(data)
        if self._minimal_size > data_len:
            return False
        return True

    def isDeprecated(self):
        """
        Is (de)compression method deprecated and should not be used
        @return: bool
        """
        return self._deprecated

    def compressData(self, data, comp_level=None):
        """
        @param  data: Data
        @type   data: bytes

        @param  comp_level: compression level - None (default), fast, normal, best
        @type   comp_level: str

        @return:    compressed data
        @rtype:     bytes
        """
        func = self._get_comp_func()
        if self.hasCompressionLevelOptions():
            opts = self.getCompressionLevelOptions(comp_level)
            if opts:
                if type(opts) is dict:
                    return func(data, **opts)
                elif type(opts) is tuple:
                    return func(data, *opts)
                else:
                    return func(data)
            else:
                return func(data)
        else:
            return func(data)

    def decompressData(self, cdata):
        func = self._get_decomp_func()
        return func(cdata)

    pass
