# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Base class for compression helper
"""

from dedupsqlfs.lib import constants

class BaseCompression:

    _method_name = None

    _minimal_size = 128

    _has_comp_level_options = False

    _module = None


    def __init__(self):
        pass

    def _get_module(self):
        if self._method_name is None:
            raise AttributeError("Attribute _method_name must be set!")
        if not self._module:
            self._module = __import__(self._method_name)
        return self._module


    def _get_comp_func(self):
        return self._get_module().compress

    def _get_decomp_func(self):
        return self._get_module().decompress

    def hasCompressionLevelOptions(self):
        return self._has_comp_level_options

    def getFastCompressionOptions(self):
        return {}

    def getNormCompressionOptions(self):
        return {}

    def getBestCompressionOptions(self):
        return {}

    def getCompressionLevelOptions(self, level=None):
        opts = {}
        if level == constants.COMPRESSION_LEVEL_FAST:
            opts = self.getFastCompressionOptions()
        if level == constants.COMPRESSION_LEVEL_NORM:
            opts = self.getNormCompressionOptions()
        if level == constants.COMPRESSION_LEVEL_BEST:
            opts = self.getBestCompressionOptions()
        return opts

    def isDataNeedCompression(self, data):
        data_len = len(data)
        if self._minimal_size > data_len:
            return False
        return True

    def compressData(self, data, comp_level=None):
        """
        @param  data: Data
        @type   data: bytes

        @param  comp_level: compression level - None (default), fast, normal, best
        @type   comp_level: str
        """
        if not self.isDataNeedCompression(data):
            return data

        if self.hasCompressionLevelOptions():
            opts = self.getCompressionLevelOptions(comp_level)
            if opts is dict and opts:
                return self._get_comp_func()(data, **opts)
            if opts is tuple and opts:
                return self._get_comp_func()(data, *opts)
            else:
                return self._get_comp_func()(data)
        else:
            return self._get_comp_func()(data)

    def decompressData(self, cdata):
        return self._get_decomp_func()(cdata)

    pass
