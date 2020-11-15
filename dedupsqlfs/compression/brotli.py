# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for Google Brotli compression helper
"""

from dedupsqlfs.compression import BaseCompression

class BrotliCompression(BaseCompression):

    _method_name = "brotli"

    _minimal_size = 19

    _has_comp_level_options = True

    def getFastCompressionOptions(self):
        return ( self._module.MODE_GENERIC, 0, )

    def getNormCompressionOptions(self):
        return ( self._module.MODE_GENERIC, 4, )

    def getBestCompressionOptions(self):
        return ( self._module.MODE_GENERIC, 9, )

    def getDefaultCompressionOptions(self):
        return ( self._module.MODE_GENERIC, 2, )

    def getCustomCompressionOptions(self):
        try:
            level = int(self._custom_comp_level)
            if level < 0:
                level = 0
            elif level > 11:
                level = 11
            opts = (self._module.MODE_GENERIC, level, )
        except:
            opts = False
            pass
        return opts

    pass
