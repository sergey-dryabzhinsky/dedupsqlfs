# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for Zstd compression helper
New version 1.0+
"""

from dedupsqlfs.compression import BaseCompression

class ZstdCompression(BaseCompression):

    _method_name = "zstd"

    _minimal_size = 18

    _has_comp_level_options = True

    def getFastCompressionOptions(self):
        return ( 1, )

    def getNormCompressionOptions(self):
        return ( 9, )

    def getBestCompressionOptions(self):
        return ( 18, )

    def getDefaultCompressionOptions(self):
        return ( 3, )

    def getCustomCompressionOptions(self):
        try:
            level = int(self._custom_comp_level)
            if level < 1:
                level = 1
            elif level > 22:
                level = 22
            opts = (level, )
        except:
            opts = False
            pass
        return opts

    pass
