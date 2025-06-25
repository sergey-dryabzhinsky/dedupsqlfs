# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for Zstd compression helper
New version 1.0+

Since libzstd-1.3.4 - support ultra-fast levels: -100..-1
"""

from dedupsqlfs.compression import BaseCompression

class ZstdCompression(BaseCompression):

    # new bundled module name
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
            if level < -100:
                level = -100
            elif level > 20:
                level = 20
            opts = (level, )
        except:
            opts = False
            pass
        return opts

    pass
