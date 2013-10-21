# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for ZLIB compression helper
"""

from dedupsqlfs.compression import BaseCompression

class ZlibCompression(BaseCompression):

    _method_name = "zlib"

    _minimal_size = 12

    _has_comp_level_options = True

    def getFastCompressionOptions(self):
        return ( 1, )

    def getNormCompressionOptions(self):
        return ( 6, )

    def getBestCompressionOptions(self):
        return ( 9, )

    pass
