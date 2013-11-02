# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for BZ2 compression helper
"""

from dedupsqlfs.compression import BaseCompression

class Bz2Compression(BaseCompression):

    _method_name = "bz2"

    _minimal_size = 40

    _has_comp_level_options = True

    def getFastCompressionOptions(self):
        return ( 1, )

    def getNormCompressionOptions(self):
        return ( 6, )

    def getBestCompressionOptions(self):
        return ( 9, )

    pass