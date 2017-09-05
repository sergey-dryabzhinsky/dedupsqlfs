# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for Old Zstd v0.0.1 compression helper
"""

from dedupsqlfs.compression import BaseCompression

class Zstd001Compression(BaseCompression):

    _method_name = "zstd001"

    _minimal_size = 26

    _has_comp_level_options = False

    _deprecated = True

    def isDataMayBeCompressed(self, data, data_len):
        """
        Disallow compression
        It's deprecated version of ZSTD

        @param data:
        @return:
        """
        return False
