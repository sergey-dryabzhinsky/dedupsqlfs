# -*- coding: utf8 -*-
"""
@author Sergey Dryabzhinsky
"""

from dedupsqlfs.compression import BaseCompression


class Lz4r07Compression(BaseCompression):
    """
    Class for LZ4 compression helper for v0.7
    """

    _method_name = "lz4r07"

    _minimal_size = 15

    _has_comp_level_options = False

    _deprecated = True

    def isDataMayBeCompressed(self, data):
        """
        Disallow compression
        It's deprecated version

        @param data:
        @return:
        """
        return False

    pass
