# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for LZ4 compression helper
"""

from dedupsqlfs.compression import BaseCompression

class Lz4Compression(BaseCompression):

    _method_name = "lz4"

    _minimal_size = 15

    _has_comp_level_options = False

    pass
