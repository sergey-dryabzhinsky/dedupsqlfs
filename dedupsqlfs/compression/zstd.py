# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for Zstd compression helper
"""

from dedupsqlfs.compression import BaseCompression

class ZstdCompression(BaseCompression):

    _method_name = "zstd"

    _minimal_size = 22

    _has_comp_level_options = False

    pass
