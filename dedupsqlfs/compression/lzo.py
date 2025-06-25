# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for LZO compression helper
"""

from dedupsqlfs.compression import BaseCompression

class LzoCompression(BaseCompression):

    _method_name = "__lzo"

    _minimal_size = 38

    _has_comp_level_options = False

    pass
