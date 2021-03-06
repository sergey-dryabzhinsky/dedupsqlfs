# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for Snappy compression helper
"""

from dedupsqlfs.compression import BaseCompression

class SnappyCompression(BaseCompression):

    _method_name = "snappy"

    _minimal_size = 17

    _has_comp_level_options = False

    pass
