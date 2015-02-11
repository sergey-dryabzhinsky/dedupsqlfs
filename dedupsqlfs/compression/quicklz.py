# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for Snappy compression helper
"""

from dedupsqlfs.compression import BaseCompression

class QuickLzCompression(BaseCompression):

    _method_name = "quicklz"

    _minimal_size = 17

    _has_comp_level_options = False

    pass
