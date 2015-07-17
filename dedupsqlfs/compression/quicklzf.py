# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for QuickLzF(ast) compression helper
"""

from dedupsqlfs.compression import BaseCompression

class QuickLzFCompression(BaseCompression):

    _method_name = "quicklzf"

    _minimal_size = 18

    _has_comp_level_options = False

    pass
