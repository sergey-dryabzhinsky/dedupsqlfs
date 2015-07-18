# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for QuickLzM(edium) compression helper
"""

from dedupsqlfs.compression import BaseCompression

class QuickLzMCompression(BaseCompression):

    _method_name = "quicklzm"

    _minimal_size = 18

    _has_comp_level_options = False

    pass
