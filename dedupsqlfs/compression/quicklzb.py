# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for QuickLzB(est) compression helper
"""

from dedupsqlfs.compression import BaseCompression

class QuickLzBCompression(BaseCompression):

    _method_name = "quicklzb"

    _minimal_size = 17

    _has_comp_level_options = False

    pass
