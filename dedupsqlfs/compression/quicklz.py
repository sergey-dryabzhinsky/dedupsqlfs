# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for QuickLz compression helper

@deprecated
"""

from dedupsqlfs.compression import BaseCompression

class QuickLzCompression(BaseCompression):

    _method_name = "quicklz"

    _minimal_size = 17

    _has_comp_level_options = False

    def isDataMayBeCompressed(self, data):
        """
        Disallow compression
        It's deprecated version

        @param data:
        @return:
        """
        return False

    pass
