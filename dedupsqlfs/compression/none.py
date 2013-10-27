# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for NONE compression helper
"""

from dedupsqlfs.compression import BaseCompression

class NoneCompression(BaseCompression):

    _method_name = "none"

    _minimal_size = 0

    _has_comp_level_options = False

    def _get_module(self):
        return None

    def _noop(self, data):
        return data

    def _get_comp_func(self):
        return self._noop

    def _get_decomp_func(self):
        return self._noop

