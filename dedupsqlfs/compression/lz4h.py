# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for LZ4 compression helper High Level
"""

from dedupsqlfs.compression import BaseCompression

class Lz4hCompression(BaseCompression):

    _method_name = "lz4"

    _minimal_size = 15

    _has_comp_level_options = False

    def _init_module(self):
        if not self._module:
            self._module = __import__(self._method_name)
            self._func_comp = getattr(self._module, "compressHC")
            self._func_decomp = getattr(self._module, "decompress")
        return

    pass
