# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for LZMA compression helper
"""

from dedupsqlfs.compression import BaseCompression

class LzmaCompression(BaseCompression):

    _method_name = "lzma"

    _minimal_size = 69

    _has_comp_level_options = True

    def getFastCompressionOptions(self):
        return {
            "preset": 1,
            "filters" : [
                {
                    "id": self._module.FILTER_DELTA,
                    "dist": 1
                },
                {
                    "id": self._module.FILTER_LZMA2,
                    "preset": 1,
                    "mode": self._module.MODE_FAST,
                    "dict_size": 1024*1024,
                    "nice_len": 64
                },
            ]
        }

    def getNormCompressionOptions(self):
        return {
            "preset": 6,
            "filters" : [
                {
                    "id": self._module.FILTER_DELTA,
                    "dist": 2
                },
                {
                    "id": self._module.FILTER_LZMA2,
                    "preset": 6,
                    "dict_size": 64*1024*1024,
                    "nice_len": 128
                },
            ]
        }

    def getBestCompressionOptions(self):
        return {
            "preset": 9,
            "filters" : [
                {
                    "id": self._module.FILTER_DELTA,
                    "dist": 5
                },
                {
                    "id": self._module.FILTER_LZMA2,
                    "preset": 9,
                    "dict_size": 256*1024*1024,
                    "nice_len": 256
                },
            ]
        }

    pass
