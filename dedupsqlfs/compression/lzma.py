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
            "filters" : [
                {
                    "id": self._get_module().FILTER_DELTA,
                    "dist": 1
                },
                {
                    "id": self._get_module().FILTER_LZMA2,
                    "preset": 1 | self._get_module().PRESET_DEFAULT,
                    "dict_size": 1024*1024,
                    "nice_len": 64
                },
            ]
        }

    def getNormCompressionOptions(self):
        return {
            "filters" : [
                {
                    "id": self._get_module().FILTER_DELTA,
                    "dist": 3
                },
                {
                    "id": self._get_module().FILTER_LZMA2,
                    "preset": 4 | self._get_module().PRESET_DEFAULT,
                    "dict_size": 64*1024*1024,
                    "nice_len": 128
                },
            ]
        }

    def getBestCompressionOptions(self):
        return {
            "filters" : [
                {
                    "id": self._get_module().FILTER_DELTA,
                    "dist": 5
                },
                {
                    "id": self._get_module().FILTER_LZMA2,
                    "preset": 7 | self._get_module().PRESET_EXTREME,
                    "dict_size": 256*1024*1024,
                    "nice_len": 256
                },
            ]
        }

    pass
