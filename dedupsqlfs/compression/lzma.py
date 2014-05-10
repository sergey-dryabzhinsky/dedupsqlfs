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
        }

    def getNormCompressionOptions(self):
        return {
            "preset": 4,
        }

    def getBestCompressionOptions(self):
        return {
            "preset": 7,
        }

    def getCustomCompressionOptions(self):
        try:
            level = int(self._custom_comp_level)
            if level < 0:
                level = 0
            elif level > 9:
                level = 9
            opts = {
                "preset": level,
            }
        except:
            opts = False
            pass
        return opts

    pass
