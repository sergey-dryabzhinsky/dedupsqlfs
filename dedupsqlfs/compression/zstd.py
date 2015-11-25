# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for Zstd compression helper
New version 0.3+
"""

from dedupsqlfs.compression import BaseCompression

class ZstdCompression(BaseCompression):

    _method_name = "zstd"

    _minimal_size = 31

    _has_comp_level_options = True

    _func_decomp_old = None

    def _init_module(self):
        super()._init_module()

        module = __import__('zstd001')
        self._func_decomp_old = getattr(module, "decompress")
        return

    def getFastCompressionOptions(self):
        return ( 1, )

    def getNormCompressionOptions(self):
        return ( 7, )

    def getBestCompressionOptions(self):
        return ( 9, )

    def getDefaultCompressionOptions(self):
        return ( 4, )

    def getCustomCompressionOptions(self):
        try:
            level = int(self._custom_comp_level)
            if level < 1:
                level = 1
            elif level > 20:
                level = 20
            opts = (level, )
        except:
            opts = False
            pass
        return opts

    def decompressData(self, cdata):
        """
        Try to decompress by new version
        If can't - try old version

        Thats because maybe some data was compressed before migrations arraived

        @param cdata:
        @return:
        """
        try:
            data = super().decompressData(cdata)
        except Exception as e:
            if str(e).find("wrongMagicNumber") != -1:
                raise e
            data = False
            pass

        if data is False:
            data = self._func_decomp_old(cdata)

        return data

    pass
