# -*- coding: utf8 -*-

__author__ = 'sergey'

"""
Class for Old Zstd v0.0.1 compression helper
"""

from dedupsqlfs.compression import BaseCompression

class Zstd001Compression(BaseCompression):

    _method_name = "zstd001"

    _minimal_size = 26

    _has_comp_level_options = False

    _func_decomp_new = None

    def _init_module(self):
        super()._init_module()

        module = __import__('zstd')
        self._func_decomp_new = getattr(module, "decompress")
        return


    def isDataMayBeCompressed(self, data):
        """
        Disallow compression
        It's deprecated version of ZSTD

        @param data:
        @return:
        """
        return False

    def decompressData(self, cdata):
        """
        Try to decompress by old version
        If can't - try new version

        Thats because maybe some data was compressed before migrations arraived

        @param cdata:
        @return:
        """
        try:
            data = super().decompressData(cdata)
        except (Exception):
            data = False
            pass

        if data is False:
            data = self._func_decomp_new(cdata)

        return data
