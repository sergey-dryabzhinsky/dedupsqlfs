# -*- coding: utf8 -*-
"""
Constants for modules
"""

__author__ = 'sergey'

COMPRESSION_SUPPORTED=('lzo', 'zlib', 'bz2', 'lzma', 'snappy', 'lz4', 'lz4h',
                       'quicklz', 'quicklzf', 'quicklzm', 'quicklzb', 'zstd',)
COMPRESSION_TYPE_BEST="auto_best"
COMPRESSION_TYPE_CUSTOM="custom"
COMPRESSION_TYPE_NONE="none"

COMPRESSION_LEVEL_DEFAULT="default"
COMPRESSION_LEVEL_FAST="fast"
COMPRESSION_LEVEL_NORM="normal"
COMPRESSION_LEVEL_BEST="best"

ROOT_SUBVOLUME_NAME=b"@root"

BLOCK_SIZE_MIN=512
BLOCK_SIZE_MAX=16*1024*1024     # 16Mb
