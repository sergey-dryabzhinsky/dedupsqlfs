# -*- coding: utf8 -*-
"""
Constants for modules
"""

__author__ = 'sergey'

COMPRESSION_SUPPORTED=('lzo', 'zlib', 'bz2', 'lzma', 'snappy', 'lz4', 'lz4h',
                       'quicklz', 'quicklzf', 'quicklzm', 'quicklzb',
                       'zstd', 'zstd001', 'zstd036', 'zstd047', 'zstd061', 'lz4r07')
COMPRESSION_READONLY=("quicklz", "zstd001", "zstd036", "zstd047", 'zstd061')
COMPRESSION_TYPE_BEST="auto_best"
COMPRESSION_TYPE_CUSTOM="custom"
COMPRESSION_TYPE_NONE="none"

COMPRESSION_LEVEL_DEFAULT="default"
COMPRESSION_LEVEL_FAST="fast"
COMPRESSION_LEVEL_NORM="normal"
COMPRESSION_LEVEL_BEST="best"

# For .sqlite3 files
COMPRESSION_PROGS = {
    "pigz": {"ext": ".gz", "comp": ["-4q"], "decomp": ["-dq"], "priority": 10, "can-comp": True, "can-decomp": True},
    "gzip": {"ext": ".gz", "comp": ["-4q"], "decomp": ["-dq"], "priority": 1, "can-comp": True, "can-decomp": True},

    "pbzip2": {"ext": ".bz2", "comp": ["-1"], "decomp": ["-d"], "priority": 10, "can-comp": True, "can-decomp": True},
    "bzip2": {"ext": ".bz2", "comp": ["-1q"], "decomp": ["-dq"], "priority": 1, "can-comp": True, "can-decomp": True},

    "pxz": {"ext": ".xz", "comp": ["-2"], "decomp": [], "priority": 10, "can-comp": True, "can-decomp": False},
    "xz": {"ext": ".xz", "comp": ["-2q"], "decomp": ["-dq"], "priority": 1, "can-comp": True, "can-decomp": True},

    "lzop": {"ext": ".lzo", "comp": ["-3q"], "decomp": ["-dq"], "priority": 1, "can-comp": True, "can-decomp": True},

    # As of 0.8 -- need to be forced to remove compressed file
    "zstd": {"ext": ".zst", "comp": ["-4q", "--rm"], "decomp": ["-dq", "--rm"], "priority": 1, "can-comp": True, "can-decomp": True},
}
COMPRESSION_PROGS_EXT = {
    ".gz": ("pigz", "gzip",),
    ".bz2": ("pbzip2", "bzip2",),
    ".xz": ("pxz", "xz",),
    ".lzo": ("lzop",),
    ".zst": ("zstd",)
}
COMPRESSION_PROGS_DEFAULT = "gzip"


ROOT_SUBVOLUME_NAME=b"@root"

BLOCK_SIZE_MIN=512
BLOCK_SIZE_MAX=16*1024*1024     # 16Mb
