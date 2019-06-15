# -*- coding: utf8 -*-
"""
Constants for modules
"""

__author__ = 'sergey'

COMPRESSION_SUPPORTED=('lzo', 'zlib', 'bz2', 'xz', 'snappy',
                       'lz4', 'lz4h', 'lz4r07',
                       'quicklz', 'quicklzf', 'quicklzm', 'quicklzb',
                       'brotli',
                       'zstd',
                       'zstd001', 'zstd036', 'zstd047', 'zstd061',
                       )
COMPRESSION_READONLY=("quicklz", "zstd001", "zstd036", "zstd047", 'zstd061', 'lz4r07')
COMPRESSION_TYPE_BEST="all_best"
COMPRESSION_TYPE_DEFAULT="all"
COMPRESSION_TYPE_FAST="all_fast"
COMPRESSION_TYPE_CUSTOM="custom"
COMPRESSION_TYPE_NONE="none"

COMPRESSION_LEVEL_DEFAULT="default"
COMPRESSION_LEVEL_FAST="fast"
COMPRESSION_LEVEL_NORM="normal"
COMPRESSION_LEVEL_BEST="best"

# Subset of hashlib simple funcs
WANTED_HASH_FUCTIONS = {'md4', 'md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512', 'whirlpool', 'ripemd160'}
HAS_FUNCTION_DEFAULT = 'md5'

# For .sqlite3 files
COMPRESSION_PROGS = {
    "pigz": {"ext": ".gz", "comp": ["-4q"], "decomp": ["-dq"], "priority": 10, "can-comp": True, "can-decomp": True},
    "gzip": {"ext": ".gz", "comp": ["-4q"], "decomp": ["-dq"], "priority": 1, "can-comp": True, "can-decomp": True},

    "pbzip2": {"ext": ".bz2", "comp": ["-1"], "decomp": ["-d"], "priority": 10, "can-comp": True, "can-decomp": True},
    "bzip2": {"ext": ".bz2", "comp": ["-1q"], "decomp": ["-dq"], "priority": 1, "can-comp": True, "can-decomp": True},

    "pxz": {"ext": ".xz", "comp": ["-2"], "decomp": [], "priority": 10, "can-comp": True, "can-decomp": False},
    "xz": {"ext": ".xz", "comp": ["-2q"], "decomp": ["-dq"], "priority": 1, "can-comp": True, "can-decomp": True},

    "lzop": {"ext": ".lzo", "comp": ["-3q"], "decomp": ["-dq"], "priority": 1, "can-comp": True, "can-decomp": True},

    "lz4": {"ext": ".lz4", "comp": ["-1q"], "decomp": ["-dq"], "priority": 1, "can-comp": True, "can-decomp": True},

    # As of 0.8 -- need to be forced to remove compressed file
    "pzstd": {"ext": ".zst", "comp": ["-6q", "--rm"], "decomp": ["-dq", "--rm"], "priority": 10, "can-comp": True, "can-decomp": True},
    "zstd": {"ext": ".zst", "comp": ["-6q", "--rm"], "decomp": ["-dq", "--rm"], "priority": 1, "can-comp": True, "can-decomp": True},
}
COMPRESSION_PROGS_EXT = {
    ".gz": ("pigz", "gzip",),
    ".bz2": ("pbzip2", "bzip2",),
    ".xz": ("pxz", "xz",),
    ".lzo": ("lzop",),
    ".lz4": ("lz4",),
    ".zst": ("pzstd", "zstd",)
}
COMPRESSION_PROGS_NONE = "none"
COMPRESSION_PROGS_DEFAULT = COMPRESSION_PROGS_NONE


ROOT_SUBVOLUME_NAME=b"@root"

BLOCK_SIZE_MIN=512
BLOCK_SIZE_DEFAULT=128*1024     # 128kb
BLOCK_SIZE_MAX=16*1024*1024     # 16Mb
