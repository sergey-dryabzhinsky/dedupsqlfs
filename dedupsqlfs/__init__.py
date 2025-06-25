# -*- coding: utf8 -*-
# Documentation. {{{1

"""
This Python library implements a file system in user space using FUSE. It's
called DedupFS because the file system's primary feature is deduplication,
which enables it to store virtually unlimited copies of files because data
is only stored once.

In addition to deduplication the file system also supports transparent
compression using any of the compression methods zlib, bz2, lzma
and optionaly lzo, lz4, snappy, zstd.

These two properties make the file system ideal for backups: I'm currently
storing 250 GB worth of backups using only 8 GB of disk space.

The latest version is available at https://github.com/sergey-dryabzhinsky/dedupsqlfs

DedupFS is licensed under the MIT license.
Copyright 2010 Peter Odding <peter@peterodding.com>.
Copyright 2013-2020 Sergey Dryabzhinsky <sergey.dryabzhinsky@gmail.com>.
"""

__name__ = "DedupSQLfs"
# for fuse mount
__fsname__ = "dedupsqlfs"
__fsversion__ = "3.8"
# Future 1.3
__version__ = "1.2.955-dev"

# Check the Python version, warn the user if untested.
import sys

if sys.version_info[0] < 3 or \
        (sys.version_info[0] == 3 and sys.version_info[1] < 4):
    msg = "Warning: %s(%s, %s) has been tested on Python 3.4+, while you're running Python %d.%d!\n"
    sys.stderr.write(msg % (__name__, __fsversion__, __version__,
                            int(sys.version_info[0]), int(sys.version_info[1])
                           ))

# Do not abuse GC - we generate alot objects
import gc
if hasattr(gc, "set_threshold"):
    gc.set_threshold(100000, 2000, 200)
