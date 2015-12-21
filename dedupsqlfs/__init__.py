# -*- coding: utf8 -*-
# Documentation. {{{1

"""
This Python library implements a file system in user space using FUSE. It's
called DedupFS because the file system's primary feature is deduplication,
which enables it to store virtually unlimited copies of files because data
is only stored once.

In addition to deduplication the file system also supports transparent
compression using any of the compression methods zlib, bz2, lzma
and optionaly lzo, lz4, snappy.

These two properties make the file system ideal for backups: I'm currently
storing 250 GB worth of backups using only 8 GB of disk space.

The latest version is available at https://github.com/sergey-dryabzhinsky/dedupsqlfs

DedupFS is licensed under the MIT license.
Copyright 2010 Peter Odding <peter@peterodding.com>.
Copyright 2013-2015 Sergey Dryabzhinsky <sergey.dryabzhinsky@gmail.com>.
"""

__name__ = "DedupSQLfs"
__fsversion__ = "3.1"
__version__ = "1.2.18"

# Check the Python version, warn the user if untested.
import sys

if sys.version_info[0] < 3 or \
        (sys.version_info[0] == 3 and sys.version_info[1] < 2):
    msg = "Warning: %s(%s, $s) has only been tested on Python 3.2, while you're running Python %d.%d!\n"
    sys.stderr.write(msg % (__name__, __fsversion__, __version__, sys.version_info[0], sys.version_info[1]))

# Do not abuse GC - wee generate alot objects
import gc
gc.set_threshold(100000, 2000, 200)
