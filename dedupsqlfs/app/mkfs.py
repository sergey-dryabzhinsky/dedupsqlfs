# -*- coding: utf8 -*-
"""
@todo: Update argument parser options
"""

# Imports. {{{1

import sys

# Try to load the required modules from Python's standard library.
try:
    import os
    import argparse
    from time import time
    import hashlib
except ImportError as e:
    msg = "Error: Failed to load one of the required Python modules! (%s)\n"
    sys.stderr.write(msg % str(e))
    sys.exit(1)

from dedupsqlfs.log import logging
from dedupsqlfs.lib import constants
from dedupsqlfs.db import check_engines
import dedupsqlfs

def mkfs(options, compression_methods=None, hash_functions=None):
    from dedupsqlfs.fuse.dedupfs import DedupFS
    from dedupsqlfs.fuse.operations import DedupOperations

    ops = None
    ret = 0
    try:
        ops = DedupOperations()

        _fuse = DedupFS(
            ops, None,
            options,
            fsname="dedupsqlfs", allow_root=True)

        _fuse.saveCompressionMethods(compression_methods)

        for modname in compression_methods:
            _fuse.appendCompression(modname)

        _fuse.setOption("gc_umount_enabled", False)
        _fuse.setOption("gc_vacuum_enabled", False)
        _fuse.setOption("gc_enabled", False)

        _fuse.operations.init()
        _fuse.operations.destroy()
    except Exception:
        import traceback
        print(traceback.format_exc())
        ret = -1
    if ops:
        ops.getManager().close()

    return ret

def main(): # {{{1
    """
    This function enables using dedupsqlfs.py as a shell script that creates FUSE
    mount points. Execute "dedupsqlfs -h" for a list of valid command line options.
    """

    logger = logging.getLogger("mkfs.dedupsqlfs/main")
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler(sys.stderr))

    parser = argparse.ArgumentParser(
        prog="%s/%s mkfs/%s" % (dedupsqlfs.__name__, dedupsqlfs.__version__, dedupsqlfs.__fsversion__),
        conflict_handler="resolve")

    # Register some custom command line options with the option parser.
    option_stored_in_db = " (this option is only useful when creating a new database, because your choice is stored in the database and can't be changed after that)"

    parser.add_argument('-h', '--help', action='help', help="show this help message followed by the command line options defined by the Python FUSE binding and exit")
    parser.add_argument('-v', '--verbose', action='count', dest='verbosity', default=0, help="increase verbosity: 0 - error, 1 - warning, 2 - info, 3 - debug, 4 - verbose")
    parser.add_argument('--log-file', dest='log_file', help="specify log file location")
    parser.add_argument('--log-file-only', dest='log_file_only', action='store_true',
                        help="Don't send log messages to stderr.")

    parser.add_argument('--data', dest='data', metavar='DIRECTORY', default="~/data", help="Specify the base location for the files in which metadata and blocks data is stored. Defaults to ~/data")
    parser.add_argument('--name', dest='name', metavar='DATABASE', default="dedupsqlfs", help="Specify the name for the database directory in which metadata and blocks data is stored. Defaults to dedupsqlfs")
    parser.add_argument('--temp', dest='temp', metavar='DIRECTORY', help="Specify the location for the files in which temporary data is stored. By default honour TMPDIR environment variable value.")
    parser.add_argument('-b', '--block-size', dest='block_size', metavar='BYTES', default=1024*128, type=int, help="Specify the maximum block size in bytes" + option_stored_in_db + ". Defaults to 128kB.")

    parser.add_argument('--memory-limit', dest='memory_limit', action='store_true', help="Use some lower values for less memory consumption.")

    parser.add_argument('--cpu-limit', dest='cpu_limit', metavar='NUMBER', default=0, type=int, help="Specify the maximum CPU count to use in multiprocess compression. Defaults to 0 (auto).")


    engines, msg = check_engines()
    if not engines:
        logger.error("No storage engines available! Please install sqlite or pymysql python module!")
        return 1

    parser.add_argument('--storage-engine', dest='storage_engine', metavar='ENGINE', choices=engines, default=engines[0],
                        help=msg)

    if "mysql" in engines:

        from dedupsqlfs.db.mysql import get_table_engines

        table_engines = get_table_engines()

        msg = "One of MySQL table engines: "+", ".join(table_engines)+". Default: %r. Aria and TokuDB engine can be used only with MariaDB or Percona server." % table_engines[0]
        parser.add_argument('--table-engine', dest='table_engine', metavar='ENGINE',
                            choices=table_engines, default=table_engines[0],
                            help=msg)

    parser.add_argument('--no-cache', dest='use_cache', action='store_false', help="Don't use cache in memory and delayed write to storage.")
    parser.add_argument('--no-transactions', dest='use_transactions', action='store_false', help="Don't use transactions when making multiple related changes, this might make the file system faster or slower (?).")
    parser.add_argument('--nosync', dest='synchronous', action='store_false', help="Disable SQLite's normal synchronous behavior which guarantees that data is written to disk immediately, because it slows down the file system too much (this means you might lose data when the mount point isn't cleanly unmounted).")

    # Dynamically check for supported hashing algorithms.
    msg = "Specify the hashing algorithm that will be used to recognize duplicate data blocks: one of %s"
    hash_functions = list({}.fromkeys([h.lower() for h in hashlib.algorithms_available]).keys())
    hash_functions.sort()
    msg %= ', '.join('%r' % fun for fun in hash_functions)
    msg += ". Defaults to 'sha1'."
    parser.add_argument('--hash', dest='hash_function', metavar='FUNCTION', choices=hash_functions, default='sha1', help=msg)

    # Dynamically check for supported compression methods.
    compression_methods = [constants.COMPRESSION_TYPE_NONE]
    compression_methods_cmd = [constants.COMPRESSION_TYPE_NONE]
    for modname in constants.COMPRESSION_SUPPORTED:
        try:
            module = __import__(modname)
            if hasattr(module, 'compress') and hasattr(module, 'decompress'):
                compression_methods.append(modname)
                if modname not in constants.COMPRESSION_READONLY:
                    compression_methods_cmd.append(modname)
        except ImportError:
            pass
    if len(compression_methods) > 1:
        compression_methods_cmd.append(constants.COMPRESSION_TYPE_BEST)
        compression_methods_cmd.append(constants.COMPRESSION_TYPE_CUSTOM)

    msg = "Enable compression of data blocks using one of the supported compression methods: one of %s"
    msg %= ', '.join('%r' % mth for mth in compression_methods_cmd)
    msg += ". Defaults to %r." % constants.COMPRESSION_TYPE_NONE
    msg += " You can use <method>:<level> syntax, <level> can be integer or value from --compression-level."
    if len(compression_methods_cmd) > 1:
        msg += " %r will try all compression methods and choose one with smaller result data." % constants.COMPRESSION_TYPE_BEST
        msg += " %r will try selected compression methods (--custom-compress) and choose one with smaller result data." % constants.COMPRESSION_TYPE_CUSTOM
    msg += "\nDefaults to %r." % constants.COMPRESSION_TYPE_NONE

    parser.add_argument('--compress', dest='compression', metavar='METHOD', action="append",
                        default=[constants.COMPRESSION_TYPE_NONE], help=msg)

    msg = "Enable compression of data blocks using one or more of the supported compression methods: %s"
    msg %= ', '.join('%r' % mth for mth in compression_methods_cmd[:-2])
    msg += ". To use two or more methods select this option in command line for each compression method."
    msg += " You can use <method>=<level> syntax, <level> can be integer or value from --compression-level."

    parser.add_argument('--force-compress', dest='compression_forced', action="store_true", help="Force compression even if resulting data is bigger than original.")
    parser.add_argument('--minimal-compress-size', dest='compression_minimal_size', metavar='BYTES', type=int, default=1024, help="Minimal block data size for compression. Defaults to 1024 bytes. Value -1 means auto - per method absolute minimum. Not compress if data size is less then BYTES long. If not forced to.")
    parser.add_argument('--minimal-compress-ratio', dest='compression_minimal_ratio', metavar='RATIO', type=float, default=0.05, help="Minimal data compression ratio. Defaults to 0.05 (5%%). Do not compress if ratio is less than RATIO. If not forced to.")

    levels = (constants.COMPRESSION_LEVEL_DEFAULT, constants.COMPRESSION_LEVEL_FAST, constants.COMPRESSION_LEVEL_NORM, constants.COMPRESSION_LEVEL_BEST)

    parser.add_argument('--compression-level', dest='compression_level', metavar="LEVEL", default=constants.COMPRESSION_LEVEL_DEFAULT,
                        help="Compression level ratio: one of %s; or INT. Defaults to %r. Not all methods support this option." % (
                            ', '.join('%r' % lvl for lvl in levels), constants.COMPRESSION_LEVEL_DEFAULT
                        ))

    # Dynamically check for profiling support.
    try:
        # Using __import__() here because of pyflakes.
        for p in 'cProfile', 'pstats': __import__(p)
        parser.add_argument('--profile', action='store_true', default=False, help="Use the Python modules cProfile and pstats to create a profile of time spent in various function calls and print out a table of the slowest functions at exit (of course this slows everything down but it can nevertheless give a good indication of the hot spots).")
    except ImportError:
        logger.warning("No profiling support available, --profile option disabled.")
        logger.warning("If you're on Ubuntu try 'sudo apt-get install python-profiler'.")

    args = parser.parse_args()

    if args.profile:
        sys.stderr.write("Enabling profiling..\n")
        import cProfile, pstats
        profile = '.dedupsqlfs.cprofile-%i' % time()
        profiler = cProfile.Profile()
        result = profiler.runcall(mkfs, args, compression_methods, hash_functions)
        profiler.dump_stats(profile)
        sys.stderr.write("\n Profiling statistics:\n\n")
        s = pstats.Stats(profile)
        s.sort_stats('calls').print_stats(0.1)
        s.sort_stats('cumtime').print_stats(0.1)
        s.sort_stats('tottime').print_stats(0.1)
        os.unlink(profile)
    else:
        result = mkfs(args, compression_methods, hash_functions)

    return result

# vim: ts=4 sw=4 et
