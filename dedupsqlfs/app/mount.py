# -*- coding: utf8 -*-

# Imports. {{{1

import sys

# Try to load the required modules from Python's standard library.
try:
    import os
    import traceback
    import argparse
    import time
    import hashlib
    import logging
    from dedupsqlfs.lib import constants
except ImportError as e:
    msg = "Error: Failed to load one of the required Python modules! (%s)\n"
    sys.stderr.write(msg % str(e))
    sys.exit(1)


def fuse_mount(options, compression_methods=None, hash_functions=None):
    from dedupsqlfs.fuse.dedupfs import DedupFS
    from dedupsqlfs.fuse.operations import DedupOperations

    ops = None
    try:
        ops = DedupOperations()

        _fuse = DedupFS(
            ops, options.mountpoint,
            options,
            fsname="dedupsqlfs", allow_root=True)

        _fuse.saveCompressionMethods(compression_methods)

        for modname in compression_methods:
            _fuse.appendCompression(modname)

        ret = _fuse.main()
    except Exception as e:
        print(e)
        import traceback
        print(traceback.format_exc())
        ret = -1
    if ops:
        ops.getManager().stopMysqld()

    return ret

def main(): # {{{1
    """
    This function enables using dedupsqlfs.py as a shell script that creates FUSE
    mount points. Execute "dedupsqlfs -h" for a list of valid command line options.
    """

    logger = logging.getLogger("mount.dedupsqlfs/main")
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler(sys.stderr))

    parser = argparse.ArgumentParser(conflict_handler="resolve")

    # Register some custom command line options with the option parser.
    option_stored_in_db = " (this option is only useful when creating a new database, because your choice is stored in the database and can't be changed after that)"

    parser.add_argument('-h', '--help', action='help', help="show this help message followed by the command line options defined by the Python FUSE binding and exit")
    parser.add_argument('-v', '--verbose', action='count', dest='verbosity', default=0, help="increase verbosity")
    parser.add_argument('--log-file', dest='log_file', help="specify log file location")
    parser.add_argument('--data', dest='data', metavar='DIRECTORY', default="~/data", help="Specify the base location for the files in which metadata and blocks data is stored. Defaults to ~/data")
    parser.add_argument('--name', dest='name', metavar='DATABASE', default="dedupsqlfs", help="Specify the name for the database directory in which metadata and blocks data is stored. Defaults to dedupsqlfs")
    parser.add_argument('--temp', dest='temp', metavar='DIRECTORY', help="Specify the location for the files in which temporary data is stored. By default honour TMPDIR environment variable value.")
    parser.add_argument('--block-size', dest='block_size', metavar='BYTES', default=1024*128, type=int, help="Specify the maximum block size in bytes" + option_stored_in_db + ". Defaults to 128kB.")
    parser.add_argument('--mount-snapshot', dest='snapshot_mount', metavar='NAME', default=None, help="Use shapshot NAME as root fs.")
    parser.add_argument('--raw-root', dest='disable_subvolumes', action="store_true", help="Disable use of all snapshots and subvolumes. Unhide them into root of FS.")

    parser.add_argument('--memory-limit', dest='memory_limit', action='store_true', help="Use some lower values for less memory consumption.")

    parser.add_argument('--no-cache', dest='use_cache', action='store_false', help="Don't use cache in memory and delayed write to storage files.")
    parser.add_argument('--cache-timeout', dest='cache_timeout', metavar='NUMBER', type=int, default=10, help="Store data in memory for NUMBER of seconds. Defaults to 10 seconds.")
    parser.add_argument('--cache-meta-timeout', dest='cache_meta_timeout', metavar='NUMBER', type=int, default=20, help="Delay flush expired metadata for NUMBER of seconds. Defaults to 20 seconds.")
    parser.add_argument('--cache-block-write-timeout', dest='cache_block_write_timeout', metavar='NUMBER', type=int, default=5, help="Delay flush expired data from memory to storage for NUMBER of seconds. Defaults to 10 seconds.")
    parser.add_argument('--cache-block-write-size', dest='cache_block_write_size', metavar='BYTES', type=int,
                        default=256*1024*1024, help="Blocks write cache potential size in BYTES. Defines upper limit of blocks count in cache. Defaults to 256 MB (2048 default blocks).")
    parser.add_argument('--cache-block-read-timeout', dest='cache_block_read_timeout', metavar='NUMBER', type=int, default=10, help="Delay flush expired data from memory for NUMBER of seconds. Defaults to 5 seconds.")
    parser.add_argument('--cache-block-read-size', dest='cache_block_read_size', metavar='BYTES', type=int,
                        default=256*1024*1024, help="Blocks read cache potential size in BYTES. Defines upper limit of blocks count in cache. Defaults to 256 MB (2048 default blocks).")

    parser.add_argument('--no-transactions', dest='use_transactions', action='store_false', help="Don't use transactions when making multiple related changes, this might make the file system faster or slower (?).")
    parser.add_argument('--nosync', dest='synchronous', action='store_false', help="Disable SQLite's normal synchronous behavior which guarantees that data is written to disk immediately, because it slows down the file system too much (this means you might lose data when the mount point isn't cleanly unmounted).")

    parser.add_argument('--nogc-on-umount', dest='gc_umount_enabled', action='store_false', help="Disable garbage collection on umount operation (only do this when you've got disk space to waste or you know that nothing will be be deleted from the file system, which means little to no garbage will be produced).")
    parser.add_argument('--gc', dest='gc_enabled', action='store_true', help="Enable the periodic garbage collection because it degrades performance (only do this when you've got disk space to waste or you know that nothing will be be deleted from the file system, which means little to no garbage will be produced).")
    parser.add_argument('--gc-vacuum', dest='gc_vacuum_enabled', action='store_true', help="Enable data vacuum after the periodic garbage collection.")
    parser.add_argument('--gc-interval', dest='gc_interval', metavar="N", type=int, default=60, help="Call garbage callector after Nth seconds on FUSE operations, if GC enabled. Defaults to 60.")

    # Dynamically check for supported hashing algorithms.
    msg = "Specify the hashing algorithm that will be used to recognize duplicate data blocks: one of %s"
    hash_functions = list({}.fromkeys([h.lower() for h in hashlib.algorithms_available]).keys())
    hash_functions.sort()
    msg %= ', '.join('%r' % fun for fun in hash_functions)
    msg += ". Defaults to 'sha1'."
    parser.add_argument('--hash', dest='hash_function', metavar='FUNCTION', choices=hash_functions, default='sha1', help=msg)

    # Dynamically check for supported compression methods.
    compression_methods = [constants.COMPRESSION_TYPE_NONE]
    for modname in constants.COMPRESSION_SUPPORTED:
        try:
            module = __import__(modname)
            if hasattr(module, 'compress') and hasattr(module, 'decompress'):
                compression_methods.append(modname)
        except ImportError:
            pass
    if len(compression_methods) > 1:
        compression_methods.append(constants.COMPRESSION_TYPE_BEST)
        compression_methods.append(constants.COMPRESSION_TYPE_CUSTOM)

    msg = "Enable compression of data blocks using one of the supported compression methods: one of %s"
    msg %= ', '.join('%r' % mth for mth in compression_methods)
    msg += ". Defaults to %r." % constants.COMPRESSION_TYPE_NONE
    if len(compression_methods) > 1:
        msg += " %r will try all compression methods and choose one with smaller result data." % constants.COMPRESSION_TYPE_BEST
        msg += " %r will try selected compression methods (--custom-compress) and choose one with smaller result data." % constants.COMPRESSION_TYPE_CUSTOM

    parser.add_argument('--compress', dest='compression_method', metavar='METHOD', choices=compression_methods, default=constants.COMPRESSION_TYPE_NONE, help=msg)

    msg = "Enable compression of data blocks using one or more of the supported compression methods: %s"
    msg %= ', '.join('%r' % mth for mth in compression_methods[:-2])
    msg += ". To use two or more methods select this option in command line for each compression method."

    parser.add_argument('--custom-compress', dest='compression_custom', metavar='METHOD', choices=compression_methods[:-2], action="append", help=msg)
    parser.add_argument('--force-compress', dest='compression_forced', action="store_true", help="Force compression even if resulting data is bigger than original.")
    parser.add_argument('--minimal-compress-size', dest='compression_minimal_size', metavar='BYTES', type=int, default=-1, help="Minimal block data size for compression. Defaults to -1 bytes (auto). Do not do compression if not forced to.")
    parser.add_argument('--compression-level', dest='compression_level', metavar="LEVEL", default=constants.COMPRESSION_LEVEL_DEFAULT,
                        choices=(constants.COMPRESSION_LEVEL_DEFAULT, constants.COMPRESSION_LEVEL_FAST, constants.COMPRESSION_LEVEL_NORM, constants.COMPRESSION_LEVEL_BEST),
                        help="Compression level ratio: one of %s. Defaults to %r. Not all methods support this option." % (
                            ', '.join((constants.COMPRESSION_LEVEL_DEFAULT, constants.COMPRESSION_LEVEL_FAST, constants.COMPRESSION_LEVEL_NORM, constants.COMPRESSION_LEVEL_BEST)), constants.COMPRESSION_LEVEL_DEFAULT
                        ))
    # Do not want 'best' after help setup

    # Dynamically check for profiling support.
    try:
        # Using __import__() here because of pyflakes.
        for p in 'cProfile', 'pstats': __import__(p)
        parser.add_argument('--profile', action='store_true', default=False, help="Use the Python modules cProfile and pstats to create a profile of time spent in various function calls and print out a table of the slowest functions at exit (of course this slows everything down but it can nevertheless give a good indication of the hot spots).")
    except ImportError:
        logger.warning("No profiling support available, --profile option disabled.")
        logger.warning("If you're on Ubuntu try 'sudo apt-get install python-profiler'.")

    parser.add_argument('-o', '--mountoption', help="specify mount option", action="append")

    parser.add_argument('--mountpoint', help="specify mount point")

    args = parser.parse_args()

    # Pop auto_best and custom methods
    compression_methods.pop()
    compression_methods.pop()

    if args.profile:
        sys.stderr.write("Enabling profiling..\n")
        import cProfile, pstats
        profile = '.dedupsqlfs.cprofile-%i' % time.time()
        cProfile.run('fuse_mount(args, compression_methods, hash_functions)', profile)
        sys.stderr.write("\n Profiling statistics:\n\n")
        s = pstats.Stats(profile)
        s.sort_stats('time')
        s.print_stats(0.1)
        os.unlink(profile)
    else:
        return fuse_mount(args, compression_methods, hash_functions)

    return 0

if __name__ == '__main__':
    main()

# vim: ts=2 sw=2 et
