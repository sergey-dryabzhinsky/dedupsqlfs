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
except ImportError as e:
    msg = "Error: Failed to load one of the required Python modules! (%s)\n"
    sys.stderr.write(msg % str(e))
    sys.exit(1)


def fuse_mount(options, compression_methods=None, hash_functions=None):
    from dedupsqlfs.fuse.dedupfs import DedupFS
    from dedupsqlfs.fuse.operations import DedupOperations


    ops = DedupOperations()
    _fuse = DedupFS(
        ops, options.mountpoint,
        options,
        use_ino=True, default_permissions=True, fsname="dedupsqlfs")

    _fuse.saveCompressionMethods(compression_methods)
    _fuse.saveHashFunctions(hash_functions)

    for modname in compression_methods:
        if modname == "none":
            continue
        module = __import__(modname)
        _fuse.appendCompression(modname, getattr(module, "compress"), getattr(module, "decompress"))

    if options.print_stats:
        _fuse.read_only = True
        _fuse.init(None)
        _fuse.report_disk_usage()
        _fuse.destroy(None)
        return 0

    return _fuse.main()

def main(): # {{{1
    """
    This function enables using dedupsqlfs.py as a shell script that creates FUSE
    mount points. Execute "dedupsqlfs -h" for a list of valid command line options.
    """

    logger = logging.getLogger("dedupsqlfs.main")
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler(sys.stderr))

    parser = argparse.ArgumentParser(conflict_handler="resolve")

    # Register some custom command line options with the option parser.
    option_stored_in_db = " (this option is only useful when creating a new database, because your choice is stored in the database and can't be changed after that)"

    parser.add_argument('-h', '--help', action='help', help="show this help message followed by the command line options defined by the Python FUSE binding and exit")
    parser.add_argument('-v', '--verbose', action='count', dest='verbosity', default=0, help="increase verbosity")
    parser.add_argument('--print-stats', dest='print_stats', action='store_true', help="print the total apparent size and the actual disk usage of the file system and exit")
    parser.add_argument('--log-file', dest='log_file', help="specify log file location")
    parser.add_argument('--data', dest='data', metavar='DIRECTORY', default="~/data", help="Specify the base location for the files in which metadata and blocks data is stored. Defaults to ~/data")
    parser.add_argument('--name', dest='name', metavar='DATABASE', default="dedupsqlfs", help="Specify the name for the database directory in which metadata and blocks data is stored. Defaults to dedupsqlfs")
    parser.add_argument('--temp', dest='temp', metavar='DIRECTORY', help="Specify the location for the files in which temporary data is stored. By default honour TMPDIR environment variable value.")
    parser.add_argument('--block-size', dest='block_size', metavar='BYTES', default=1024*128, type=int, help="Specify the maximum block size in bytes" + option_stored_in_db + ". Defaults to 128kB.")

    parser.add_argument('--cpu-limit', dest='cpu_limit', metavar='NUMBER', default=0, type=int, help="Specify the maximum CPU count to use in multiprocess compression. Defaults to 0 (auto).")


    parser.add_argument('--mount-subvolume', dest='subvolume', metavar='NAME', help="Use subvolume as root fs.")
    parser.add_argument('--inmemory', dest='inmemory', action='store_true', help="Use memory storage for databases (@todo). Alot of memory.")
    parser.add_argument('--no-cache', dest='use_cache', action='store_false', help="Don't use cache in memory and delayed write to storage files (@todo).")
    parser.add_argument('--cache-timeout', dest='cache_timeout', metavar='NUMBER', type=int, default=60, help="Delay write to storage for NUMBER of seconds (@todo).")
    parser.add_argument('--no-transactions', dest='use_transactions', action='store_false', help="Don't use transactions when making multiple related changes, this might make the file system faster or slower (?).")
    parser.add_argument('--nosync', dest='synchronous', action='store_false', help="Disable SQLite's normal synchronous behavior which guarantees that data is written to disk immediately, because it slows down the file system too much (this means you might lose data when the mount point isn't cleanly unmounted).")
    parser.add_argument('--nogc-on-umount', dest='gc_umount_enabled', action='store_false', help="Disable garbage collection on umount operation (only do this when you've got disk space to waste or you know that nothing will be be deleted from the file system, which means little to no garbage will be produced).")
    parser.add_argument('--gc', dest='gc_enabled', action='store_true', help="Enable the periodic garbage collection because it degrades performance (only do this when you've got disk space to waste or you know that nothing will be be deleted from the file system, which means little to no garbage will be produced).")
    parser.add_argument('--gc-every-n-calls', dest='gc_n_calls', metavar="N", type=int, default=0, help="Call garbage callector after Nth FUSE operations, if GC enabled. Defaults to 0, which mean - none.")
    parser.add_argument('--gc-every-n-second', dest='gc_n_seconds', metavar="N", type=int, default=10, help="call garbage callector after Nth seconds on FUSE operations, if GC enabled. Defaults to 10.")
    parser.add_argument('--verify-writes', dest='verify_writes', action='store_true', help="After writing a new data block to the database, check that the block was written correctly by reading it back again and checking for differences.")

    # Dynamically check for supported hashing algorithms.
    msg = "Specify the hashing algorithm that will be used to recognize duplicate data blocks: one of %s"
    hash_functions = list({}.fromkeys([h.lower() for h in hashlib.algorithms_available]).keys())
    hash_functions.sort()
    msg %= ', '.join('%r' % fun for fun in hash_functions)
    msg += ". Defaults to 'sha1'."
    parser.add_argument('--hash', dest='hash_function', metavar='FUNCTION', choices=hash_functions, default='md5', help=msg)

    # Dynamically check for supported compression methods.
    compression_methods = ['none']
    for modname in 'lzo', 'zlib', 'bz2', "lzma", "snappy":
        try:
            module = __import__(modname)
            if hasattr(module, 'compress') and hasattr(module, 'decompress'):
                compression_methods.append(modname)
        except ImportError:
            pass
    if len(compression_methods) > 1:
        compression_methods.append("choose_best")

    msg = "enable compression of data blocks using one of the supported compression methods: one of %s"
    msg %= ', '.join('%r' % mth for mth in compression_methods)
    msg += ". Defaults to 'none'."
    parser.add_argument('--compress', dest='compression_method', metavar='METHOD', choices=compression_methods, default='none', help=msg)
    parser.add_argument('--force-compress', dest='compression_forced', action="store_true", help="Force compression event if resulting data is bigger than original.")
    # Do not want 'best' after help setup
    compression_methods.pop()

    # Dynamically check for profiling support.
    try:
        # Using __import__() here because of pyflakes.
        for p in 'cProfile', 'pstats': __import__(p)
        parser.add_argument('--profile', action='store_true', default=False, help="Use the Python modules cProfile and pstats to create a profile of time spent in various function calls and print out a table of the slowest functions at exit (of course this slows everything down but it can nevertheless give a good indication of the hot spots).")
    except ImportError:
        logger.warning("No profiling support available, --profile option disabled.")
        logger.warning("If you're on Ubuntu try 'sudo apt-get install python-profiler'.")

    parser.add_argument('--mountpoint', help="specify mount point")

    args = parser.parse_args()

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
