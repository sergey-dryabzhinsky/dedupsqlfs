# -*- coding: utf8 -*-
"""
Do commands for dedupsqlfs filesystem

Snapshot:
    create
    remove by name
    remove by date
    list
    statistic

Data:
    verify hashes
    rehash with new hash alg @todo
    defragment (gc, vacuum)
    (de)compress file(s) and director(y|ies) @todo
    change block size (need double free space) @todo
    statistic

FS tune:
    Change default mount options

"""

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


def get_db_manager(options):
    from dedupsqlfs.db.manager import DbManager
    manager = DbManager(options.name, options.data)
    manager.setSynchronous(options.synchronous)
    manager.setAutocommit(not options.use_transactions)
    return manager


def create_snapshot(options, _fuse):
    _fuse.operations.init()
    _fuse.setReadonly(False)
    _fuse.getLogger().setLevel(logging.INFO)
    _fuse.createSubvolume(options.snapshot_create)
    _fuse.operations.destroy()


def print_fs_stats(options, _fuse):
    _fuse.setReadonly(True)
    _fuse.operations.init()
    _fuse.getLogger().setLevel(logging.INFO)
    _fuse.report_disk_usage()
    _fuse.operations.destroy()
    return 0

def data_defragment(options, _fuse):
    _fuse.operations.init()
    _fuse.setReadonly(False)
    _fuse.getLogger().setLevel(logging.INFO)
    _fuse.operations.gc_enabled = True
    _fuse.operations.gc_vacuum_enabled = True
    _fuse.operations.should_vacuum = True
    _fuse.operations.destroy()
    return 0


def do(options, compression_methods=None):
    from dedupsqlfs.fuse.dedupfs import DedupFS
    from dedupsqlfs.fuse.operations import DedupOperations


    ops = DedupOperations()
    _fuse = DedupFS(
        ops, "",
        options,
        use_ino=True, default_permissions=True, fsname="dedupsqlfs")

    _fuse.saveCompressionMethods(compression_methods)

    for modname in compression_methods:
        if modname == constants.COMPRESSION_TYPE_NONE:
            continue
        module = __import__(modname)
        _fuse.appendCompression(modname, getattr(module, "compress"), getattr(module, "decompress"))

    if options.defragment:
        return data_defragment(options, _fuse)

    if options.print_stats:
        return print_fs_stats(options, _fuse)

    return 0

def main(): # {{{1
    """
    This function enables using dedupsqlfs.py as a shell script that creates FUSE
    mount points. Execute "dedupsqlfs -h" for a list of valid command line options.
    """

    logger = logging.getLogger("do.dedupsqlfs/main")
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler(sys.stderr))

    parser = argparse.ArgumentParser(conflict_handler="resolve")

    # Register some custom command line options with the option parser.
    generic = parser.add_argument_group('Generic')

    generic.add_argument('-h', '--help', action='help', help="show this help message followed by the command line options defined by the Python FUSE binding and exit")
    generic.add_argument('-v', '--verbose', action='count', dest='verbosity', default=0, help="increase verbosity")
    generic.add_argument('--log-file', dest='log_file', metavar='PATH', help="specify log file location")
    generic.add_argument('--data', dest='data', metavar='DIRECTORY', default="~/data", help="Specify the base location for the files in which metadata and blocks data is stored. Defaults to ~/data")
    generic.add_argument('--name', dest='name', metavar='DATABASE', default="dedupsqlfs", help="Specify the name for the database directory in which metadata and blocks data is stored. Defaults to dedupsqlfs")
    generic.add_argument('--temp', dest='temp', metavar='DIRECTORY', help="Specify the location for the files in which temporary data is stored. By default honour TMPDIR environment variable value.")
    generic.add_argument('--no-transactions', dest='use_transactions', action='store_false', help="Don't use transactions when making multiple related changes, this might make the file system faster or slower (?).")
    generic.add_argument('--nosync', dest='synchronous', action='store_false', help="Disable SQLite's normal synchronous behavior which guarantees that data is written to disk immediately, because it slows down the file system too much (this means you might lose data when the mount point isn't cleanly unmounted).")
    generic.add_argument('--nogc-on-umount', dest='gc_umount_enabled', action='store_false', help="Disable garbage collection on umount operation (only do this when you've got disk space to waste or you know that nothing will be be deleted from the file system, which means little to no garbage will be produced).")
    generic.add_argument('--gc', dest='gc_enabled', action='store_true', help="Enable the periodic garbage collection because it degrades performance (only do this when you've got disk space to waste or you know that nothing will be be deleted from the file system, which means little to no garbage will be produced).")
    generic.add_argument('--verify-writes', dest='verify_writes', action='store_true', help="After writing a new data block to the database, check that the block was written correctly by reading it back again and checking for differences.")

    generic.add_argument('--memory-limit', dest='memory_limit', action='store_true', help="Use some lower values for less memory consumption.")


    data = parser.add_argument_group('Data')
    data.add_argument('--print-stats', dest='print_stats', action='store_true', help="print the total apparent size and the actual disk usage of the file system and exit")
    data.add_argument('--defragment', dest='defragment', action='store_true', help="defragment all stored data, do garbage collection")
    data.add_argument('--verify', dest='verify', action='store_true', help="verify all stored data hashes")
    data.add_argument('--new-block-size', dest='new_block_size', metavar='BYTES', default=1024*128, type=int, help="Specify the new maximum block size in bytes. Defaults to 128kB.")

    # Dynamically check for supported hashing algorithms.
    msg = "Specify the hashing algorithm that will be used to recognize duplicate data blocks: one of %s"
    hash_functions = list({}.fromkeys([h.lower() for h in hashlib.algorithms_available]).keys())
    hash_functions.sort()
    msg %= ', '.join('%r' % fun for fun in hash_functions)
    msg += ". Defaults to 'sha1'."
    data.add_argument('--rehash', dest='hash_function', metavar='FUNCTION', choices=hash_functions, default='md5', help=msg)

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

    msg = "Enable compression of data blocks using one of the supported compression methods: one of %s"
    msg %= ', '.join('%r' % mth for mth in compression_methods)
    msg += ". Defaults to %r." % constants.COMPRESSION_TYPE_NONE
    data.add_argument('--compress-method', dest='compression_method', metavar='METHOD', choices=compression_methods, default=constants.COMPRESSION_TYPE_NONE, help=msg)
    data.add_argument('--recompress', dest='recompress_path', metavar='PATH', help="Compress file or entire directory with new compression method")
    data.add_argument('--force-compress', dest='compression_forced', action="store_true", help="Force compression even if resulting data is bigger than original.")
    # Do not want 'best' after help setup
    compression_methods.pop()

    # Dynamically check for profiling support.
    try:
        # Using __import__() here because of pyflakes.
        for p in 'cProfile', 'pstats': __import__(p)
        generic.add_argument('--profile', action='store_true', default=False, help="Use the Python modules cProfile and pstats to create a profile of time spent in various function calls and print out a table of the slowest functions at exit (of course this slows everything down but it can nevertheless give a good indication of the hot spots).")
    except ImportError:
        logger.warning("No profiling support available, --profile option disabled.")
        logger.warning("If you're on Ubuntu try 'sudo apt-get install python-profiler'.")


    snapshot = parser.add_argument_group('Snapshot')
    snapshot.add_argument('--list-snapshots', dest='snapshot_list', action='store_true', help="Show list of all snapshots")
    snapshot.add_argument('--create-snapshot', dest='snapshot_create', metavar='NAME', help="Create new snapshot")
    snapshot.add_argument('--select-snapshot', dest='snapshot', metavar='NAME', help="Select snapshot")
    snapshot.add_argument('--remove-snapshot', dest='snapshot_remove', metavar='NAME', help="Remove selected snapshot")
    snapshot.add_argument('--remove-older-than', dest='snapshot_remove_older', metavar='DATE', help="Remove snapshots older than selected date")
    snapshot.add_argument('--snapshot-stats', dest='snapshot_stats', action='store_true', help="Print information about selected snapshot")


    args = parser.parse_args()

    if args.profile:
        sys.stderr.write("Enabling profiling..\n")
        import cProfile, pstats
        profile = '.dedupsqlfs.cprofile-%i' % time.time()
        cProfile.run('do(args, compression_methods)', profile)
        sys.stderr.write("\n Profiling statistics:\n\n")
        s = pstats.Stats(profile)
        s.sort_stats('time')
        s.print_stats(0.1)
        os.unlink(profile)
    else:
        return do(args, compression_methods)

    return 0

# vim: ts=4 sw=4 et
