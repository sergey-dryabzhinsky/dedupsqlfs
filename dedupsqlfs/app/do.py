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
    from time import time
    import hashlib
    import logging
    from dedupsqlfs.lib import constants
    from dedupsqlfs.db import check_engines
except ImportError as e:
    msg = "Error: Failed to load one of the required Python modules! (%s)\n"
    sys.stderr.write(msg % str(e))
    sys.exit(1)

def create_subvolume(options, _fuse):
    """
    @param options: Commandline options
    @type  options: object

    @param _fuse: FUSE wrapper
    @type  _fuse: dedupsqlfs.fuse.dedupfs.DedupFS
    """
    _fuse.setOption("gc_umount_enabled", False)
    _fuse.setOption("gc_vacuum_enabled", False)
    _fuse.setOption("gc_enabled", False)
    _fuse.setOption("use_transactions", True)
    _fuse.setReadonly(False)

    from dedupsqlfs.fuse.subvolume import Subvolume
    sv = Subvolume(_fuse.operations)
    sv.create(options.subvol_create.encode('utf8'))

    _fuse.operations.destroy()
    return

def list_subvolume(options, _fuse):
    """
    @param options: Commandline options
    @type  options: object

    @param _fuse: FUSE wrapper
    @type  _fuse: dedupsqlfs.fuse.dedupfs.DedupFS
    """
    _fuse.setOption("gc_umount_enabled", False)
    _fuse.setOption("gc_vacuum_enabled", False)
    _fuse.setOption("gc_enabled", False)
    _fuse.setReadonly(True)

    from dedupsqlfs.fuse.subvolume import Subvolume
    sv = Subvolume(_fuse.operations)
    sv.list(_fuse.getOption("subvol_list_with_stats"))

    _fuse.operations.destroy()
    return

def remove_subvolume(options, _fuse):
    """
    @param options: Commandline options
    @type  options: object

    @param _fuse: FUSE wrapper
    @type  _fuse: dedupsqlfs.fuse.dedupfs.DedupFS
    """
    _fuse.setOption("gc_umount_enabled", False)
    _fuse.setOption("gc_vacuum_enabled", False)
    _fuse.setOption("gc_enabled", False)
    _fuse.setOption("use_transactions", True)
    _fuse.setReadonly(False)

    from dedupsqlfs.fuse.subvolume import Subvolume
    sv = Subvolume(_fuse.operations)
    sv.remove(options.subvol_remove.encode('utf8'))

    _fuse.operations.destroy()
    return


def print_subvol_stats(options, _fuse):
    """
    @param options: Commandline options
    @type  options: object

    @param _fuse: FUSE wrapper
    @type  _fuse: dedupsqlfs.fuse.dedupfs.DedupFS
    """
    _fuse.setOption("gc_umount_enabled", False)
    _fuse.setOption("gc_vacuum_enabled", False)
    _fuse.setOption("gc_enabled", False)
    _fuse.setReadonly(True)

    from dedupsqlfs.fuse.subvolume import Subvolume
    sv = Subvolume(_fuse.operations)
    sv.report_usage(options.subvol_stats.encode('utf8'))

    _fuse.operations.destroy()
    return


def create_snapshot(options, _fuse):
    """
    @param options: Commandline options
    @type  options: object

    @param _fuse: FUSE wrapper
    @type  _fuse: dedupsqlfs.fuse.dedupfs.DedupFS
    """
    logger = logging.getLogger("do.dedupsqlfs/create_snapshot")
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler(sys.stderr))

    if not options.subvol_selected:
        logger.error("Select subvolume/snapshot from which create new one!")
        return

    _fuse.setOption("gc_umount_enabled", False)
    _fuse.setOption("gc_vacuum_enabled", False)
    _fuse.setOption("gc_enabled", False)
    _fuse.setOption("use_transactions", True)
    _fuse.setReadonly(False)

    from dedupsqlfs.fuse.snapshot import Snapshot
    snap = Snapshot(_fuse.operations)
    snap.make(options.subvol_selected.encode('utf8'), options.snapshot_create.encode('utf8'))
    return

def remove_snapshot(options, _fuse):
    """
    @param options: Commandline options
    @type  options: object

    @param _fuse: FUSE wrapper
    @type  _fuse: dedupsqlfs.fuse.dedupfs.DedupFS
    """
    _fuse.setOption("gc_umount_enabled", False)
    _fuse.setOption("gc_vacuum_enabled", False)
    _fuse.setOption("gc_enabled", False)
    _fuse.setOption("use_transactions", True)
    _fuse.setReadonly(False)

    from dedupsqlfs.fuse.snapshot import Snapshot
    snap = Snapshot(_fuse.operations)
    snap.remove(options.snapshot_remove.encode('utf8'))
    return

def remove_snapshot_older(options, _fuse):
    """
    @param options: Commandline options
    @type  options: object

    @param _fuse: FUSE wrapper
    @type  _fuse: dedupsqlfs.fuse.dedupfs.DedupFS
    """
    _fuse.setOption("gc_umount_enabled", False)
    _fuse.setOption("gc_vacuum_enabled", False)
    _fuse.setOption("gc_enabled", False)
    _fuse.setOption("use_transactions", True)
    _fuse.setReadonly(False)

    from dedupsqlfs.fuse.snapshot import Snapshot
    snap = Snapshot(_fuse.operations)
    snap.remove_older_than(options.snapshot_remove_older, options.snapshot_select_by_last_update_time)
    return

def remove_snapshot_plan(options, _fuse):
    """
    @param options: Commandline options
    @type  options: object

    @param _fuse: FUSE wrapper
    @type  _fuse: dedupsqlfs.fuse.dedupfs.DedupFS
    """
    _fuse.setOption("gc_umount_enabled", False)
    _fuse.setOption("gc_vacuum_enabled", False)
    _fuse.setOption("gc_enabled", False)
    _fuse.setOption("use_transactions", True)
    _fuse.setReadonly(False)

    from dedupsqlfs.fuse.snapshot import Snapshot
    snap = Snapshot(_fuse.operations)
    snap.remove_plan(options.snapshot_remove_plan, options.snapshot_select_by_last_update_time)
    return

def count_snapshot_older(options, _fuse):
    """
    @param options: Commandline options
    @type  options: object

    @param _fuse: FUSE wrapper
    @type  _fuse: dedupsqlfs.fuse.dedupfs.DedupFS
    """
    _fuse.setOption("gc_umount_enabled", False)
    _fuse.setOption("gc_vacuum_enabled", False)
    _fuse.setOption("gc_enabled", False)
    _fuse.setOption("use_transactions", True)
    _fuse.setReadonly(True)

    from dedupsqlfs.fuse.snapshot import Snapshot
    snap = Snapshot(_fuse.operations)
    snap.count_older_than(options.snapshot_count_older, options.snapshot_select_by_last_update_time)
    return

def count_snapshot_plan(options, _fuse):
    """
    @param options: Commandline options
    @type  options: object

    @param _fuse: FUSE wrapper
    @type  _fuse: dedupsqlfs.fuse.dedupfs.DedupFS
    """
    _fuse.setOption("gc_umount_enabled", False)
    _fuse.setOption("gc_vacuum_enabled", False)
    _fuse.setOption("gc_enabled", False)
    _fuse.setOption("use_transactions", True)
    _fuse.setReadonly(True)

    from dedupsqlfs.fuse.snapshot import Snapshot
    snap = Snapshot(_fuse.operations)
    snap.count_plan(options.snapshot_count_plan, options.snapshot_select_by_last_update_time)
    return

def print_snapshot_stats(options, _fuse):
    """
    @param options: Commandline options
    @type  options: object

    @param _fuse: FUSE wrapper
    @type  _fuse: dedupsqlfs.fuse.dedupfs.DedupFS
    """
    _fuse.setOption("gc_umount_enabled", False)
    _fuse.setOption("gc_vacuum_enabled", False)
    _fuse.setOption("gc_enabled", False)
    _fuse.setReadonly(True)

    from dedupsqlfs.fuse.snapshot import Snapshot
    snap = Snapshot(_fuse.operations)
    snap.report_usage(options.snapshot_stats.encode('utf8'))
    return

def set_snapshot_readonly(options, _fuse, flag):
    """
    @param options: Commandline options
    @type  options: object

    @param _fuse: FUSE wrapper
    @type  _fuse: dedupsqlfs.fuse.dedupfs.DedupFS
    """
    _fuse.setOption("gc_umount_enabled", False)
    _fuse.setOption("gc_vacuum_enabled", False)
    _fuse.setOption("gc_enabled", False)
    _fuse.setOption("use_transactions", True)
    _fuse.setReadonly(True)

    from dedupsqlfs.fuse.snapshot import Snapshot
    snap = Snapshot(_fuse.operations)
    if flag:
        snap.readonly(options.snapshot_readonly_set.encode('utf8'))
    else:
        snap.readonly(options.snapshot_readonly_unset.encode('utf8'))
    return


def print_fs_stats(options, _fuse):
    _fuse.setReadonly(True)
    _fuse.getLogger().setLevel(logging.INFO)
    _fuse.report_disk_usage()
    _fuse.getLogger().setLevel(logging.ERROR)
    _fuse.operations.destroy()
    return 0


def data_vacuum(options, _fuse):
    _fuse.setOption("gc_umount_enabled", False)
    _fuse.setOption("gc_vacuum_enabled", True)
    _fuse.setOption("gc_enabled", False)
    _fuse.setReadonly(False)

    _fuse.operations.init()
    _fuse.operations.should_vacuum = True
    _fuse.operations.forced_vacuum()
    _fuse.operations.destroy()
    return 0


def data_defragment(options, _fuse):
    """
    @todo
    """
    _fuse.setOption("gc_umount_enabled", True)
    _fuse.setOption("gc_vacuum_enabled", False)
    _fuse.setOption("gc_enabled", True)
    _fuse.setReadonly(False)

    _fuse.operations.init()
    _fuse.operations.destroy()
    return 0


def do(options, compression_methods=None):
    from dedupsqlfs.fuse.dedupfs import DedupFS
    from dedupsqlfs.fuse.operations import DedupOperations

    ops = None
    _fuse = None
    try:
        ops = DedupOperations()
        _fuse = DedupFS(
            ops, "",
            options,
            use_ino=True, default_permissions=True, fsname="dedupsqlfs")

        _fuse.preInit()

        basePath = os.path.expanduser(_fuse.getOption("data"))
        if os.path.exists(basePath):
            if not _fuse.getOption("storage_engine"):
                _fuse.setOption("storage_engine", "auto")

        _fuse.saveCompressionMethods(compression_methods)

        for modname in compression_methods:
            _fuse.appendCompression(modname)

        # Actions

        if options.subvol_create:
            create_subvolume(options, _fuse)

        if options.subvol_list:
            list_subvolume(options, _fuse)

        if options.subvol_remove:
            remove_subvolume(options, _fuse)

        if options.subvol_stats:
            print_subvol_stats(options, _fuse)

        if options.snapshot_create:
            create_snapshot(options, _fuse)

        if options.snapshot_list:
            list_subvolume(options, _fuse)

        if options.snapshot_remove:
            remove_snapshot(options, _fuse)

        if options.snapshot_remove_older:
            remove_snapshot_older(options, _fuse)

        if options.snapshot_remove_plan:
            remove_snapshot_plan(options, _fuse)

        if options.snapshot_count_older:
            count_snapshot_older(options, _fuse)

        if options.snapshot_count_plan:
            count_snapshot_plan(options, _fuse)

        if options.snapshot_readonly_set:
            set_snapshot_readonly(options, _fuse, True)

        if options.snapshot_readonly_unset:
            set_snapshot_readonly(options, _fuse, False)

        if options.snapshot_stats:
            print_snapshot_stats(options, _fuse)

        if options.defragment:
            data_defragment(options, _fuse)

        if options.vacuum:
            data_vacuum(options, _fuse)

        if options.print_stats:
            print_fs_stats(options, _fuse)

        ret = 0
    except Exception:
        import traceback
        traceback.print_exc()
        ret = 1

    if _fuse:
        _fuse.postDestroy()

    if ops:
        ops.getManager().close()

    return ret

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

    generic.add_argument('--cpu-limit', dest='cpu_limit', metavar='NUMBER', default=0, type=int, help="Specify the maximum CPU count to use in multiprocess compression. Defaults to 0 (auto).")


    engines, msg = check_engines()
    if not engines:
        logger.error("No storage engines available! Please install sqlite or pymysql python module!")
        return 1

    generic.add_argument('--storage-engine', dest='storage_engine', metavar='ENGINE', choices=engines, default=engines[0],
                        help=msg)

    if "mysql" in engines:

        from dedupsqlfs.db.mysql import get_table_engines

        table_engines = get_table_engines()

        msg = "One of MySQL table engines: "+", ".join(table_engines)+". Default: %r. Aria and TokuDB engine can be used only with MariaDB or Percona server." % table_engines[0]
        generic.add_argument('--table-engine', dest='table_engine', metavar='ENGINE',
                            choices=table_engines, default=table_engines[0],
                            help=msg)

    data = parser.add_argument_group('Data')
    data.add_argument('--print-stats', dest='print_stats', action='store_true', help="Print the total apparent size and the actual disk usage of the file system and exit")
    data.add_argument('--check-tree-inodes', dest='check_tree_inodes', action='store_true', help="Check if inodes exists in fs tree on fs usage calculation. Applies to subvolume and snapshot stats calculation too.")
    data.add_argument('--defragment', dest='defragment', action='store_true', help="Defragment all stored data, do garbage collection.")
    data.add_argument('--vacuum', dest='vacuum', action='store_true', help="Like defragment, but force SQLite to vacuum databases.")
    data.add_argument('--verify', dest='verify', action='store_true', help="Verify all stored data hashes. (@todo)")
    data.add_argument('--new-block-size', dest='new_block_size', metavar='BYTES', default=1024*128, type=int, help="Specify the new block size in bytes. Defaults to 128kB. (@todo)")
    data.add_argument('--maximum-block-size', dest='maximum_block_size', metavar='BYTES', default=1024*1024*10, type=int, help="Specify the maximum block size in bytes for defragmentation. Defaults to 10MB.")

    # Dynamically check for supported hashing algorithms.
    msg = "Specify the hashing algorithm that will be used to recognize duplicate data blocks: one of %s"
    hash_functions = list({}.fromkeys([h.lower() for h in hashlib.algorithms_available]).keys())
    hash_functions.sort()
    msg %= ', '.join('%r' % fun for fun in hash_functions)
    msg += ". Defaults to 'sha1'. (@todo)"
    data.add_argument('--rehash', dest='hash_function', metavar='FUNCTION', choices=hash_functions, default='sha1', help=msg)

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

    msg = "Enable compression of data blocks using one of the supported compression methods: %s"
    msg %= ', '.join('%r' % mth for mth in compression_methods)
    msg += ". Defaults to %r." % constants.COMPRESSION_TYPE_NONE
    msg += " You can use <method>=<level> syntax, <level> can be integer or value from --compression-level."
    if len(compression_methods) > 1:
        msg += " %r will try all compression methods and choose one with smaller result data." % constants.COMPRESSION_TYPE_BEST
        msg += " %r will try selected compression methods (--custom-compress) and choose one with smaller result data." % constants.COMPRESSION_TYPE_CUSTOM

    data.add_argument('--compress-method', dest='compression_method', metavar='METHOD', default=constants.COMPRESSION_TYPE_NONE, help=msg)
    data.add_argument('--recompress', dest='recompress_path', metavar='PATH', help="Compress file or entire directory with new compression method")

    msg = "Enable compression of data blocks using one or more of the supported compression methods: %s"
    msg %= ', '.join('%r' % mth for mth in compression_methods[:-2])
    msg += ". To use two or more methods select this option in command line for each compression method."
    msg += " You can use <method>=<level> syntax, <level> can be integer or value from --compression-level."

    data.add_argument('--custom-compress', dest='compression_custom', metavar='METHOD', action="append", help=msg)
    data.add_argument('--force-compress', dest='compression_forced', action="store_true", help="Force compression even if resulting data is bigger than original.")
    data.add_argument('--minimal-compress-size', dest='compression_minimal_size', metavar='BYTES', type=int, default=-1, help="Minimal block data size for compression. Defaults to -1 bytes (auto). Do not do compression if not forced to.")

    levels = (constants.COMPRESSION_LEVEL_DEFAULT, constants.COMPRESSION_LEVEL_FAST, constants.COMPRESSION_LEVEL_NORM, constants.COMPRESSION_LEVEL_BEST)

    data.add_argument('--compression-level', dest='compression_level', metavar="LEVEL", default=constants.COMPRESSION_LEVEL_DEFAULT,
                        help="Compression level ratio: one of %s; or INT. Defaults to %r. Not all methods support this option." % (
                            ', '.join('%r' % lvl for lvl in levels), constants.COMPRESSION_LEVEL_DEFAULT
                        ))
    # Do not want 'best' after help setup

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
    snapshot.add_argument('--list-snapshots-with-stats', dest='subvol_list_with_stats', action='store_true', help="Show more statistics in snapshots list. Slow.")
    snapshot.add_argument('--select-subvol', dest='subvol_selected', metavar='NAME', default=None, help="Select subvolume for operations.")
    snapshot.add_argument('--create-snapshot', dest='snapshot_create', metavar='NAME', help="Create new snapshot from selected subvolume")
    snapshot.add_argument('--remove-snapshot', dest='snapshot_remove', metavar='NAME', help="Remove selected snapshot")
    snapshot.add_argument('--remove-snapshots-older-than', dest='snapshot_remove_older', metavar='DATE', help="Remove snapshots older than selected creation date. Date format: 'YYYY-mm-ddTHH:MM:SS'.")
    snapshot.add_argument('--remove-snapshots-by-plan', dest='snapshot_remove_plan', metavar='PLAN', help="Remove snapshots by cleanup plan order by creation date. Plan format: 'Nd(ays),Nw(eeks),Nm(onths),Ny(ears)'. Example: 14d,8w,6m,2y. Default: 7d,4w,2m,1y")
    snapshot.add_argument('--count-snapshots-older-than', dest='snapshot_count_older', metavar='DATE', help="Count snapshots older than selected creation date. Date format: 'YYYY-mm-ddTHH:MM:SS'.")
    snapshot.add_argument('--count-snapshots-by-plan', dest='snapshot_count_plan', metavar='PLAN', help="Count snapshots to be removed by cleanup plan order by creation date. Plan format: 'Nd(ays),Nw(eeks),Nm(onths),Ny(ears)'. Example: 14d,8w,6m,2y. Default: 7d,4w,2m,1y")
    snapshot.add_argument('--select-snapshots-by-last-update-time', dest='snapshot_select_by_last_update_time', action='store_true', help="Remove or count snapshots older than selected date by last update time.")
    snapshot.add_argument('--snapshot-stats', dest='snapshot_stats', metavar='NAME', help="Print information about selected snapshot")
    snapshot.add_argument('--set-snapshot-readonly', dest='snapshot_readonly_set', metavar='NAME', help="Set subvolume READONLY flag for selected snapshot.")
    snapshot.add_argument('--unset-snapshot-readonly', dest='snapshot_readonly_unset', metavar='NAME', help="UnSet subvolume READONLY flag for selected snapshot.")

    subvol = parser.add_argument_group('Subvolume')
    subvol.add_argument('--list-subvol', dest='subvol_list', action='store_true', help="Show list of all subvolumes")
    subvol.add_argument('--list-subvol-with-stats', dest='subvol_list_with_stats', action='store_true', help="Show more statistics in subvolumes list. Slow.")
    subvol.add_argument('--create-subvol', dest='subvol_create', metavar='NAME', help="Create new subvolume")
    subvol.add_argument('--remove-subvol', dest='subvol_remove', metavar='NAME', help="Remove selected subvolume")
    subvol.add_argument('--subvol-stats', dest='subvol_stats', metavar='NAME', help="Print information about selected subvolume")

    args = parser.parse_args()

    compression_methods.pop()
    compression_methods.pop()

    if args.profile:
        sys.stderr.write("Enabling profiling..\n")
        import cProfile, pstats
        profile = '.dedupsqlfs.cprofile-%i' % time()
        profiler = cProfile.Profile()
        result = profiler.runcall(do, compression_methods)
        profiler.dump_stats(profile)
        sys.stderr.write("\n Profiling statistics:\n\n")
        s = pstats.Stats(profile)
        s.sort_stats('calls').print_stats(0.1)
        s.sort_stats('cumtime').print_stats(0.1)
        s.sort_stats('tottime').print_stats(0.1)
        os.unlink(profile)
    else:
        return do(args, compression_methods)

    return 0

# vim: ts=4 sw=4 et
