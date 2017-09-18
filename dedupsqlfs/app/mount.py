# -*- coding: utf8 -*-

# Imports. {{{1

import sys

# Try to load the required modules from Python's standard library.
try:
    import os
    import traceback
    import argparse
    from time import time
    import hashlib
except ImportError as e:
    msg = "Error: Failed to load one of the required Python modules! (%s)\n"
    sys.stderr.write(msg % str(e))
    sys.exit(1)

from dedupsqlfs.lib import constants
from dedupsqlfs.db import check_engines
from dedupsqlfs.log import logging
from dedupsqlfs.fs import which
from dedupsqlfs.argp import SmartFormatter
import dedupsqlfs

def fuse_mount(options, compression_methods=None):
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
    except Exception:
        import traceback
        err_str = traceback.format_exc()
        logger = ops.getApplication().getLogger()
        logger.error(err_str)
        print(err_str)
        ret = -1
    if ops:
        ops.getManager().close()
        ops.getApplication().stopCacheFlusher()

        if options.memory_usage:
            import resource
            kbytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            logger = ops.getApplication().getLogger()
            logger.important("\n  ~~ MOUNT ~~")
            logger.important("-= Memory statistics: =-")
            logger.important("Peak memory usage: %.2f Mb\n" % (kbytes/1000.0))

    return ret

def main(): # {{{1
    """
    This function enables using dedupsqlfs.py as a shell script that creates FUSE
    mount points. Execute "dedupsqlfs -h" for a list of valid command line options.
    """

    logger = logging.getLogger("mount.dedupsqlfs/main")
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler(sys.stderr))

    parser = argparse.ArgumentParser(
        prog="%s/%s mount/%s" % (dedupsqlfs.__name__, dedupsqlfs.__version__, dedupsqlfs.__fsversion__),
        formatter_class = SmartFormatter,
        conflict_handler="resolve")

    # Register some custom command line options with the option parser.
    option_stored_in_db = " (this option is only useful when creating a new database, because your choice is stored in the database and can't be changed after that)"

    generic = parser.add_argument_group('Generic')

    generic.add_argument('-h', '--help', action='help', help="show this help message followed by the command line options defined by the Python FUSE binding and exit")
    generic.add_argument('-v', '--verbose', action='count', dest='verbosity', default=0, help="increase verbosity: 0 - error, 1 - warning, 2 - info, 3 - debug, 4 - verbose")
    generic.add_argument('--verbose-stats', dest='verbose_stats', action='store_true', help="Enable FS opterations statistic output. Verbosity level must be 1+.")
    generic.add_argument('--log-file', dest='log_file', help="specify log file location")
    generic.add_argument('--log-file-only', dest='log_file_only', action='store_true',
                        help="Don't send log messages to stderr.")

    generic.add_argument('--lock-file', dest='lock_file', help="Specify lock file location. Useful to check fs status via content or existsnce.")
    generic.add_argument('--data', dest='data', metavar='DIRECTORY', default="~/data", help="Specify the base location for the files in which metadata and blocks data is stored. Defaults to ~/data")
    generic.add_argument('--name', dest='name', metavar='DATABASE', default="dedupsqlfs", help="Specify the name for the database directory in which metadata and blocks data is stored. Defaults to dedupsqlfs")
    generic.add_argument('--temp', dest='temp', metavar='DIRECTORY', help="Specify the location for the files in which temporary data is stored. By default honour TMPDIR environment variable value.")
    generic.add_argument('-b', '--block-size', dest='block_size', metavar='BYTES', default=1024*128, type=int, help="Specify the maximum block size in bytes" + option_stored_in_db + ". Defaults to 128kB.")
    generic.add_argument('--mount-subvolume', dest='mounted_subvolume', metavar='NAME', default=None, help="Use subvolume NAME as root fs.")

    generic.add_argument('--memory-limit', dest='memory_limit', action='store_true', help="Use some lower values for less memory consumption.")

    generic.add_argument('--cpu-limit', dest='cpu_limit', metavar='NUMBER', default=0, type=int, help="Specify the maximum CPU count to use in multiprocess compression. Defaults to 0 (auto).")
    generic.add_argument('--multi-cpu', dest='multi_cpu', metavar='TYPE', default="single", choices=("single", "process", "thread",), help="Specify type of compression tool: single process, multi-process or multi-thread. Choices are: 'single', 'process', 'thread'. Defaults to 'single'.")


    grp_data = parser.add_argument_group('Data')

    engines, msg = check_engines()
    if not engines:
        logger.error("No storage engines available! Please install sqlite or pymysql python module!")
        return 1

    grp_data.add_argument('--storage-engine', dest='storage_engine', metavar='ENGINE', choices=engines, default=engines[0],
                        help=msg)

    if "mysql" in engines:

        from dedupsqlfs.db.mysql import get_table_engines

        table_engines = get_table_engines()

        msg = "One of MySQL table engines: "+", ".join(table_engines)+". Default: %r. Aria and TokuDB engine can be used only with MariaDB or Percona server." % table_engines[0]
        grp_data.add_argument('--table-engine', dest='table_engine', metavar='ENGINE',
                            choices=table_engines, default=table_engines[0],
                            help=msg)

    grp_data.add_argument('--no-transactions', dest='use_transactions', action='store_false', help="Don't use transactions when making multiple related changes, this might make the file system faster or slower (?).")
    grp_data.add_argument('--nosync', dest='synchronous', action='store_false', help="Disable SQLite's normal synchronous behavior which guarantees that data is written to disk immediately, because it slows down the file system too much (this means you might lose data when the mount point isn't cleanly unmounted).")

    grp_data.add_argument('--nogc-on-umount', dest='gc_umount_enabled', action='store_false', help="Disable garbage collection on umount operation (only do this when you've got disk space to waste or you know that nothing will be be deleted from the file system, which means little to no garbage will be produced).")
    grp_data.add_argument('--gc', dest='gc_enabled', action='store_true', help="Enable the periodic garbage collection. It degrades performance. Only do this when you don't have disk space to waste or you know that alot of data will be be deleted from the file system.")
    grp_data.add_argument('--gc-vacuum', dest='gc_vacuum_enabled', action='store_true', help="Enable data vacuum after the periodic garbage collection.")
    grp_data.add_argument('--gc-fast', dest='gc_fast_enabled', action='store_true', help="Enable faster periodic garbage collection. Don't collect hash and block garbage.")
    grp_data.add_argument('--gc-interval', dest='gc_interval', metavar="N", type=int, default=60, help="Call garbage callector after Nth seconds on FUSE operations, if GC enabled. Defaults to 60.")

    # Dynamically check for supported hashing algorithms.
    msg = "Specify the hashing algorithm that will be used to recognize duplicate data blocks: one of %s"
    hash_functions = list({}.fromkeys([h.lower() for h in hashlib.algorithms_available]).keys())
    hash_functions.sort()
    work_hash_funcs = set(hash_functions) & constants.WANTED_HASH_FUCTIONS
    msg %= ', '.join('%r' % fun for fun in work_hash_funcs)
    msg += ". Defaults to 'sha1'."
    grp_data.add_argument('--hash', dest='hash_function', metavar='FUNCTION', choices=hash_functions, default='sha1', help=msg)

    grp_data.add_argument('--collision-check', dest='collision_check_enabled', action='store_true', help="Check for hash collision on writed data.")


    grp_cache = parser.add_argument_group('Cache')

    grp_cache.add_argument('--no-cache', dest='use_cache', action='store_false', help="Don't use cache in memory and delayed write to storage.")
    grp_cache.add_argument('--no-cache-flusher', dest='use_cache_flusher', action='store_false', help="Don't use separate cache flusher process. It touches file in mount_point directory. This may prevent FS to umount cleanly.")
    grp_cache.add_argument('--cache-meta-timeout', dest='cache_meta_timeout', metavar='SECONDS', type=int, default=20, help="Delay flush expired metadata for NUMBER of seconds. Defaults to 20 seconds.")
    grp_cache.add_argument('--cache-block-write-timeout', dest='cache_block_write_timeout', metavar='SECONDS', type=int, default=10, help="Expire writed data and flush from memory after NUMBER of seconds. Defaults to 10 seconds.")
    grp_cache.add_argument('--cache-block-write-size', dest='cache_block_write_size', metavar='BYTES', type=int,
                        default=1024*1024*1024,
                        help="Write cache for blocks: potential size in BYTES. Set to -1 for infinite. Defaults to 1024 MB.")
    grp_cache.add_argument('--cache-block-read-timeout', dest='cache_block_read_timeout', metavar='SECONDS', type=int, default=10, help="Expire readed data and flush from memory after NUMBER of seconds. Defaults to 10 seconds.")
    grp_cache.add_argument('--cache-block-read-size', dest='cache_block_read_size', metavar='BYTES', type=int,
                        default=1024*1024*1024,
                        help="Readed cache for blocks: potential size in BYTES. Set to -1 for infinite. Defaults to 1024 MB.")
    grp_cache.add_argument('--flush-interval', dest='flush_interval', metavar="SECONDS", type=int, default=5, help="Call expired cache callector every Nth seconds on FUSE operations. Defaults to 5.")


    grp_compress = parser.add_argument_group('Compression')

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
        compression_methods_cmd.append(constants.COMPRESSION_TYPE_FAST)

    msg = "R|Enable compression of data blocks using one or more of the supported compression methods: %s"
    msg %= ', '.join('%r' % mth for mth in compression_methods_cmd[:-2])
    msg += ".\n- To use two or more methods select this option in command line for each compression method."
    msg += "\n- You can use <method>:<level> syntax, <level> can be integer or value from --compression-level."
    if len(compression_methods_cmd) > 1:
        msg += "\n- Method %r will try all compression methods with 'best' level and choose one with smaller result data." % constants.COMPRESSION_TYPE_BEST
        msg += "\n- Method %r will try all compression methods with 'fast' level and choose one with smaller result data." % constants.COMPRESSION_TYPE_FAST
    msg += "\nDefaults to %r." % constants.COMPRESSION_TYPE_NONE

    grp_compress.add_argument('--compress', dest='compression', metavar='METHOD', action="append", default=[constants.COMPRESSION_TYPE_NONE], help=msg)

    grp_compress.add_argument('--force-compress', dest='compression_forced', action="store_true", help="Force compression even if resulting data is bigger than original.")
    grp_compress.add_argument('--minimal-compress-size', dest='compression_minimal_size', metavar='BYTES', type=int, default=1024, help="Minimal block data size for compression. Defaults to 1024 bytes. Value -1 means auto - per method absolute minimum. Do not compress if data size is less than BYTES long. If not forced to.")
    grp_compress.add_argument('--minimal-compress-ratio', dest='compression_minimal_ratio', metavar='RATIO', type=float, default=0.05, help="Minimal data compression ratio. Defaults to 0.05 (5%%). Do not compress if ratio is less than RATIO. If not forced to.")

    levels = (constants.COMPRESSION_LEVEL_DEFAULT, constants.COMPRESSION_LEVEL_FAST, constants.COMPRESSION_LEVEL_NORM, constants.COMPRESSION_LEVEL_BEST)

    grp_compress.add_argument('--compression-level', dest='compression_level', metavar="LEVEL", default=constants.COMPRESSION_LEVEL_DEFAULT,
                        help="Compression level ratio: one of %s; or INT. Defaults to %r. Not all methods support this option." % (
                            ', '.join('%r' % lvl for lvl in levels), constants.COMPRESSION_LEVEL_DEFAULT
                        ))


    # Dynamically check for supported compression programs
    compression_progs = [constants.COMPRESSION_PROGS_NONE]
    for pname, opts in constants.COMPRESSION_PROGS.items():
        if which(pname):
            compression_progs.append(pname)

    msg = "R|Enable compression of snapshot sqlite database files using one of the supported compression programs: %s"
    msg %= ', '.join('%r' % mth for mth in compression_progs)
    msg += ".\nDefaults to %r." % constants.COMPRESSION_PROGS_DEFAULT
    grp_compress.add_argument('--sqlite-compression-prog', dest='sqlite_compression_prog', metavar='PROGNAME', default=constants.COMPRESSION_PROGS_DEFAULT, help=msg)

    grp_compress.add_argument('--recompress-on-fly', dest='compression_recompress_now', action="store_true", help="Do recompress blocks which compressed with deprecated compression method.")

    grp_compress.add_argument('--recompress-not-current', dest='compression_recompress_current', action="store_true",
                        help="Do recompress blocks which compressed with not currently selected compression method.")

    grp_compress.add_argument('--decompress-try-all', dest='decompress_try_all', action="store_true", help="Try to decompress blocks with every available method if stored one fails.")


    grp_profile = parser.add_argument_group('Profiling')
    # Dynamically check for profiling support.
    try:
        # Using __import__() here because of pyflakes.
        for p in 'cProfile', 'pstats': __import__(p)
        grp_profile.add_argument('--profile', action='store_true', default=False, help="Use the Python modules cProfile and pstats to create a profile of time spent in various function calls and print out a table of the slowest functions at exit (of course this slows everything down but it can nevertheless give a good indication of the hot spots).")
    except ImportError:
        logger.warning("No profiling support available, --profile option disabled.")
        logger.warning("If you're on Ubuntu try 'sudo apt-get install python-profiler'.")
        pass

    grp_profile.add_argument('-M', '--memory-usage', dest='memory_usage', help="Output into stderr memory statistics at the exit of process", action="store_true")


    grp_mount = parser.add_argument_group('Mounting')
    grp_mount.add_argument('-o', '--mountoption', help="specify mount option", action="append")

    grp_mount.add_argument('mountpoint', help="specify mount point")

    args = parser.parse_args()

    if args.profile:
        sys.stderr.write("Enabling profiling..\n")
        import cProfile, pstats
        profile = '.dedupsqlfs.cprofile-%i' % time()
        profiler = cProfile.Profile()
        result = profiler.runcall(fuse_mount, args, compression_methods)
        profiler.dump_stats(profile)
        sys.stderr.write("\n Profiling statistics:\n\n")
        s = pstats.Stats(profile)
        s.sort_stats('calls').print_stats(0.1)
        s.sort_stats('cumtime').print_stats(0.1)
        s.sort_stats('tottime').print_stats(0.1)
        os.unlink(profile)
    else:
        result = fuse_mount(args, compression_methods)

    return result

if __name__ == '__main__':
    sys.exit(main())

# vim: ts=2 sw=2 et
