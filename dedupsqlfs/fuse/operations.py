# -*- coding: utf8 -*-

# Imports. {{{1
import sys

# Try to load the required modules from Python's standard library.

try:
    from io import BytesIO
    import errno
    import hashlib
    from math import floor, ceil, modf
    import os
    import stat
    from time import time
    import traceback
except ImportError as e:
    msg = "Error: Failed to load one of the required Python modules! (%s)\n"
    sys.stderr.write(msg % str(e))
    sys.exit(1)

# Try to load bundled Python FUSE binding.
loaded = False
try:
    from _llfuse import loaded
    if loaded:
        from _llfuse import module as fuse
        from _llfuse import module as llfuse
except ImportError:
    sys.stderr.write("Error: Bundled The Python FUSE binding isn't compiled!\n" + \
        "If you're on Ubuntu try running `apt-get install libfuse-dev'\n"+
        " and `cd lid-dynload && python3 setup.py build_ext clean`.\n")

if not loaded:
    # Try to load the Python FUSE binding.
    try:
        import llfuse as fuse
    except ImportError:
        sys.stderr.write("Error: The Python FUSE binding isn't installed!\n" + \
            "If you're on Ubuntu try running `sudo -i apt-get install python3-pip'\n"+
            " and `sudo pip3 install llfuse`.\n")
        sys.exit(1)

FUSEError = fuse.FUSEError

# Local modules that are mostly useful for debugging.
from dedupsqlfs.lib import constants
from dedupsqlfs.my_formats import format_timespan
from dedupsqlfs.lib.cache.simple import CacheTTLseconds, CompressionSizesValue
from dedupsqlfs.lib.cache.storage import StorageTimeSize
from dedupsqlfs.lib.cache.index import IndexTime
from dedupsqlfs.lib.cache.inodes import InodesTime
from dedupsqlfs.fuse.subvolume import Subvolume
from dedupsqlfs.fuse.helpers.repr import entry_attributes_to_dict, setattr_fields_to_dict
from dedupsqlfs.fuse.helpers.report import ReportHelper
from dedupsqlfs import __fsversion__


class DedupOperations(llfuse.Operations):  # {{{1

    def __init__(self, **kwargs):  # {{{2

        llfuse.Operations.__init__(self, **kwargs)

        # Initialize instance attributes.
        self.block_partitions = 1

        self.block_size = constants.BLOCK_SIZE_DEFAULT
        self.hash_function = constants.HASH_FUNCTION_DEFAULT
        self.compression_method = constants.COMPRESSION_TYPE_NONE

        self.cache_enabled = True
        self.cache_gc_meta_last_run = time()
        self.cache_gc_block_write_last_run = time()
        self.cache_gc_block_writeSize_last_run = time()
        self.cache_gc_block_read_last_run = time()
        self.cache_gc_block_readSize_last_run = time()
        self.cache_meta_timeout = 10
        self.cache_block_write_timeout = 10
        self.cache_block_read_timeout = 10
        self.cache_block_write_size = -1
        self.cache_block_read_size = -1
        self.flush_interval = 5
        self.flushBlockSize_interval = 1

        self.subvol_uptate_last_run = time()

        self.cached_names = CacheTTLseconds()
        self.cached_name_ids = CacheTTLseconds()
        self.cached_nodes = CacheTTLseconds()
        self.cached_attrs = InodesTime()
        self.cached_xattrs = CacheTTLseconds()

        self.cached_hash_sizes = CacheTTLseconds()
        self.cached_hash_compress = CacheTTLseconds()

        self.cached_blocks = StorageTimeSize()
        self.cached_indexes = IndexTime()

        self.fs_mounted_at = time()
        self.mounted_subvolume = None
        self.mounted_subvolume_name = None

        self.link_mode = stat.S_IFLNK | 0o777

        self.root_mode = stat.S_IFDIR | 0o755


        self.reportHelper = ReportHelper(self)


        self._compression_types = {}
        self._compression_types_revert = {}

        self.manager = None

    # FUSE API implementation: {{{2

    def setApplication(self, application):
        """
        @type application: dedupsqlfs.fuse.dedupfs.DedupFS
        """
        self.application = application
        return self

    def getManager(self):
        """
        @return: DbManager
        @rtype: dedupsqlfs.db.sqlite.manager.DbManager | dedupsqlfs.db.mysql.manager.DbManager
        """

        if not self.manager:
            engine = self.getOption('storage_engine')
            if not engine:
                engine = 'sqlite'

            self.getLogger().info("Selected storage engine: %s" % engine)

            dp = self.getOption("data")
            self.getLogger().info("Current database path: %r" % dp)

            if engine == 'mysql':
                from dedupsqlfs.db.mysql.manager import DbManager
                self.manager = DbManager(dbname=self.getOption("name"))
            elif engine == 'sqlite':
                from dedupsqlfs.db.sqlite.manager import DbManager
                self.manager = DbManager(dbname=self.getOption("name"))
            elif engine == 'auto':
                # Only for tools and for existing fs
                from dedupsqlfs.db.sqlite.manager import DbManager
                self.manager = DbManager(dbname=self.getOption("name"))
                self.manager.setBasePath(os.path.expanduser(dp))
                engine = "sqlite"
                if not self.manager.isSupportedStorage():
                    from dedupsqlfs.db.mysql.manager import DbManager
                    self.manager = DbManager(dbname=self.getOption("name"))
                    self.manager.setBasePath(os.path.expanduser(dp))
                    engine = "mysql"
                    if not self.manager.isSupportedStorage():
                        raise RuntimeError("Unsupported storage on %r" % dp)

            else:
                raise ValueError("Unknown storage engine: %r" % engine)

            dpc = self.getOption("data_clustered")
            if not dpc:
                dpc = dp

            self.getLogger().info("Clustered database path: %r" % dpc)

            self.manager.setLogger(self.getLogger())
            self.manager.setTableEngine(self.getOption('table_engine'))
            self.manager.setSynchronous(self.getOption("synchronous"))
            self.manager.setAutocommit(self.getOption("use_transactions"))
            self.manager.setBasePath(os.path.expanduser(dp))
            self.manager.setClusterPath(os.path.expanduser(dpc))
            self.manager.begin()

            self.getLogger().info("Databases engine: %s" % engine)
            self.getLogger().info("Databases module version: %s" % self.manager.getModuleVersion())
            self.getLogger().info("Databases engine version: %s" % self.manager.getEngineVersion())

            from dedupsqlfs.db.migration import DbMigration
            migr = DbMigration(self.manager, self.getLogger())
            if migr.isMigrationNeeded():
                self.getLogger().info("FS databases need to process migrations.")
                migr.process()
            if migr.isMigrationNeeded():
                self.getLogger().error("FS databases need to process migrations! They not (all) applyed!")
                raise OSError("FS DB not migrated!")

            self.flushCompressionType()

        return self.manager

    def getTable(self, table_name):
        if self.mounted_subvolume and table_name in ("tree", "inode", "link", "xattr", "inode_hash_block", "inode_option"):
            table_name += "_%d" % self.mounted_subvolume["id"]
        t = self.getManager().getTable(table_name)
        if self.getOption("verbosity") < 2:
            t.setEnableTimers(False)
        else:
            t.setEnableTimers()
        return t

    def getApplication(self):
        return self.application

    def setReadonly(self, flag=True):
        self.application.setReadonly(flag)
        return self

    def isReadonly(self):
        return self.application.isReadonly()

    def getOption(self, key):
        return self.application.getOption(key)

    def getLogger(self):
        return self.application.logger

    def flushCaches(self):
        return self.__cache_meta_hook() + self.__cache_block_hook()

    def flushCompressionType(self):
        if self._compression_types:
            self._compression_types = None
        if self._compression_types_revert:
            self._compression_types_revert = None
        return

    def getCompressionTypeName(self, comp_id):
        if not self._compression_types:
            self._compression_types = self.getManager().getTable("compression_type").getAll()
        return self._compression_types[comp_id]

    def getCompressionTypeId(self, name):
        if not self._compression_types_revert:
            self._compression_types_revert = self.getManager().getTable("compression_type").getAllRevert()
        return self._compression_types_revert[name]

    def getCompressionTypeIds(self):
        if not self._compression_types:
            self._compression_types = self.getManager().getTable("compression_type").getAll()
        return set(self._compression_types.keys())

    # --------------------------------------------------------------------------------------
    #       FUSE OPERATIONS
    # --------------------------------------------------------------------------------------

    def access(self, inode, mode, ctx):  # {{{3
        """
        Check inode access

        @param  inode:
        @type   inode: int

        @param  mode:
        @type   mode: int

        @param  ctx:
        @type   ctx: llfuse.RequestContext

        @return:
        @rtype: bool
        """
        self.getLogger().logCall('access', '->(inode=%i, mode=%o)', inode, mode)
        if mode != os.F_OK and not self.__access(inode, mode, ctx):
            return False
        return True

    def create(self, inode_parent, name, mode, flags, ctx):
        """
        Create file 'name' in 'inode_parent' directory with 'mode' by 'flags' and 'ctx'

        @param  inode_parent:
        @type   inode_parent: int

        @param  name:
        @type   name: bytes

        @param  mode:
        @type   mode: int

        @param  flags:
        @type   flags: int

        @param  ctx:
        @type   ctx: llfuse.RequestContext

        @return: @raise FUSEError:
        @rtype: tuple
        """
        self.getLogger().logCall('create', '->(inode_parent=%i, name=%r, mode=%o, flags=%o)',
                        inode_parent, name, mode, flags)
        if self.isReadonly(): return errno.EROFS

        try:
            node = self.__get_tree_node_by_parent_inode_and_name(inode_parent, name)
        except FUSEError as e:
            node = None
            self.getLogger().logCall('create', '-- node with that name not found, creating...')
            if e.errno != errno.ENOENT:
                raise

        if not node:
            inode, parent_ino = self.__insert(inode_parent, name, mode, 0, ctx)
        else:
            if flags & os.O_CREAT and flags & os.O_EXCL:
                self.getLogger().logCall('open', '-- exception for existed file! cant create!')
                raise FUSEError(errno.EIO)

            inode = node["inode_id"]

        fh = self.open(inode, flags, ctx)
        attrs = self.__getattr(inode)

        self.__cache_meta_hook()

        self.getLogger().logCall('create', '<-(created inode=%i, attrs=%r)', fh, entry_attributes_to_dict(attrs))

        return fh, attrs

    def destroy(self):  # {{{3

        # Stop flushing thread if it started
        self.getApplication().stopCacheFlusher()

        self.getApplication().addLockMessage("destroy")

        try:
            self.getLogger().logCall('destroy', '->()')
            self.getLogger().debug("Umount file system in process...")
            if not self.getOption("readonly"):

                # Flush all cached blocks
                self.getLogger().debug("Flush remaining inodes.")
                count = self.__flush_expired_inodes(self.cached_attrs.clear())
                self.getLogger().debug("-- flushed: %d" % count)
                self.getLogger().debug("Flush remaining blocks.")
                count = self.__flush_old_cached_blocks(self.cached_blocks.clear())
                self.getLogger().debug("-- flushed: %d" % count)
                self.cached_indexes.clear()

                self.getLogger().debug("Committing outstanding changes.")
                self.getManager().commit()

            if self.getOption("verbosity") > 1:
                self.getLogger().info("Umount file system: final statistics")
                self.reportHelper.do_print_stats_ontime(True)

            self.getManager().getTable('option').update('mounted', 0)
            self.getManager().commit()

            self.getManager().close()
        except Exception as e:
            # Should not raise here anything!
            self.__except_to_status('destroy', e, errno.EIO)

        self.getApplication().addLockMessage("destroy-done")

        return 0

    def flush(self, fh):
        try:
            self.getLogger().logCall('flush', '->(fh=%i)', fh)

            attr = self.__get_inode_row(fh)
            self.getLogger().logCall('flush', '-- inode(%i) size=%i', fh, attr["size"])
            if not attr["size"]:
                self.getLogger().logCall('flush', '-- inode(%i) zero sized! remove all blocks', fh)
                self.getTable("inode_hash_block").delete(fh)
                self.cached_blocks.forget(fh)
            else:
                self.cached_blocks.flush(fh)
            self.cached_attrs.flush(fh)
            self.cached_indexes.expire(fh)

            self.__cache_meta_hook()
            self.__cache_block_hook()
        except FUSEError:
            raise
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('flush', e, errno.EIO)

    def forget(self, inode_list):
        """
        Should not raise any excepton here
        @param inode_list: inode_list is a list of (inode, nlookup) tuples. 
        @return: 
        """
        try:
            self.getLogger().logCall('forget', '->(inode_list=%r)', inode_list)
            # clear block cache
            for ituple in inode_list:
                self.cached_attrs.expire(ituple[0])
                self.cached_blocks.expire(ituple[0])
                self.cached_indexes.expire(ituple[0])
        except FUSEError:
            pass
        except Exception as e:
            self.__rollback_changes()
            self.__except_to_status('forget', e, errno.EIO)

    def fsync(self, fh, datasync):
        """
        Sync inode metadata

        @param fh: inode
        @param datasync: force sync data
        @return: None
        """
        self.getLogger().logCall('fsync', '->(fh=%i, datasync=%r)', fh, datasync)

        if self.isReadonly():
            raise FUSEError(errno.EROFS)

        try:
            attr = self.__get_inode_row(fh)
            self.getLogger().logCall('fsync', '-- inode(%i) size=%i', fh, attr["size"])
            if not attr["size"]:
                self.getLogger().logCall('fsync', '-- inode(%i) zero sized! remove all blocks', fh)
                self.getTable("inode_hash_block").delete(fh)
                self.cached_blocks.forget(fh)
                self.cached_indexes.expire(fh)
            else:
                if datasync:
                    self.cached_blocks.flush(fh)
                    self.cached_indexes.expire(fh)

            self.cached_attrs.flush(fh)

            self.__cache_meta_hook()
            self.__cache_block_hook()
        except FUSEError:
            raise
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('fsync', e, errno.EIO)

    def fsyncdir(self, fh, datasync):
        self.getLogger().logCall('fsyncdir', '->(fh=%i, datasync=%r)', fh, datasync)
        if self.isReadonly():
            raise FUSEError(errno.EROFS)
        if datasync:
            # flush directory content
            for name, attr, node in self.readdir(fh, 0):
                self.cached_attrs.flush(attr.st_ino)
        else:
            # flush directory itself
            self.cached_attrs.flush(fh)

        self.__cache_meta_hook()

    def getattr(self, inode, ctx):  # {{{3
        """
        Get inode attributes

        @param  inode:
        @type   inode: int

        @param  ctx:
        @type   ctx: llfuse.RequestContext

        @return:
        @rtype: llfuse.EntryAttributes
        """
        self.getLogger().logCall('getattr', '->(inode=%r)', inode)
        v = self.__get_inode_row(inode)
        attr = self.__fill_attr_inode_row(v)
        self.getLogger().logCall('getattr', '<-(inode=%r, attr=%r)', inode, v)
        return attr

    def getxattr(self, inode, name, ctx):  # {{{3
        """
        Get inode attributes

        @param  inode:
        @type   inode: int

        @param  name:
        @type   name: bytes

        @param  ctx:
        @type   ctx: llfuse.RequestContext

        @return:
        @rtype: bytes
        """
        self.getLogger().logCall('getxattr', '->(inode=%r, name=%r)', inode, name)

        xattrs = self.__get_cached_xattrs(inode)
        if not xattrs:
            raise FUSEError(llfuse.ENOATTR)
        if name not in xattrs:
            raise FUSEError(llfuse.ENOATTR)
        return xattrs[name]

    def init(self):  # {{{3
        try:
            # Disable log for fuse functions
            if self.getOption("verbosity") < 2:
                self.getLogger().addCallToFilter("none")

            # Process the custom command line options defined in __init__().
            if self.getOption("block_partitions") is not None:
                self.block_partitions = int(self.getOption("block_partitions"))

            if self.getOption("block_size") is not None:
                self.block_size = int(self.getOption("block_size"))

            if self.block_size < constants.BLOCK_SIZE_MIN:
                self.getLogger().warn("Block size less than minimal! (%i<%i) Set to default minimal.",
                    self.block_size, constants.BLOCK_SIZE_MIN
                )
                self.block_size = constants.BLOCK_SIZE_MIN

            if self.block_size > constants.BLOCK_SIZE_MAX:
                self.getLogger().warn("Block size more than maximal! (%i>%i) Set to default maximal.",
                    self.block_size, constants.BLOCK_SIZE_MAX
                )
                self.block_size = constants.BLOCK_SIZE_MAX

            if self.getOption("compression_method") is not None:
                self.compression_method = self.getOption("compression_method")
            if self.getOption("hash_function") is not None:
                self.hash_function = self.getOption("hash_function")

            if self.getOption("use_cache") is not None:
                self.cache_enabled = self.getOption("use_cache")
            if self.getOption("cache_block_write_timeout") is not None:
                self.cache_block_write_timeout = self.getOption("cache_block_write_timeout")
            if self.getOption("cache_block_read_timeout") is not None:
                self.cache_block_read_timeout = self.getOption("cache_block_read_timeout")
            if self.getOption("cache_block_write_size") is not None:
                self.cache_block_write_size = self.getOption("cache_block_write_size")
            if self.getOption("cache_block_read_size") is not None:
                self.cache_block_read_size = self.getOption("cache_block_read_size")
            if self.getOption("cache_meta_timeout") is not None:
                self.cache_meta_timeout = self.getOption("cache_meta_timeout")
            if self.getOption("flush_interval") is not None:
                self.flush_interval = self.getOption("flush_interval")

            if self.getOption("verbosity") < 2:
                self.cached_blocks.setEnableTimers(False)
                self.cached_indexes.setEnableTimers(False)
                self.cached_nodes.setEnableTimers(False)
                self.cached_names.setEnableTimers(False)
                self.cached_name_ids.setEnableTimers(False)
                self.cached_attrs.setEnableTimers(False)
                self.cached_xattrs.setEnableTimers(False)
                self.cached_hash_sizes.setEnableTimers(False)
                self.cached_hash_compress.setEnableTimers(False)
            else:
                self.cached_blocks.setEnableTimers()
                self.cached_indexes.setEnableTimers()
                self.cached_nodes.setEnableTimers()
                self.cached_names.setEnableTimers()
                self.cached_name_ids.setEnableTimers()
                self.cached_attrs.setEnableTimers()
                self.cached_xattrs.setEnableTimers()
                self.cached_hash_sizes.setEnableTimers()
                self.cached_hash_compress.setEnableTimers()

            if not self.cache_enabled:
                self.cached_blocks.setMaxReadTtl(0)
                self.cached_blocks.setMaxWriteTtl(0)
                self.cached_indexes.setMaxTtl(0)
                self.cached_nodes.set_max_ttl(0)
                self.cached_names.set_max_ttl(0)
                self.cached_name_ids.set_max_ttl(0)
                self.cached_attrs.set_max_ttl(0)
                self.cached_xattrs.set_max_ttl(0)
                self.cached_hash_sizes.set_max_ttl(0)
                self.cached_hash_compress.set_max_ttl(0)
            else:
                if self.block_size:
                    self.cached_blocks.setBlockSize(self.block_size)
                self.cached_blocks.setMaxWriteTtl(self.cache_block_write_timeout)
                self.cached_blocks.setMaxReadTtl(self.cache_block_read_timeout)

                self.cached_hash_sizes.set_max_ttl(self.cache_block_write_timeout)
                self.cached_hash_compress.set_max_ttl(self.cache_block_write_timeout)

                if self.cache_block_write_size:
                    if self.getOption("memory_limit") and not self.getOption("cache_block_write_size"):
                        if self.cache_block_write_size > 256*self.block_size or self.cache_block_write_size < 0:
                            self.cache_block_write_size = 256*self.block_size
                    self.cached_blocks.setMaxWriteCacheSize(self.cache_block_write_size)

                if self.cache_block_read_size:
                    if self.getOption("memory_limit") and not self.getOption("cache_block_read_size"):
                        if self.cache_block_read_size > 256*self.block_size or self.cache_block_read_size < 0:
                            self.cache_block_read_size = 256*self.block_size
                    self.cached_blocks.setMaxReadCacheSize(self.cache_block_read_size)

                self.cached_nodes.set_max_ttl(self.cache_meta_timeout)
                self.cached_names.set_max_ttl(self.cache_meta_timeout)
                self.cached_name_ids.set_max_ttl(self.cache_meta_timeout)
                self.cached_attrs.set_max_ttl(self.cache_meta_timeout)
                self.cached_xattrs.set_max_ttl(self.cache_meta_timeout)
                self.cached_indexes.setMaxTtl(self.cache_meta_timeout)

            if self.getOption("synchronous") is not None:
                self.synchronous = self.getOption("synchronous")
            if self.getOption("use_transactions") is not None:
                self.use_transactions = self.getOption("use_transactions")

            # Initialize the logging and database subsystems.
            self.getLogger().logCall('init', 'init()')

            # Disable synchronous operation. This is supposed to make SQLite perform
            # MUCH better but it has to be enabled wit --nosync because you might
            # lose data when the file system isn't cleanly unmounted...
            if not self.synchronous and not self.isReadonly():
                self.getLogger().warning("Warning: Disabling synchronous operation, you might lose data!")

            if not self.isReadonly():
                self.__init_store()

            self.mounted_subvolume_name = self.getOption("mounted_subvolume")
            if self.mounted_subvolume_name is not None:
                self.mounted_subvolume_name = b'' + self.mounted_subvolume_name.encode()

            self.__select_subvolume()
            self.__get_opts_from_db()
            # Make sure the hash function is (still) valid (since the database was created).

            try:
                # Get a reference to the hash function.
                hashlib.new(self.hash_function)
            except Exception:
                self.getLogger().critical("Error: The selected hash function %r doesn't exist!", self.hash_function)
                sys.exit(1)


            # NOT READONLY - AND - Mountpoint defined (mount action)
            self.getApplication().startCacheFlusher()


            if self.getApplication().mountpoint:
                self.getManager().getTable('option').update('mounted', 1)

            # Recalc page size for sqlite block table, based on block size
            blockTable = self.getManager().getTable('block')
            # Set partitioning
            blockTable.n_parts = self.block_partitions
            if self.block_partitions > 1:
                # force to create all tables
                blockTable.create()

            ps = blockTable.getPageSize()
            self.getLogger().debug("DedupFS: block table page_size=%r" % ps)
            if ps < self.block_size:
                self.getLogger().debug("DedupFS: set page_size to block_size %r" % self.block_size)
                blockTable.setPageSize(self.block_size)
                blockTable.close()
                blockTable.connect()
                ps = blockTable.getPageSize()
                self.getLogger().debug("DedupFS: result block table page_size=%r" % ps)
                ps = blockTable.getDbPageSize()
                self.getLogger().debug("DedupFS: real block table page_size=%r" % ps)

            self.getApplication().addLockMessage("inited")

            self.getLogger().debug("DedupFS: inited and mounted")
            return 0
        except Exception as e:
            self.__except_to_status('init', e, errno.EIO)
            # Bug fix: Break the mount point when initialization failed with an
            # exception, because self.conn might not be valid, which results in
            # an internal error message for every FUSE API call...
            raise e

    def link(self, inode, new_parent_inode, new_name, ctx):  # {{{3
        """
        Get inode attributes

        @param  inode:
        @type   inode: int

        @param  new_parent_inode:
        @type   new_parent_inode: int

        @param  new_name:
        @type   new_name: bytes

        @param  ctx:
        @type   ctx: llfuse.RequestContext

        @return:
        @rtype: llfuse.EntryAttributes
        """
        self.getLogger().logCall('link', '->(inode=%r, parent_inode=%r, new_name=%r)', inode, new_parent_inode, new_name)
        if self.isReadonly(): raise FUSEError(errno.EROFS)

        attr = self.__get_inode_row(inode)
        if attr["mode"] & stat.S_IFDIR:
            # No links to directory!
            raise FUSEError(errno.ENOTSUP)

        parent_node = self.__get_tree_node_by_inode(new_parent_inode)
        string_id = self.__intern(new_name)

        treeTable = self.getTable("tree")

        treeTable.insert(parent_node["id"], string_id, inode)
        attr["nlinks"] += 1
        self.cached_attrs.set(inode, attr, writed=True)

        self.__cache_meta_hook()

        return self.__fill_attr_inode_row(attr)

    def listxattr(self, inode, ctx):
        """
        Get inode attributes

        @param  inode:
        @type   inode: int

        @param  ctx:
        @type   ctx: llfuse.RequestContext

        @return: list of bytes
        @rtype: list
        """
        self.getLogger().logCall('listxattr', '->(inode=%r)', inode)
        xattrs = self.__get_cached_xattrs(inode)
        self.getLogger().logCall('listxattr', '<-(xattrs=%r)', xattrs)
        if not xattrs:
            return []
        return xattrs.keys()

    def lookup(self, parent_inode, name, ctx):
        """
        Look up a directory entry by name and get its attributes.

        @param  parent_inode:
        @type   parent_inode: int

        @param  name:
        @type   name: bytes

        @param  ctx:
        @type   ctx: llfuse.RequestContext

        @return:
        @rtype: llfuse.EntryAttributes
        """
        self.getLogger().logCall('lookup', '->(parent_inode=%r, name=%r)', parent_inode, name)

        if name == b'.':
            v = self.__get_inode_row(parent_inode)
        elif name == b'..':
            node = self.__get_tree_parent_node_by_inode(parent_inode)
            self.getLogger().logCall('lookup', '-- parent node=%r', node)
            v = self.__get_inode_row(node['inode_id'])
        else:
            node = self.__get_tree_node_by_parent_inode_and_name(parent_inode, name)
            self.getLogger().logCall('lookup', '-- node=%r', node)
            v = self.__get_inode_row(node["inode_id"])

        attr = self.__fill_attr_inode_row(v)

        self.getLogger().logCall('lookup', '<-(attr=%r)', v)

        self.__cache_meta_hook()
        return attr

    def mkdir(self, parent_inode, name, mode, ctx):  # {{{3
        """
        Create a directory

        @param  parent_inode:
        @type   parent_inode: int

        @param  name:
        @type   name: bytes

        @param  mode:
        @type   mode: int

        @param  ctx:
        @type   ctx: llfuse.RequestContext

        @return:
        @rtype: llfuse.EntryAttributes
        """
        if self.isReadonly(): raise FUSEError(errno.EROFS)

        try:
            self.getLogger().logCall('mkdir', '->(parent_inode=%i, name=%r, mode=%o)',
                            parent_inode, name, mode)

            nameTable = self.getTable("name")
            inodeTable = self.getTable("inode")
            treeTable = self.getTable("tree")
            # SIZE = name row size + tree row size + inode row size
            size = nameTable.getRowSize(name) + inodeTable.getRowSize() + treeTable.getRowSize()

            inode, parent_ino = self.__insert(parent_inode, name, mode | stat.S_IFDIR, size, ctx)

            parino = self.__get_inode_row(parent_inode)
            # inodeTable.inc_nlinks(parent_inode)
            parino["nlinks"] += 1
            self.cached_attrs.set(parent_inode, parino, True)

            self.__setattr_mtime(parent_inode)

            self.__cache_meta_hook()

            return self.__getattr(inode)
        except FUSEError:
            self.__rollback_changes()
            raise
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('mkdir', e, errno.EIO)

    def mknod(self, parent_inode, name, mode, rdev, ctx):  # {{{3
        """
        Create (possibly special) file

        @param  parent_inode:
        @type   parent_inode: int

        @param  name:
        @type   name: bytes

        @param  mode:
        @type   mode: int

        @param  rdev:
        @type   rdev: int

        @param  ctx:
        @type   ctx: llfuse.RequestContext

        @return:
        @rtype: llfuse.EntryAttributes
        """
        if self.isReadonly():
            raise FUSEError(errno.EROFS)

        try:
            self.getLogger().logCall('mknod', '->(parent_inode=%i, name=%r, mode=%o, rdev=%i)',
                            parent_inode, name, mode, rdev)

            inode, parent_ino = self.__insert(parent_inode, name, mode, 0, ctx, rdev)
            return self.__getattr(inode)
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('mknod', e, errno.EIO)

    def open(self, inode, flags, ctx):  # {{{3
        """
        Return filehandler ID
        """
        self.getLogger().logCall('open', '->(inode=%i, flags=%o)', inode, flags)
        # Make sure the file exists?

        if not self.isReadonly():
            self.__setattr_atime(inode)

        if flags & os.O_TRUNC:
            if self.isReadonly(): raise FUSEError(errno.EROFS)
            self.getLogger().logCall('open', '-- truncate file!')
            row = self.__get_inode_row(inode)
            row["size"] = 0
            self.cached_attrs.set(inode, row, writed=True)

        # Make sure the file is readable and/or writable.
        return inode

    def opendir(self, inode, ctx):  # {{{3
        self.getLogger().logCall('opendir', 'opendir(inode=%i)', inode)
        # Make sure the file exists?
        self.__get_tree_node_by_inode(inode)
        # Make sure the file is readable and/or writable.

        if not self.isReadonly():
            self.__setattr_atime(inode)

        return inode

    def read(self, fh, offset, size):  # {{{3
        """
        @param fh: file handler number - inode.id
        @type  fh: int
        """
        try:
            start_time = time()

            self.getLogger().logCall('read', '->(fh=%i, offset=%i, size=%i)', fh, offset, size, )

            row = self.__get_inode_row(fh)
            if row["size"] <= offset:
                self.getLogger().logCall('read', '-- oversized! inode(size)=%i', row["size"] )
                data = b''
            else:
                if row["size"] < offset + size:
                    size = row["size"] - offset
                    self.getLogger().logCall('read', '-- oversized! inode(size)=%i, corrected read size: %i', row["size"], size )
                data = self.__get_block_data_by_offset(fh, offset, size)
            lr = len(data)
            self.reportHelper.bytes_read += lr

            # Too much output
            # self.__log_call('read', 'readed: size=%s, data=%r', len(data), data, )
            self.getLogger().logCall('read', '<-readed: size=%s', lr, )

            self.__cache_block_hook()

            self.reportHelper.time_spent_reading += time() - start_time
            return data
        except Exception as e:
            return self.__except_to_status('read', e, code=errno.EIO)

    def readdir(self, fh, offset):  # {{{3
        """
        @param fh: file handler number - inode.id
        @type  fh: int
        """
        self.getLogger().logCall('readdir', '->(fh=%r, offset=%i)', fh, offset)

        inode = fh

        self.getLogger().logCall('readdir', '-- (inode=%r)', inode)

        cur_node = self.__get_tree_node_by_inode(inode)

        self.getLogger().logCall('readdir', '-- (node=%r)', cur_node)

        for node in self.getTable("tree").get_children(cur_node["id"], offset):
            #if node["id"] <= offset:
            #    continue
            name = self.__get_name_by_id(node['name_id'])
            irow = self.__get_inode_row(node["inode_id"])
            attrs = self.__fill_attr_inode_row(irow)
            self.getLogger().logCall('readdir', '<-(name=%r, attrs=%r, node=%i)',
                            name, irow, node["id"])
            yield (name, attrs, node["id"],)

    def readlink(self, inode, ctx):  # {{{3
        self.getLogger().logCall('readlink', '->(inode=%i)', inode)

        target = self.getTable("link").find_by_inode(inode)
        if not target:
            raise FUSEError(errno.ENOENT)
        return target

    def release(self, fh):  # {{{3
        self.getLogger().logCall('release', '->(fh=%i)', fh)
        #self.__flush_inode_cached_blocks(fh, clean=True)
        self.cached_blocks.expire(fh)
        self.cached_attrs.expire(fh)
        self.__cache_block_hook()
        self.__cache_meta_hook()
        return 0

    def releasedir(self, fh):
        self.getLogger().logCall('releasedir', '->(fh=%r)', fh)
        self.cached_attrs.expire(fh)
        self.__cache_meta_hook()
        return 0

    def removexattr(self, inode, name, ctx):
        """
        Remove extended attribute name of inode

        @param  inode:
        @type   inode: int

        @param  name:
        @type   name: bytes

        @param  ctx:
        @type   ctx: llfuse.RequestContext

        @return:
        @rtype: int
        """
        if self.isReadonly():
            raise FUSEError(errno.EROFS)

        try:
            self.getLogger().logCall('removexattr', '->(inode=%i, name=%r)', inode, name)

            xattrs = self.__get_cached_xattrs(inode)
            self.getLogger().logCall('removexattr', '==(xattrs=%r)', xattrs)
            if not xattrs:
                raise FUSEError(llfuse.ENOATTR)
            if name not in xattrs:
                raise FUSEError(llfuse.ENOATTR)
            del xattrs[name]
            self.getTable("xattr").update(inode, xattrs)
            self.cached_xattrs.set(inode, xattrs)
            self.getLogger().logCall('removexattr', '<-(xattrs=%r)', xattrs)
            self.__setattr_ctime(inode)
            return 0
        except FUSEError:
            raise
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('removexattr', e, errno.ENOENT)

    def rename(self, inode_parent_old, name_old, inode_parent_new, name_new, ctx):  # {{{3
        if self.isReadonly():
            raise FUSEError(errno.EROFS)

        try:
            self.getLogger().logCall('rename', '->(inode_parent_old=%i, name_old=%r, inode_parent_new=%i, name_new=%r)',
                            inode_parent_old, name_old, inode_parent_new, name_new)

            # Try to remove the existing target path (if if exists).
            # NB: This also makes sure target directories are empty.
            try:
                # Node exists?
                self.__get_tree_node_by_parent_inode_and_name(inode_parent_new, name_new)
                # Then unlink
                self.__remove(inode_parent_new, name_new, True)
            except FUSEError as e:
                # Ignore errno.ENOENT, re raise other exceptions.
                if e.errno != errno.ENOENT:
                    raise

            node_old = self.__get_tree_node_by_parent_inode_and_name(inode_parent_old, name_old)
            node_parent_new = self.__get_tree_node_by_inode(inode_parent_new)
            string_id = self.__intern(name_new)

            treeTable = self.getTable("tree")
            treeTable.rename_inode(node_old["id"], node_parent_new["id"], string_id)

            self.cached_nodes.unset("%i-%s" % (inode_parent_old, hashlib.md5(name_old).hexdigest()))
            if name_old != name_new:
                self.cached_names.unset(hashlib.md5(name_old).hexdigest())
                self.cached_name_ids.unset(node_old['name_id'])

            self.__cache_meta_hook()
        except FUSEError:
            self.__rollback_changes()
            raise
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('rename', e, errno.ENOENT)
        return 0

    def rmdir(self, inode_parent, name, ctx):  # {{{3
        if self.isReadonly():
            raise FUSEError(errno.EROFS)

        try:
            self.getLogger().logCall('rmdir', '->(inode_parent=%i, name=%r)', inode_parent, name)

            if not self.isReadonly():
                self.__setattr_mtime(inode_parent)

            self.__remove(inode_parent, name, check_empty=True)
            return 0
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('rmdir', e, errno.ENOENT)

    def setattr(self, inode, attr, fields, fh, ctx):
        """
        @param  inode:  inode ID
        @type   inode:  int

        @param  attr:   attributes
        @type   attr:   llfuse.EntryAttributes

        @param  fields: changed fields
        @type   fields: llfuse.SetattrFields

        @param  fh:     inode
        @type   fh:     int|None

        @param  ctx:
        @type   ctx: llfuse.RequestContext

        @return:
        @rtype: llfuse.EntryAttributes
        """
        if self.isReadonly():
            raise FUSEError(errno.EROFS)

        try:
            if self.getOption('verbosity') >= 3:
                self.getLogger().logCall('setattr', '->(inode=%i, attr=%r, fields=%r)',
                                         inode,
                                         entry_attributes_to_dict(attr),
                                         setattr_fields_to_dict(fields))

            row = self.__get_inode_row(inode)
            self.getLogger().debug("-- current row: %r", row)

            update_db = False
            new_data = {}

            # Emulate truncate
            if fields.update_size:
                new_data["size"] = attr.st_size
                if row["size"] > attr.st_size:
                    new_data["truncated"] = True
                update_db = True

            if fields.update_mode:
                new_data["mode"] = attr.st_mode
                update_db = True

            if fields.update_uid:
                new_data["uid"] = attr.st_uid
                update_db = True

            if fields.update_gid:
                new_data["gid"] = attr.st_gid
                update_db = True

            if fields.update_atime and not self.getOption('noatime'):
                new_data["atime"] = attr.st_atime_ns
                update_db = True

            if fields.update_mtime:
                new_data["mtime"] = attr.st_mtime_ns
                update_db = True

            if update_db:
                new_data["ctime"] = self.newctime64()

                self.getLogger().debug("-- new attrs: %r", new_data)

                row.update(new_data)
                self.getLogger().debug("-- new row: %r", row)

                self.cached_attrs.set(inode, row, writed=True)

            self.__cache_meta_hook()

            return self.__fill_attr_inode_row(row)
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('setattr', e, errno.EIO)

    def __setattr_atime(self, inode):
        """
        :param  inode:  inode ID
        :type   inode:  int
        """
        if self.getOption('noatime'):
            return
        try:
            row = self.__get_inode_row(inode)
            row["atime"] = self.newctime64()
            self.cached_attrs.set(inode, row, writed=True)
            self.__cache_meta_hook()
        except Exception as e:
            raise self.__except_to_status('__setattr_atime', e, errno.EIO)

    def __setattr_ctime(self, inode):
        """
        :param  inode:  inode ID
        :type   inode:  int
        """
        try:
            row = self.__get_inode_row(inode)
            row["ctime"] = self.newctime64()
            self.cached_attrs.set(inode, row, writed=True)
            self.__cache_meta_hook()
        except Exception as e:
            raise self.__except_to_status('__setattr_ctime', e, errno.EIO)

    def __setattr_mtime(self, inode):
        """
        :param  inode:  inode ID
        :type   inode:  int
        """
        try:
            row = self.__get_inode_row(inode)
            row["mtime"] = self.newctime64()
            self.cached_attrs.set(inode, row, writed=True)
            self.__cache_meta_hook()
        except Exception as e:
            raise self.__except_to_status('__setattr_mtime', e, errno.EIO)

    def setxattr(self, inode, name, value, ctx):
        if self.isReadonly():
            raise FUSEError(errno.EROFS)

        try:
            self.getLogger().logCall('setxattr', '->(inode=%i, name=%r, value=%r)', inode, name, value)

            xattrs = self.__get_cached_xattrs(inode)

            newxattr = False
            if not xattrs:
                newxattr = True
                xattrs = {}

            self.getLogger().logCall('setxattr', '==(xattrs=%r)', xattrs)

            # Don't do DB write if value not changed
            if xattrs.get(name) != value:
                xattrs[name] = value

                if not newxattr:
                    self.getTable("xattr").update(inode, xattrs)
                else:
                    self.getTable("xattr").insert(inode, xattrs)

                self.__setattr_ctime(inode)

                self.getLogger().logCall('setxattr', '<-(xattrs=%r)', xattrs)

            # Update cache ttl
            self.cached_xattrs.set(inode, xattrs)

            return 0
        except FUSEError:
            raise
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('setxattr', e, errno.ENOENT)

    def stacktrace(self):
        self.getLogger().logCall('stacktrace', '->()')

    def statfs(self, ctx):  # {{{3
        try:
            self.getLogger().logCall('statfs', '->()')
            # Use os.statvfs() to report the host file system's storage capacity.
            host_fs = os.statvfs(os.path.expanduser( self.getOption("data")) )

            stats = llfuse.StatvfsData()

            subv = Subvolume(self)

            usage = subv.get_apparent_size_fast(self.mounted_subvolume_name)

            # The total number of free blocks available to a non privileged process.
            #stats.f_bavail = host_fs.f_bsize * host_fs.f_bavail / self.block_size
            #if stats.f_bavail < 0:
            #    stats.f_bavail = 0

            stats.f_bavail = 0

            # The total number of free blocks in the file system.
            #stats.f_bfree = host_fs.f_frsize * host_fs.f_bfree / self.block_size
            stats.f_bfree = 0

            # stats.f_bfree = stats.f_bavail
            # The total number of blocks in the file system in terms of f_frsize.
            # stats.f_blocks = stats.f_bavail + usage / self.block_size
            import math
            stats.f_blocks = int(math.ceil(1.0 * usage / self.block_size))
            #if stats.f_blocks < 0:
            #    stats.f_blocks = 0

            stats.f_bsize = self.block_size # The file system block size in bytes.
            stats.f_favail = 0 # The number of free file serial numbers available to a non privileged process.
            stats.f_ffree = 0 # The total number of free file serial numbers.
            stats.f_files = 0 # The total number of file serial numbers.
            # File system flags. Symbols are defined in the <sys/statvfs.h> header file to refer to bits in this field (see The f_flags field).
            stats.f_frsize = self.block_size # The fundamental file system block size in bytes.
            stats.f_namemax = 65000
            return stats
        except Exception as e:
            raise self.__except_to_status('statfs', e, errno.EIO)

    def symlink(self, inode_parent, name, target, ctx):  # {{{3
        try:
            self.getLogger().logCall('symlink', '->(inode_parent=%i, name=%r, target=%r)',
                            inode_parent, name, target)
            if self.isReadonly():
                raise FUSEError(errno.EROFS)

            # Create an inode to hold the symbolic link.
            inode, parent_ino = self.__insert(inode_parent, name, self.link_mode, len(target), ctx)
            # Save the symbolic link's target.
            self.getTable("link").insert(inode, target)
            attr = self.__getattr(inode)
            self.__cache_meta_hook()
            return attr
        except FUSEError:
            raise
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('symlink', e, errno.EIO)

    def unlink(self, parent_inode, name, ctx):  # {{{3
        try:
            self.getLogger().logCall('unlink', '->(parent_inode=%i, name=%r)', parent_inode, name)
            if self.isReadonly():
                raise FUSEError(errno.EROFS)

            self.__setattr_mtime(parent_inode)

            self.__remove(parent_inode, name)
            self.__cache_meta_hook()
        except FUSEError:
            raise
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('unlink', e, errno.EIO)

    def write(self, fh, offset, buf):  # {{{3
        if self.isReadonly():
            raise FUSEError(errno.EROFS)

        try:
            start_time = time()

            self.getLogger().logCall('write', '->(fh=%i, offset=%i)', fh, offset)

            length = self.__write_block_data_by_offset(fh, offset, buf)

            self.getLogger().logCall('write', 'length(writed)=%i', length)

            attrs = self.__get_inode_row(fh)
            if attrs["size"] < offset + length:
                attrs["size"] = offset + length

            attrs["mtime"] = self.newctime64()

            self.cached_attrs.set(fh, attrs, writed=True)

            self.__cache_meta_hook()
            self.__cache_block_hook()

            self.reportHelper.time_spent_writing += time() - start_time
            return length
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('write', e, errno.EIO)


    # ---------------------------- Miscellaneous methods: {{{2

    def __update_mounted_subvolume_time(self):
        t_now = time()
        if t_now - self.subvol_uptate_last_run < 1:
            return self

        self.subvol_uptate_last_run = t_now

        if self.mounted_subvolume and self.getApplication().mountpoint:
            self.getTable('subvolume').update_time(self.mounted_subvolume["id"])
        return self

    def __get_cached_xattrs(self, inode):
        xattrs = self.cached_xattrs.get(inode)
        if xattrs is None:
            xattrs = self.getTable("xattr").find(inode)
            if xattrs:
                self.cached_xattrs.set(inode, xattrs)
            else:
                self.cached_xattrs.set(inode, False)
        return xattrs

    def __get_id_by_name(self, name):
        self.getLogger().logCall('__get_id_by_name', '->(name=%r)', name)

        xname = hashlib.md5(name).hexdigest()
        name_id = self.cached_names.get(xname)
        if not name_id:
            name_id = self.getTable("name").find(name)
            if name_id:
                self.cached_names.set(xname, name_id)
                self.cached_name_ids.set(name_id, name)
        if not name_id:
            self.getLogger().debug("! No name %r found, cant find name.id" % name)
            raise FUSEError(errno.ENOENT)
        return name_id

    def __get_name_by_id(self, name_id):
        self.getLogger().logCall('__get_name_by_id', '->(name_id=%r)', name_id)

        name = self.cached_name_ids.get(name_id)
        if not name:
            name = self.getTable("name").get(name_id)
            if name:
                self.cached_names.set(hashlib.md5(name).hexdigest(), name_id)
                self.cached_name_ids.set(name_id, name)
        if not name:
            self.getLogger().debug("! No name for %r found, cant find name" % name_id)
            raise FUSEError(errno.ENOENT)
        return name

    def __get_tree_node_by_parent_inode_and_name(self, parent_inode, name):
        self.getLogger().logCall('__get_tree_node_by_parent_inode_and_name', '->(parent_inode=%i, name=%r)', parent_inode, name)

        xname = hashlib.md5(name).hexdigest()
        xkey = "%i-%s" % (parent_inode, xname)
        node = self.cached_nodes.get(xkey)

        if not node:

            name_id = self.__get_id_by_name(name)

            par_node = self.__get_tree_node_by_inode(parent_inode)
            if not par_node:
                self.getLogger().debug("! No parent inode %i found, cant get tree node", parent_inode)
                raise FUSEError(errno.ENOENT)

            node = self.getTable("tree").find_by_parent_name(par_node["id"], name_id)
            if not node:
                self.getLogger().debug("! No node %i and name %i found, cant get tree node", par_node["id"], name_id)
                raise FUSEError(errno.ENOENT)

            self.cached_nodes.set(xkey, node)

        return node

    def __get_tree_node_by_inode(self, inode):
        self.getLogger().logCall('__get_tree_node_by_inode', '->(inode=%i)', inode)

        node = self.cached_nodes.get(inode)

        if not node:

            node = self.getTable("tree").find_by_inode(inode)
            if not node:
                self.getLogger().debug("! No inode %i found, cant get tree node", inode)
                raise FUSEError(errno.ENOENT)

            self.cached_nodes.set(inode, node)

        return node

    def __get_tree_parent_node_by_inode(self, inode):
        self.getLogger().logCall('__get_tree_parent_node_by_inode', '->(inode=%i)', inode)

        node = self.__get_tree_node_by_inode(inode)
        node = self.getTable("tree").get(node['parent_id'])
        if not node:
            self.getLogger().debug("! No parent node found for inode %i", inode)
            raise FUSEError(errno.ENOENT)
        return node

    def __get_inode_row(self, inode_id):
        self.getLogger().logCall('__get_inode_row', '->(inode=%i)', inode_id)
        row = self.cached_attrs.get(inode_id)
        if not row:
            row = self.getTable("inode").get(inode_id)
            if not row:
                self.getLogger().debug("! No inode %i found, cant get row", inode_id)
                raise FUSEError(errno.ENOENT)

            self.cached_attrs.set(inode_id, row)

        self.getLogger().logCall('__get_inode_row', '<-(row=%r)', row)
        return row

    def __fill_attr_inode_row(self, row):  # {{{3
        """
        Fill instance of llfuse.EntryAttributes with inodeTable row

        @param row:
        @type  row: dict

        @return:
        @rtype: llfuse.EntryAttributes
        """
        self.getLogger().logCall('__fill_attr_inode_row', '->(row=%r)', row)

        result = llfuse.EntryAttributes()

        result.entry_timeout = self.getOption("cache_meta_timeout")
        result.attr_timeout = self.getOption("cache_meta_timeout")
        result.st_ino = int(row["id"])
        # http://stackoverflow.com/questions/11071996/what-are-inode-generation-numbers
        result.generation = 0
        result.st_nlink = int(row["nlinks"])
        result.st_mode = int(row["mode"])
        result.st_uid = int(row["uid"])
        result.st_gid = int(row["gid"])
        result.st_rdev = int(row["rdev"])
        result.st_size = int(row["size"])
        result.st_atime_ns = int(row["atime"])
        result.st_mtime_ns = int(row["mtime"])
        result.st_ctime_ns = int(row["ctime"])
        result.st_blksize = int(self.block_size)
        result.st_blocks = int(result.st_size / self.block_size)
        return result

    def __getattr(self, inode_id):  # {{{3
        """
        Get inode row and fill llfuse.EntryAttributes

        @param inode_id:
        @type  inode_id: int

        @return:
        @rtype: llfuse.EntryAttributes
        """
        self.getLogger().logCall('__getattr', '->(inode=%i)', inode_id)
        row = self.__get_inode_row(inode_id)
        return self.__fill_attr_inode_row(row)

    def __get_index_from_cache(self, inode, block_number):
        self.getLogger().logCall('__get_index_from_cache', '->(inode=%i, block_number=%i)', inode, block_number)

        item = self.cached_indexes.get(inode, block_number)

        if item is None:

            self.getLogger().debug("get index from DB: inode=%i, number=%i", inode, block_number)

            tableIndex = self.getTable("inode_hash_block")

            item = tableIndex.get(inode, block_number)
            if not item:
                # Do hack here... store False to prevent table reread until it stored in cache or deleted
                self.getLogger().debug("-- new index")
                self.cached_indexes.set(inode, block_number, False)
            else:
                self.cached_indexes.set(inode, block_number, item)
        return item

    def __get_compression_type_by_hash_from_cache(self, hash_id):
        self.getLogger().logCall('__get_compression_type_by_hash_from_cache', '->(hash_id=%i)', hash_id)

        type_id = self.cached_hash_compress.get(hash_id)

        if type_id is None:

            self.getLogger().debug("get compression type from DB: hash_id=%i", hash_id)

            tableHCT = self.getTable("hash_compression_type")

            item = tableHCT.get(hash_id)
            if item:
                type_id = item["type_id"]
            self.cached_hash_compress.set(hash_id, type_id)
        return type_id

    def __get_sizes_by_hash_from_cache(self, hash_id):
        self.getLogger().logCall('__get_sizes_by_hash_from_cache', '->(hash_id=%i)', hash_id)

        citem = self.cached_hash_sizes.get(hash_id)

        if citem is None:

            self.getLogger().debug("get sizes for hash from DB: hash_id=%i", hash_id)

            tableHSZ = self.getTable("hash_sizes")

            item = tableHSZ.get(hash_id)
            citem = CompressionSizesValue()
            if item:
                citem = CompressionSizesValue(item["compressed_size"], item["writed_size"])
            self.cached_hash_sizes.set(hash_id, citem)
        return citem

    def __get_block_from_cache(self, inode, block_number):
        self.getLogger().logCall('__get_block_from_cache', '->(inode=%i, block_number=%i)', inode, block_number)

        block = self.cached_blocks.get(inode, block_number)

        if block is None:

            self.getLogger().debug("get block from DB: inode=%i, number=%i", inode, block_number)

            indexItem = self.__get_index_from_cache(inode, block_number)
            item = None

            recompress = False

            block = BytesIO()
            if not indexItem:
                self.getLogger().debug("-- new block")

            else:
                # Fully allocate block
                if int(indexItem["real_size"]):
                    # If we have real block size
                    block.write(b'\x00' * indexItem["real_size"])
                else:
                    # Missing size
                    # If there was migration
                    #
                    tableIndex = self.getTable("inode_hash_block")
                    """
                    :type tableIndex:   dedupsqlfs.db.sqlite.table.inode_hash_block.TableInodeHashBlock |
                                        dedupsqlfs.db.mysql.table.inode_hash_block.TableInodeHashBlock
                    """
                    # Else - try to calculate
                    irow = self.__get_inode_row(inode)
                    if irow["size"] <= self.block_size:
                        if irow["size"] > 0:
                            block.write(b'\x00' * irow["size"])
                            tableIndex.update_size(inode, block_number, irow["size"])
                    else:
                        if irow["size"] <= self.block_size * block_number:
                            # Last block?
                            sz = irow["size"] % self.block_size
                            block.write(b'\x00' * sz)
                            tableIndex.update_size(inode, block_number, sz)
                        else:
                            # Middle block
                            block.write(b'\x00' * self.block_size)
                            tableIndex.update_size(inode, block_number, self.block_size)

                tableBlock = self.getTable("block")

                item = tableBlock.get(indexItem["hash_id"])
                if not item:
                    self.getLogger().error("get block from DB: block not found! (inode=%i, block_number=%i, hash_id=%s)", inode, block_number, indexItem["hash_id"])
                    raise OSError("get block from DB: block not found!")

            # XXX: how block can be defragmented away?
            if item:
                compTypeId = self.__get_compression_type_by_hash_from_cache(indexItem["hash_id"])

                self.getLogger().debug("-- decompress block")
                self.getLogger().debug("-- in db size: %s", len(item["data"]))

                block.seek(0)

                compression = self.getCompressionTypeName(compTypeId)

                self.getLogger().debug("READ: Hash = %r, method = %r", indexItem["hash_id"], compression)

                tryAll = self.getOption('decompress_try_all')

                # Try all decompression methods
                if tryAll:
                    try:
                        bdata = self.__decompress(item["data"], compTypeId)
                    except:
                        bdata = False
                    if bdata is False:
                        for type_id in self.getCompressionTypeIds():
                            compressionT = self.getCompressionTypeName(type_id)
                            if compressionT == 'none':
                                continue
                            # Don't repeat
                            if type_id == compTypeId:
                                continue

                            try:
                                bdata = self.__decompress(item["data"], type_id)
                            except:
                                bdata = False
                            if bdata is not False:
                                # If stored wrong method - recompress
                                if type_id != compTypeId:
                                    self.getLogger().debug("-- Different compression types! Do recompress!")
                                    self.getLogger().debug("----   compressed with: %s", compression)
                                    self.getLogger().debug("---- decompressed with: %s", compressionT)
                                    recompress = True
                                break
                        if bdata is False:
                            self.getLogger().error("Can't decompress data block! Data corruption? Original method was: %s (%d)",
                                compression, compTypeId)
                            raise OSError("Can't decompress data block! Data corruption?")
                    block.write(bdata)

                else:
                    # If it fails - OSError raised
                    block.write(self.__decompress(item["data"], compTypeId))

                if compression != constants.COMPRESSION_TYPE_NONE:
                    if self.getOption('compression_recompress_now') and self.application.isDeprecated(compression):
                        recompress = True
                    if self.getOption('compression_recompress_current') and not self.application.isMethodSelected(compression):
                        recompress = True

                if recompress:
                    self.getLogger().debug("-- will recompress block")

                self.getLogger().debug("-- decomp size: %s", len(block.getvalue()))

            self.cached_blocks.set(inode, block_number, block, writed=recompress)
        return block

    def __get_block_data_by_offset(self, inode, offset, size):
        """
        @type inode: int

        @param offset: in file offset
        @type offset: int

        @param size: size of data to read
        @type size: int

        @return: bytes
        """
        self.getLogger().debug("__get_block_data_by_offset: inode = %s, offset = %s, size = %s", inode, offset, size)
        if not size:
            return b''

        inblock_offset = offset % self.block_size
        first_block_number = int(floor(1.0 * (offset - inblock_offset) / self.block_size))
        raw_data = BytesIO()

        # if we in the middle of a block by offset and read blocksize - need to read more then one...
        read_blocks = int(ceil(1.0 * (size + inblock_offset) / self.block_size))
        if not read_blocks:
            read_blocks = 1

        self.getLogger().debug("-- first block number = %s, read blocks = %s, inblock offset = %s", first_block_number, read_blocks, inblock_offset)

        readed_size = 0

        for n in range(read_blocks):

            block = self.__get_block_from_cache(inode, n + first_block_number)
            block.seek(0)

            read_size = size - readed_size
            if read_size > self.block_size:
                read_size = self.block_size
            if n == 0:
                block.seek(inblock_offset)
                if read_size > (self.block_size - inblock_offset):
                    read_size = self.block_size - inblock_offset

            raw_data.write(block.read(read_size))
            readed_size += read_size

        self.reportHelper.bytes_read += readed_size

        raw_value = raw_data.getvalue()

        self.getLogger().debug("-- readed size = %s", readed_size)
        self.getLogger().debug("-- raw data size = %s", len(raw_value))

        return raw_value

    def __write_block_data_by_offset(self, inode, offset, block_data):
        """
        @param inode: Inode ID
        @type inode: int

        @param offset: in file offset
        @type offset: int

        @param block_data: Data buffer - length can by more than block_size
        @type block_data: bytes
        """
        size = len(block_data)

        self.getLogger().debug("__write_block_data_by_offset: inode = %s, offset = %s, block size = %s", inode, offset, size)

        if not size:
            return 0

        inblock_offset = offset % self.block_size
        first_block_number = int(floor(1.0 * (offset - inblock_offset) / self.block_size))

        io_data = BytesIO(block_data)

        write_blocks = int(ceil(1.0 * size / self.block_size))
        if not write_blocks:
            write_blocks = 1

        self.getLogger().debug(
            "-- first block number = %s, size = %s, write blocks = %s, inblock offset = %s",
            first_block_number, size, write_blocks, inblock_offset
        )

        writed_size = 0

        for n in range(write_blocks):

            block = self.__get_block_from_cache(inode, n + first_block_number)
            block.seek(0)

            write_size = size - writed_size
            if write_size > self.block_size:
                write_size = self.block_size
            if n == 0:
                block.seek(inblock_offset)
                if write_size > (self.block_size - inblock_offset):
                    write_size = self.block_size - inblock_offset

            block.write(io_data.read(write_size))

            self.cached_blocks.set(inode, n + first_block_number, block, writed=True)

            writed_size += write_size

        self.getLogger().debug("-- writed size = %s", writed_size)

        return writed_size

    def __init_store(self):  # {{{3
        # Bug fix: At this point fuse.FuseGetContext() returns uid = 0 and gid = 0
        # which differs from the info returned in later calls. The simple fix is to
        # use Python's os.getuid() and os.getgid() library functions instead of
        # fuse.FuseGetContext().
        self.getLogger().debug("__init_store()")

        optTable = self.getTable("option")
        inited = optTable.get("inited")

        self.getLogger().debug("__init_store(): inited=%r", inited)

        if not inited:

            nameRoot = constants.ROOT_SUBVOLUME_NAME

            subv = Subvolume(self)
            self.mounted_subvolume = subv.create(nameRoot)

            self.mounted_subvolume_name = nameRoot

            for name in ("block_size", "block_partitions"):
                optTable.insert(name, "%i" % self.getOption(name))

            for name in ("hash_function",):
                optTable.insert(name, "%s" % self.getOption(name))

            optTable.insert("mounted_subvolume", self.mounted_subvolume_name)

            optTable.insert("fs_version", __fsversion__)

            from dedupsqlfs.db.migration import DbMigration
            migr = DbMigration(self.manager, self.getLogger())
            # Always use last migration number on new FS
            migr.setLastMigrationNumber()

            optTable.insert("mounted", 1)
            optTable.insert("inited", 1)

        migration = self.getTable("option").get("migration")
        self.getLogger().info("FS databases last migration: %s" % migration)

        for name in ("compression_method", "compression_level",):
            opt = optTable.get(name)
            popt = self.getOption(name)
            if opt is None:
                optTable.insert(name, "%s" % popt)
            else:
                optTable.update(name, "%s" % popt)

        for name in ("synchronous", "compression_forced",):
            opt = optTable.get(name)
            popt = self.getOption(name)
            if opt is None:
                optTable.insert(name, "%i" % popt)
            else:
                optTable.update(name, "%i" % popt)

        optTable.commit()

        return

    def __select_subvolume(self):

        optTable = self.getTable("option")
        subvTable = self.getTable('subvolume')

        self.getLogger().debug("__select_subvolume(1): mounted_subvolume=%r", self.mounted_subvolume_name)

        if self.mounted_subvolume_name:
            if subvTable.find(self.mounted_subvolume_name):
                pass
            else:
                self.getLogger().warning("__select_subvolume(1.2): subvolume not found, select default")
                self.mounted_subvolume_name = constants.ROOT_SUBVOLUME_NAME
        else:
            check = optTable.get("mounted_subvolume", True)
            if check:
                self.getLogger().debug("__select_subvolume(1.2): subvolume not found, select previous: %r", check)
                self.mounted_subvolume_name = check
            else:
                self.getLogger().warning("__select_subvolume(1.2): previous mounted subvolume not found, select default")
                self.mounted_subvolume_name = constants.ROOT_SUBVOLUME_NAME

        if self.mounted_subvolume_name:
            optTable.update("mounted_subvolume", self.mounted_subvolume_name)

            subvolItem = subvTable.find(self.mounted_subvolume_name)
            if subvolItem:
                self.mounted_subvolume = subvolItem
            else:
                subv = Subvolume(self)
                self.mounted_subvolume = subv.create(self.mounted_subvolume_name)

            if self.mounted_subvolume["readonly"]:
                self.application.setReadonly(True)

        if self.getApplication().mountpoint:
            subvTable.mount_time(self.mounted_subvolume["id"], int(time()))

        self.getLogger().debug("__select_snapshot(2): mounted_subvolume=%r", self.mounted_subvolume_name)

        return

    def __get_opts_from_db(self):  # {{{3
        options = self.getTable("option").getAll()
        self.getLogger().debug("Options in DB: %r", options)

        block_size = options.get("block_size")
        if block_size is not None:
            block_size = int(block_size)
            if block_size != self.block_size:
                self.getLogger().warning("Ignoring --block-size=%r argument, using previously chosen block size %i instead",
                    self.block_size, block_size)
                self.block_size = block_size
                self.cached_blocks.setBlockSize(self.block_size)

        hash_function = options.get("hash_function")
        if hash_function is not None and hash_function != self.hash_function:
            self.getLogger().warning("Ignoring --hash=%r argument, using previously chosen hash function %r instead",
                self.hash_function, hash_function)
            self.hash_function = hash_function

        parts = int(options.get("block_partitions"))
        if hash_function is not None and parts != self.block_partitions:
            self.getLogger().warning("Ignoring --block-partitions=%r argument, using previously chosen partitions number %r instead",
                self.block_partitions, parts)
            self.block_partitions = parts
        pass

    def __insert(self, parent_inode, name, mode, size, ctx, rdev=0):  # {{{3
        """

        @param parent_inode: int
        @param name: bytes
        @param mode: int
        @param size: int
        @param ctx: llfuse.RequestContext
        @param rdev: int
        @return:
        """
        self.getLogger().debug(
            "__insert->(parent_inode=%i,name=%r,mode=%o,size=%i,ctx.uid=%i,ctx.gid=%i)",
            parent_inode, name, mode, size, ctx.uid, ctx.gid
        )

        nlinks = mode & stat.S_IFDIR and 2 or 1

        inodeTable = self.getTable("inode")
        treeTable = self.getTable("tree")

        newt = self.newctime64()

        inode_id = inodeTable.insert(
            nlinks, mode, ctx.uid, ctx.gid, rdev, size,
            newt, newt, newt
        )

        name_id = self.__intern(name)

        par_node = self.__get_tree_node_by_inode(parent_inode)

        treeTable.insert(par_node["id"], name_id, inode_id)

        self.getLogger().debug("__insert<-(inode=%i,parent_inode=%i)", inode_id, par_node["inode_id"])

        return inode_id, par_node["inode_id"]

    def __intern(self, name): # {{{3
        """
        Search stored names

        @param name: bytes
        @return: int
        """
        name_id = self.cached_names.get(hashlib.md5(name).hexdigest())
        if not name_id:
            start_time = time()

            nameTable = self.getTable("name")
            name_id = nameTable.find(name)
            if not name_id:
                name_id = nameTable.insert(name)
            self.cached_names.set(hashlib.md5(name).hexdigest(), name_id)
            self.cached_name_ids.set(name_id, name)

            self.reportHelper.time_spent_interning += time() - start_time
        return int(name_id)

    def __remove(self, parent_inode, name, check_empty=False):  # {{{3
        """
        @param  check_empty:    Check is this directory not empty
        @param  check_empty:    bool
        """
        self.getLogger().debug("__remove->(parent_inode=%i,name=%r,check_empty=%r)", parent_inode, name, check_empty)

        cur_node = self.__get_tree_node_by_parent_inode_and_name(parent_inode, name)

        self.getLogger().debug("__remove -- (cur_node=%r)", cur_node)

        treeTable = self.getTable("tree")
        inodeTable = self.getTable("inode")

        attr = self.__get_inode_row(cur_node["inode_id"])

        # Make sure directories are empty before deleting them to avoid orphaned inodes.
        if check_empty:
            tree_inodes = treeTable.get_children_inodes(cur_node["id"])
            inodes = inodeTable.count_nlinks_by_ids(tree_inodes)
            if inodes:
                raise FUSEError(errno.ENOTEMPTY)
            else:
                if attr["nlinks"] > 0:
                    attr["nlinks"] -= 1
                    self.cached_attrs.set(cur_node["inode_id"], attr, writed=True)

        self.getLogger().debug("__remove -- (del tree node)")
        treeTable.delete(cur_node["id"])

        if attr["nlinks"] > 0:
            attr["nlinks"] -= 1
            self.cached_attrs.set(cur_node["inode_id"], attr, writed=True)

        # Inodes with nlinks = 0 are purged periodically from __collect_garbage() so
        # we don't have to do that here.

        self.cached_attrs.expire(cur_node["inode_id"])
        self.cached_xattrs.unset(cur_node["inode_id"])
        self.cached_nodes.unset("%i-%s" % (parent_inode, hashlib.md5(name).hexdigest()))
        self.cached_nodes.unset(cur_node["inode_id"])
        self.cached_names.unset(hashlib.md5(name).hexdigest())
        self.cached_name_ids.unset(cur_node["name_id"])
        self.cached_indexes.expire(cur_node["inode_id"])

        self.__cache_meta_hook()

        return

    def __access(self, inode, mode, ctx):  # {{{3
        """
        @param  inode:  inode.id
        @type   inode:  int

        @param  mode:   access mode
        @type   mode:   int

        @param  ctx:    request context
        @type   ctx:    llfuse.RequestContext
        """
        # Check if the flags include writing while the database is read only.
        if self.isReadonly() and mode & os.W_OK:
            return False
            # Get the path's mode, owner and group through the inode.
        attrs = self.__get_inode_row(inode)
        # Determine by whom the request is being made.

        self.getLogger().debug("__access: ctx.uid=%i, ctx.gid=%i", ctx.uid, ctx.gid)

        # Allow ROOT everything
        if ctx.uid == 0 or ctx.gid == 0:
            return True

        o = ctx.uid == attrs['uid'] # access by same user id?
        g = ctx.gid == attrs['gid'] and not o # access by same group id?
        # Note: "and not o" added after experimenting with EXT4.
        w = not (o or g) # anything else
        m = int(attrs['mode'])

        self.__cache_meta_hook()

        # The essence of UNIX file permissions. Did I miss anything?! (Probably...)
        return (not (mode & os.R_OK) or ((o and (m & 0o400)) or (g and (m & 0o040)) or (w and (m & 0o004)))) \
                and (not (mode & os.W_OK) or ((o and (m & 0o200)) or (g and (m & 0o020)) or (w and (m & 0o002)))) \
                and (not (mode & os.X_OK) or ((o and (m & 0o100)) or (g and (m & 0o010)) or (w and (m & 0o001))))

    def newctime64(self):  # {{{3
        t_ns, t_i = modf(time())
        t_ns = int(t_ns * 10**9)
        return int(t_i * 10**9) + t_ns

    def newctime64_32(self):  # {{{3
        t_ns, t_i = modf(time())
        t_ns = int(t_ns * 10**9)
        return int(t_i * 10**9) + t_ns, t_i

    def do_hash(self, data):  # {{{3
        start_time = time()
        digest = hashlib.new(self.hash_function, data).digest()
        self.reportHelper.time_spent_hashing += time() - start_time
        return digest

    def __decompress(self, block_data, compression_type_id):
        """
        @param block_data: bytes
        @param compression_type_id: int
        @return: bytes
        """
        start_time = time()
        compression = self.getCompressionTypeName( compression_type_id )
        self.getLogger().debug("-- decompress block: type = %s", compression)
        result = self.application.decompressData(compression, block_data)
        self.reportHelper.time_spent_decompressing += time() - start_time
        return result


    def __write_block_data(self, inode, block_number, block, blocks_from_cache={}):
        """
        @param  inode: inode ID
        @type   inode: int

        @param  block_number: block number
        @type   block_number: int

        @param  block: Block data
        @type   block: BytesIO
        """
        start_time = time()

        tableIndex = self.getTable("inode_hash_block")

        block.seek(0)
        data_block = block.getvalue()

        block_length = len(data_block)

        result = {
            "hash": None,
            "data": None,
            "new": False,
            "recompress": False,
            "update": False,
            "deleted": False,
            "inode": inode,
            "block_number": block_number,
            "real_size": block_length,
            "writed_size": block_length,
        }

        self.getLogger().debug("write block: inode=%s, block number=%s, data length=%s", inode, block_number, block_length)

        indexItem = self.__get_index_from_cache(inode, block_number)

        # Second sparse files variant = remove zero-bytes tail
        data_block = data_block.rstrip(b"\x00")

        block_length = len(data_block)

        # # First sparse files variant = flush zero-bytes string
        # sparsed_block = False
        # if block_length == 0:
        #     sparsed_block = True

        result["writed_size"] = block_length

        # self.getLogger().debug("write block: updated data length=%s, sparse=%r" % (block_length, sparsed_block,))
        #
        # if (block_length == 0 and indexItem) or sparsed_block:
        #     self.getLogger().debug("write block: remove empty or zero-filled block")
        #     tableIndex.delete_by_inode_number(inode, block_number)
        #     self.cached_indexes.expireBlock(inode, block_number)
        #
        #     result["deleted"] = True
        #
        #     self.time_spent_writing_blocks += time() - start_time
        #     return result

        tableHash = self.getTable("hash")

        hash_value = self.do_hash(data_block)
        self.getLogger().debug("-- hash_value: %r", hash_value)
        hash_id = tableHash.find(hash_value)
        self.getLogger().debug("-- hash_id: %r", hash_id)

        result["hash"] = hash_id

        # It is new block now?
        if not hash_id:

            result["new"] = True
            result["data"] = data_block

            self.getLogger().debug("-- insert new block data")
            hash_id = tableHash.insert(hash_value)
            self.getLogger().debug("-- hash_id: %r", hash_id)
            result["hash"] = hash_id

            self.reportHelper.bytes_written += block_length
        else:
            hash_CompressType_id = self.__get_compression_type_by_hash_from_cache(hash_id)

            # It may not be at this time because at first time only hashes
            # stored in DB. Compression and indexes are stored later.
            if hash_CompressType_id:
                compression = self.getCompressionTypeName(hash_CompressType_id)

                if compression != constants.COMPRESSION_TYPE_NONE:
                    if self.getOption('compression_recompress_now') and self.application.isDeprecated(compression):
                        self.getLogger().debug("FS thinks that compression %r is deprecated. Block data will be recompressed!", compression)
                        self.getLogger().debug("hash id: %s, value: %r, inode: %s, block-number: %s",
                            hash_id, hash_value, inode, block_number
                        )

                        result["recompress"] = True
                        result["data"] = data_block

                    if self.getOption('compression_recompress_current') and not self.application.isMethodSelected(compression):
                        self.getLogger().debug("FS thinks that compression %r is not selected. Block data will be recompressed!", compression)
                        self.getLogger().debug("hash id: %s, value: %r, inode: %s, block-number: %s",
                            hash_id, hash_value, inode, block_number
                        )

                        result["recompress"] = True
                        result["data"] = data_block

            if self.getOption('collision_check_enabled'):

                old_block = self.getTable("block").get(hash_id)
                if not old_block:
                    # Not written yeat
                    old_data = blocks_from_cache.get(hash_id)

                elif hash_CompressType_id:
                    old_data = self.__decompress(old_block["data"], hash_CompressType_id)
                    del old_block

                else:
                    old_data = b''

                # Is it exists? Not empty?
                if old_data:
                    if old_data != data_block:
                        self.getLogger().error("EEE: weird hashed data collision detected! hash id: %s, value: %r, inode: %s, block-number: %s",
                            hash_id, hash_value, inode, block_number
                        )
                        self.getLogger().warn("Use more strong hashing algo! I'm continue, but you are warned...")
                    old_hash = self.do_hash(old_data)
                    if old_hash != hash_value:
                        self.getLogger().error("Decompressed block data hash not equal with stored!")
                        self.getLogger().error("FS data corruption? Something wrong with db layer? I'm done with that!")
                        raise RuntimeError("Data corruption!")

            # Old hash found
            self.reportHelper.bytes_deduped += block_length

        if not indexItem:
            tableIndex.insert(
                inode, block_number, hash_id, result["real_size"]
            )
            indexItem = {
                "real_size": result["real_size"],
                "hash_id": hash_id
            }
            self.cached_indexes.set(inode, block_number, indexItem)
        elif indexItem["hash_id"] != hash_id or indexItem["real_size"] != result["real_size"]:
            tableIndex.update(
                inode, block_number, hash_id, result["real_size"]
            )
            indexItem.update({
                "real_size": result["real_size"],
                "hash_id": hash_id
            })
            self.cached_indexes.set(inode, block_number, indexItem)
            result["update"] = True

        self.reportHelper.time_spent_writing_blocks += time() - start_time
        return result

    def __flush_old_cached_blocks(self, cached_blocks, writed=False):
        count = 0

        blocksToCompress = {}
        blocksReCompress = {}
        blockSize = {}

        hashToBlock = {}

        for inode, inode_data in cached_blocks.items():
            for block_number, block_data in inode_data.items():
                if block_data.c_written:
                    block = block_data.c_block
                    item = self.__write_block_data(int(inode), int(block_number), block, hashToBlock)
                    if item["hash"] and (item["new"] or item["recompress"]):
                        blocksToCompress[ item["hash"] ] = item["data"]
                        blocksReCompress[ item["hash"] ] = item["recompress"]
                        blockSize[ item["hash"] ] = item["writed_size"]

                        if item["hash"] not in hashToBlock:
                            hashToBlock[ item["hash"] ] = item["data"]
                    if writed:
                        count += 1
                else:
                    if not writed:
                        count += 1

        tableBlock = self.getTable("block")
        tableHCT = self.getTable("hash_compression_type")
        tableHSZ = self.getTable("hash_sizes")

        self.application.getCompressTool().time_spent_compressing = 0

        for hash_id, cItem in self.application.compressData(blocksToCompress):
            cdata, cmethod = cItem

            self.getLogger().debug("WRITE: Hash = %r, method = %r", hash_id, cmethod)

            comp_size = len(cdata)

            writed_size = blockSize[ hash_id ]

            cmethod_id = self.getCompressionTypeId(cmethod)

            if blocksReCompress.get(hash_id, False) is True:
                tableBlock.update(hash_id, cdata)
            else:
                tableBlock.insert(hash_id, cdata)

            hash_CompressType_id = self.__get_compression_type_by_hash_from_cache(hash_id)
            if hash_CompressType_id:
                if hash_CompressType_id != cmethod_id:
                    tableHCT.update(hash_id, cmethod_id)
                    self.cached_hash_compress.set(hash_id, cmethod_id)
            else:
                tableHCT.insert(hash_id, cmethod_id)
                self.cached_hash_compress.set(hash_id, cmethod_id)

            hash_SZ = self.__get_sizes_by_hash_from_cache(hash_id)
            if hash_SZ and hash_SZ.size_c > 0 and hash_SZ.size_w > 0:
                if hash_SZ.size_c != comp_size or hash_SZ.size_w != writed_size:
                    tableHSZ.update(hash_id, writed_size, comp_size)
                    hash_SZ.size_c = comp_size
                    hash_SZ.size_w = writed_size
                    self.cached_hash_sizes.set(hash_id, hash_SZ)
            else:
                tableHSZ.insert(hash_id, writed_size, comp_size)
                hash_SZ.size_c = comp_size
                hash_SZ.size_w = writed_size
                self.cached_hash_sizes.set(hash_id, hash_SZ)

            self.reportHelper.bytes_written_compressed += comp_size

        self.reportHelper.time_spent_compressing += self.application.getCompressTool().time_spent_compressing

        return count

    def __cache_block_hook(self):  # {{{3

        self.__update_mounted_subvolume_time()

        start_time = time()
        flushed_writed_blocks = 0
        flushed_readed_blocks = 0
        flushed_writed_expiredByTime_blocks = 0
        flushed_readed_expiredByTime_blocks = 0
        flushed_writed_expiredBySize_blocks = 0
        flushed_readed_expiredBySize_blocks = 0

        start_time1 = time()
        if start_time1 - self.cache_gc_block_write_last_run >= self.flush_interval:

            expired = self.cached_blocks.expired()

            flushed_readed_blocks += expired[0]
            flushed_readed_expiredByTime_blocks += expired[0]

            flushed = self.__flush_old_cached_blocks(expired[1], True)
            flushed_writed_blocks += flushed
            flushed_writed_expiredByTime_blocks += flushed

            self.cache_gc_block_write_last_run = time()

            elapsed_time1 = self.cache_gc_block_write_last_run - start_time1

            self.reportHelper.time_spent_flushing_writed_block_cache += elapsed_time1
            self.reportHelper.time_spent_flushing_writedByTime_block_cache += elapsed_time1

        start_time1 = time()
        if start_time1 - self.cache_gc_block_writeSize_last_run >= self.flushBlockSize_interval:
            if self.cached_blocks.isWritedCacheFull():
                flushed = self.__flush_old_cached_blocks(self.cached_blocks.expireByCount(True), True)
                flushed_writed_blocks += flushed
                flushed_writed_expiredBySize_blocks += flushed

            self.cache_gc_block_writeSize_last_run = time()

            elapsed_time1 = self.cache_gc_block_writeSize_last_run - start_time1

            self.reportHelper.time_spent_flushing_writed_block_cache += elapsed_time1
            self.reportHelper.time_spent_flushing_writedBySize_block_cache += elapsed_time1

        start_time1 = time()
        if start_time1 - self.cache_gc_block_readSize_last_run >= self.flushBlockSize_interval:
            if self.cached_blocks.isReadCacheFull():
                flushed = self.cached_blocks.expireByCount(False)
                flushed_readed_blocks += flushed
                flushed_readed_expiredBySize_blocks += flushed

            self.cache_gc_block_readSize_last_run = time()

            elapsed_time1 = self.cache_gc_block_readSize_last_run - start_time1
            self.reportHelper.time_spent_flushing_readed_block_cache += elapsed_time1
            self.reportHelper.time_spent_flushing_readedBySize_block_cache += elapsed_time1

        if flushed_writed_blocks + flushed_readed_blocks > 0:

            self.__commit_changes()

            elapsed_time = time() - start_time

            self.reportHelper.time_spent_flushing_block_cache += elapsed_time

            self.getLogger().debug(
                "Block cache cleanup: flushed %i writed (%i/t, %i/sz), %i readed (%i/t, %i/sz) blocks in %s",
                flushed_writed_blocks, flushed_writed_expiredByTime_blocks, flushed_writed_expiredBySize_blocks,
                flushed_readed_blocks, flushed_readed_expiredByTime_blocks, flushed_readed_expiredBySize_blocks,
                format_timespan(elapsed_time)
            )

        self.reportHelper.do_print_stats_ontime()

        return flushed_readed_blocks + flushed_writed_blocks

    def __flush_expired_inodes(self, inodes):
        count = 0
        for inode_id, update_data in inodes.items():
            self.getLogger().debug("flush inode: %i = %r", int(inode_id), update_data)
            if "truncated" in update_data:
                del update_data["truncated"]
                self.__truncate_inode_blocks(inode_id, update_data["size"])
            count += self.getTable("inode").update_data(inode_id, update_data)
        return count

    def __truncate_inode_blocks(self, inode_id, size):

        inblock_offset = size % self.block_size
        max_block_number = int(floor(1.0 * (size - inblock_offset) / self.block_size))

        # 1. Remove blocks that has number more than MAX by size
        tableIndex = self.getTable("inode_hash_block")
        items = tableIndex.delete_by_inode_number_more(inode_id, max_block_number)
        for item in items:
            self.cached_indexes.expireBlock(inode_id, item["block_number"])

        # 2. Truncate last block with zeroes
        block = self.__get_block_from_cache(inode_id, max_block_number)
        block.truncate(inblock_offset)

        tableIndex.update_size(inode_id, max_block_number, inblock_offset)
        self.cached_indexes.expireBlock(inode_id, max_block_number)

        # 3. Put to cache, it will be rehashed and compressed
        self.cached_blocks.set(inode_id, max_block_number, block, writed=True)

        return

    def __cache_meta_hook(self):  # {{{3

        self.__update_mounted_subvolume_time()

        flushed_nodes = 0
        flushed_names = 0
        flushed_attrs = 0
        flushed_xattrs = 0
        flushed_indexes = 0
        flushed_hash_compress = 0
        flushed_hash_sizes = 0

        start_time = time()

        if start_time - self.cache_gc_meta_last_run >= self.flush_interval:

            flushed_nodes = self.cached_nodes.clear()
            flushed_names = self.cached_names.clear()
            flushed_names += self.cached_name_ids.clear()
            flushed_indexes = self.cached_indexes.expired()

            flushed_xattrs = self.cached_xattrs.clear()

            flushed_hash_compress = self.cached_hash_compress.clear()
            flushed_hash_sizes = self.cached_hash_sizes.clear()

            # Just readed...
            expired = self.cached_attrs.expired()
            flushed_attrs += expired[0]
            # Just writed/updated...
            flushed_attrs += self.__flush_expired_inodes(expired[1])

            self.cache_gc_meta_last_run = time()

        if flushed_attrs + flushed_nodes + flushed_names + flushed_indexes + flushed_xattrs + flushed_hash_compress + flushed_hash_sizes > 0:

            self.__commit_changes()

            elapsed_time = time() - start_time

            self.reportHelper.time_spent_writing_meta += elapsed_time

            elapsed_time = self.cache_gc_meta_last_run - start_time
            self.getLogger().debug(
                "Meta cache cleanup: flushed %i nodes, %i attrs,  %i xattrs, %i names, %i indexes, %i compressTypes, %i hashSizes in %s.",
                flushed_nodes, flushed_attrs, flushed_xattrs, flushed_names, flushed_indexes,
                flushed_hash_compress, flushed_hash_sizes,
                format_timespan(elapsed_time)
            )

        self.reportHelper.do_print_stats_ontime()

        return flushed_attrs + flushed_names + flushed_nodes


    def __commit_changes(self):  # {{{3
        if not self.use_transactions:
            start_time = time()
            self.getManager().commit()
            self.getManager().begin()
            self.reportHelper.time_spent_commiting += time() - start_time
        self.getManager().shrinkMemory()

    def __rollback_changes(self):  # {{{3
        if not self.use_transactions:
            self.getLogger().note('Rolling back changes')
            self.getManager().rollback()

    def __except_to_status(self, method, exception, code=errno.ENOENT):  # {{{3
        # Don't report ENOENT raised from getattr().
        if method != 'getattr' or code != errno.ENOENT:
            sys.stderr.write('%s\n' % ('-' * 50))
            sys.stderr.write("Caught exception in %s(): %s\n" % (method, exception))
            traceback.print_exc(file=sys.stderr)
            sys.stderr.write('%s\n' % ('-' * 50))
            sys.stderr.write("Returning %i\n" % code)
            sys.stderr.flush()

            self.getLogger().error("Caught exception in %s(): %s\n" % (method, exception))
            self.getLogger().error(traceback.format_exc())
            self.getLogger().error("Returning %i\n" % code)
            # Convert the exception to a FUSE error code.
        if isinstance(exception, OSError):
            return FUSEError(exception.errno)
        else:
            return FUSEError(code)
