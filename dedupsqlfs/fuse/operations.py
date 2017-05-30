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

# Try to load the Python FUSE binding.
try:
    import llfuse
    from llfuse import FUSEError
except ImportError:
    sys.stderr.write("Error: The Python FUSE binding isn't installed!\n" + \
                     "If you're on Ubuntu try running `sudo apt-get install python-fuse'.\n")
    sys.exit(1)

# Local modules that are mostly useful for debugging.
from dedupsqlfs.log import logging
from dedupsqlfs.lib import constants
from dedupsqlfs.my_formats import format_size, format_timespan
from dedupsqlfs.get_memory_usage import get_real_memory_usage, get_memory_usage
from dedupsqlfs.lib.cache.simple import CacheTTLseconds
from dedupsqlfs.lib.cache.storage import StorageTimeSize
from dedupsqlfs.lib.cache.index import IndexTime
from dedupsqlfs.lib.cache.inodes import InodesTime
from dedupsqlfs.fuse.subvolume import Subvolume

class DedupOperations(llfuse.Operations): # {{{1

    def __init__(self, **kwargs):  # {{{2

        llfuse.Operations.__init__(self, **kwargs)

        # Initialize instance attributes.
        self.block_size = 1024 * 128

        self.bytes_read = 0
        self.bytes_written = 0
        self.bytes_written_compressed = 0
        self.compressed_ratio = 0.0

        self.bytes_deduped = 0
        self.bytes_deduped_last = 0

        self.cache_enabled = True
        self.cache_gc_meta_last_run = time()
        self.cache_gc_block_write_last_run = time()
        self.cache_gc_block_writeSize_last_run = time()
        self.cache_gc_block_read_last_run = time()
        self.cache_gc_block_readSize_last_run = time()
        self.cache_meta_timeout = 20
        self.cache_block_write_timeout = 10
        self.cache_block_read_timeout = 10
        self.cache_block_write_size = -1
        self.cache_block_read_size = -1
        self.flush_interval = 5
        self.flushBlockSize_interval = 1

        self.subvol_uptate_last_run = time()

        self.cached_names = CacheTTLseconds()
        self.cached_nodes = CacheTTLseconds()
        self.cached_attrs = InodesTime()
        self.cached_xattrs = CacheTTLseconds()

        self.cached_blocks = StorageTimeSize()
        self.cached_indexes = IndexTime()

        self.calls_log_filter = set()

        self.fs_mounted_at = time()
        self.mounted_subvolume = None
        self.mounted_subvolume_name = None

        self.gc_enabled = True
        self.gc_umount_enabled = True
        self.gc_vacuum_enabled = False
        self.gc_hook_last_run = time()
        self.gc_interval = 60

        self.link_mode = stat.S_IFLNK | 0o777

        self.memory_usage = 0
        self.memory_usage_real = 0
        self.opcount = 0
        self.should_vacuum = False

        self.root_mode = stat.S_IFDIR | 0o755

        self.timing_report_last_run = time()

        self.time_spent_caching_nodes = 0
        self.time_spent_hashing = 0
        self.time_spent_interning = 0
        self.time_spent_querying_tree = 0
        self.time_spent_reading = 0
        self.time_spent_traversing_tree = 0
        self.time_spent_writing = 0
        self.time_spent_writing_blocks = 0

        self.time_spent_flushing_block_cache = 0
        self.time_spent_flushing_writed_block_cache = 0
        self.time_spent_flushing_readed_block_cache = 0
        self.time_spent_flushing_writedByTime_block_cache = 0
        self.time_spent_flushing_readedByTime_block_cache = 0
        self.time_spent_flushing_writedBySize_block_cache = 0
        self.time_spent_flushing_readedBySize_block_cache = 0

        self.time_spent_compressing = 0
        self.time_spent_decompressing = 0

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
                self.manager.setBasepath(os.path.expanduser(self.getOption("data")))
                if not self.manager.isSupportedStorage():
                    from dedupsqlfs.db.mysql.manager import DbManager
                    self.manager = DbManager(dbname=self.getOption("name"))
                    self.manager.setBasepath(os.path.expanduser(self.getOption("data")))
                    if not self.manager.isSupportedStorage():
                        raise RuntimeError("Unsupported storage on %r" % self.getOption("data"))

            else:
                raise ValueError("Unknown storage engine: %r" % engine)

            self.manager.setLogger(self.getLogger())
            self.manager.setTableEngine(self.getOption('table_engine'))
            self.manager.setSynchronous(self.getOption("synchronous"))
            self.manager.setAutocommit(self.getOption("use_transactions"))
            self.manager.setBasepath(os.path.expanduser(self.getOption("data")))
            self.manager.begin()

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
            table_name += "_" + self.mounted_subvolume["hash"]
        return self.getManager().getTable(table_name)

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
        if not self._compression_types_revert:
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
            self._compression_types = self.getManager().getTable("compression_type").getAllRevert()
        return set(self._compression_types.keys())

    # --------------------------------------------------------------------------------------
    #       FUSE OPERATIONS
    # --------------------------------------------------------------------------------------

    def access(self, inode, mode, ctx): # {{{3
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
        c = {}
        for key in ctx.__slots__:
            c[ key ] = getattr(ctx, key)

        self.__log_call('access', '->(inode=%i, mode=%o, ctx=%r)', inode, mode, c)
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
        self.__log_call('create', '->(inode_parent=%i, name=%r, mode=%o, flags=%o)',
                        inode_parent, name, mode, flags)
        if self.isReadonly(): return errno.EROFS

        try:
            node = self.__get_tree_node_by_parent_inode_and_name(inode_parent, name)
        except FUSEError as e:
            node = None
            self.__log_call('create', '-- node with that name not found, creating...')
            if e.errno != errno.ENOENT:
                raise

        if not node:
            inode, parent_ino = self.__insert(inode_parent, name, mode, 0, ctx)
        else:
            if flags & os.O_CREAT and flags & os.O_EXCL:
                self.__log_call('open', '-- exception for existed file! cant create!')
                raise FUSEError(errno.EIO)

            inode = node["inode_id"]

        fh = self.open(inode, flags)
        attrs = self.__getattr(inode)

        self.__cache_meta_hook()

        self.__log_call('create', '<-(created inode=%i, attrs=%r)', fh, attrs)

        return (fh, attrs,)

    def destroy(self): # {{{3

        # Stop flushing thread if it started
        self.getApplication().stopCacheFlusher()

        if self.getOption('lock_file'):
            try:
                f = open(self.getOption('lock_file'), 'w')
                f.write("destroy\n")
                f.close()
            except:
                self.getLogger().warning("DedupFS: can't write to %r" % self.getOption('lock_file'))
                pass

        try:
            self.__log_call('destroy', '->()')
            self.getLogger().debug("Umount file system in process...")
            if not self.getOption("readonly"):

                # Flush all cached blocks
                self.getLogger().debug("Flush remaining inodes.")
                self.__flush_expired_inodes(self.cached_attrs.clear())
                self.getLogger().debug("Flush remaining blocks.")
                self.__flush_old_cached_blocks(self.cached_blocks.clear())
                self.cached_indexes.clear()

                self.getLogger().debug("Committing outstanding changes.")
                self.getManager().commit()

                if self.getOption("gc_umount_enabled"):
                    # Force vacuum on umount
                    self.gc_enabled = True
                    self.__collect_garbage()
            if self.getOption("verbosity") > 1:
                self.__print_stats()

            self.getManager().getTable('option').update('mounted', 0)

            self.getManager().close()
        except Exception as e:
            raise self.__except_to_status('destroy', e, errno.EIO)

        if self.getOption('lock_file'):
            try:
                f = open(self.getOption('lock_file'), 'w')
                f.write("destroy-done\n")
                f.close()
            except:
                self.getLogger().warning("DedupFS: can't write to %r" % self.getOption('lock_file'))
                pass

        return 0

    def flush(self, fh):
        try:
            self.__log_call('flush', '->(fh=%i)', fh)

            attr = self.__get_inode_row(fh)
            self.__log_call('flush', '-- inode(%i) size=%i', fh, attr["size"])
            if not attr["size"]:
                self.__log_call('flush', '-- inode(%i) zero sized! remove all blocks', fh)
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
        @param inode_list: inode_list is a list of (inode, nlookup) tuples. 
        @return: 
        """
        try:
            self.__log_call('forget', '->(inode_list=%r)', inode_list)
            # clear block cache
            for ituple in inode_list:
                self.cached_attrs.expire(ituple[0])
                self.cached_blocks.expire(ituple[0])
                self.cached_indexes.expire(ituple[0])
        except FUSEError:
            raise
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('forget', e, errno.EIO)

    def fsync(self, fh, datasync):
        """
        Sync inode metadata

        @param fh: inode
        @param datasync: force sync data
        @return: None
        """
        self.__log_call('fsync', '->(fh=%i, datasync=%r)', fh, datasync)

        if self.isReadonly(): raise FUSEError(errno.EROFS)

        try:
            attr = self.__get_inode_row(fh)
            self.__log_call('fsync', '-- inode(%i) size=%i', fh, attr["size"])
            if not attr["size"]:
                self.__log_call('fsync', '-- inode(%i) zero sized! remove all blocks', fh)
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
        self.__log_call('fsyncdir', '->(fh=%i, datasync=%r)', fh, datasync)
        if self.isReadonly(): raise FUSEError(errno.EROFS)
        self.cached_attrs.flush(fh)
        self.__cache_meta_hook()

    def getattr(self, inode): # {{{3
        self.__log_call('getattr', '->(inode=%r)', inode)
        attr = self.__getattr(inode)
        v = self.__get_inode_row(inode)
        self.__log_call('getattr', '<-(inode=%r, attr=%r)', inode, v)
        return attr

    def getxattr(self, inode, name): # {{{3
        self.__log_call('getxattr', '->(inode=%r, name=%r)', inode, name)

        xattrs = self.__get_cached_xattrs(inode)
        if not xattrs:
            raise FUSEError(llfuse.ENOATTR)
        if name not in xattrs:
            raise FUSEError(llfuse.ENOATTR)
        return xattrs[name]

    def init(self): # {{{3
        try:
            # Disable log for fuse functions
            if self.getOption("verbosity") < 2:
                self.calls_log_filter.add("none")

            # Process the custom command line options defined in __init__().
            if self.getOption("block_size") is not None:
                self.block_size = self.getOption("block_size")

            if self.block_size < constants.BLOCK_SIZE_MIN:
                self.getLogger().warn("Block size less than minimal! (%i<%i) Set to default minimal." % (
                    self.block_size, constants.BLOCK_SIZE_MIN
                ))
                self.block_size = constants.BLOCK_SIZE_MIN

            if self.block_size > constants.BLOCK_SIZE_MAX:
                self.getLogger().warn("Block size more than maximal! (%i>%i) Set to default maximal." % (
                    self.block_size, constants.BLOCK_SIZE_MAX
                ))
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

            if self.getOption("gc_enabled") is not None:
                self.gc_enabled = self.getOption("gc_enabled")
            if self.getOption("gc_umount_enabled") is not None:
                self.gc_umount_enabled = self.getOption("gc_umount_enabled")
            if self.getOption("gc_vacuum_enabled") is not None:
                self.gc_vacuum_enabled = self.getOption("gc_vacuum_enabled")
            if self.getOption("gc_interval") is not None:
                self.gc_interval = self.getOption("gc_interval")

            if not self.cache_enabled:
                self.cached_blocks.setMaxReadTtl(0)
                self.cached_blocks.setMaxWriteTtl(0)
                self.cached_indexes.setMaxTtl(0)
                self.cached_nodes.set_max_ttl(0)
                self.cached_names.set_max_ttl(0)
                self.cached_attrs.set_max_ttl(0)
                self.cached_xattrs.set_max_ttl(0)
            else:
                if self.block_size:
                    self.cached_blocks.setBlockSize(self.block_size)
                self.cached_blocks.setMaxWriteTtl(self.cache_block_write_timeout)
                self.cached_blocks.setMaxReadTtl(self.cache_block_read_timeout)

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
                self.cached_attrs.set_max_ttl(self.cache_meta_timeout)
                self.cached_xattrs.set_max_ttl(self.cache_meta_timeout)
                self.cached_indexes.setMaxTtl(self.cache_meta_timeout)


            if self.getOption("synchronous") is not None:
                self.synchronous = self.getOption("synchronous")
            if self.getOption("use_transactions") is not None:
                self.use_transactions = self.getOption("use_transactions")

            try:
                # Get a reference to the hash function.
                hashlib.new(self.hash_function)
            except:
                self.getLogger().critical("Error: The selected hash function %r doesn't exist!", self.hash_function)
                sys.exit(1)

            # Initialize the logging and database subsystems.
            self.__log_call('init', 'init()')

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

            # NOT READONLY - AND - Mountpoint defined (mount action)
            self.getApplication().startCacheFlusher()


            if self.getApplication().mountpoint:
                self.getManager().getTable('option').update('mounted', 1)

            if self.getOption('lock_file'):
                try:
                    f = open(self.getOption('lock_file'), 'w')
                    f.write("inited\n")
                    f.close()
                except:
                    self.getLogger().warning("DedupFS: can't write to %r" % self.getOption('lock_file'))
                    pass

            self.getLogger().debug("DedupFS: inited and mounted")
            return 0
        except Exception as e:
            self.__except_to_status('init', e, errno.EIO)
            # Bug fix: Break the mount point when initialization failed with an
            # exception, because self.conn might not be valid, which results in
            # an internal error message for every FUSE API call...
            raise e

    def link(self, inode, new_parent_inode, new_name): # {{{3
        self.__log_call('link', '->(inode=%r, parent_inode=%r, new_name=%r)', inode, new_parent_inode, new_name)
        if self.isReadonly(): raise FUSEError(errno.EROFS)

        parent_node = self.__get_tree_node_by_inode(new_parent_inode)
        string_id = self.__intern(new_name)

        treeTable = self.getTable("tree")

        attr = self.__get_inode_row(inode)

        treeTable.insert(parent_node["id"], string_id, inode)
        attr["nlinks"] += 1
        self.cached_attrs.set(inode, attr, writed=True)

        #if attr["mode"] & stat.S_IFDIR:
            #attr = self.__get_inode_row(new_parent_inode)
            #attr["nlinks"] += 1
        #    self.cached_attrs.set(new_parent_inode, attr, writed=True)

        self.__cache_meta_hook()

        return self.__getattr(inode)

    def listxattr(self, inode):
        self.__log_call('listxattr', '->(inode=%r)', inode)
        xattrs = self.__get_cached_xattrs(inode)
        self.__log_call('listxattr', '<-(xattrs=%r)', xattrs)
        if not xattrs:
            return []
        return xattrs.keys()

    def lookup(self, parent_inode, name):
        self.__log_call('lookup', '->(parent_inode=%r, name=%r)', parent_inode, name)

        node = self.__get_tree_node_by_parent_inode_and_name(parent_inode, name)
        attr = self.__getattr(node["inode_id"])
        self.__log_call('lookup', '-- node=%r', node)

        v = {}
        for a in attr.__slots__:
            v[a] = getattr(attr, a)

        self.__log_call('lookup', '<-(attr=%r)', v)

        self.__cache_meta_hook()
        return attr

    def get_tree_node_by_parent_inode_and_name(self, parent_inode, name):
        """
        @TODO: move to LowLevelOperations
        """
        self.__log_call('get_tree_node_by_parent_inode_and_name', '->(parent_inode=%r, name=%r)', parent_inode, name)
        node = self.__get_tree_node_by_parent_inode_and_name(parent_inode, name)
        self.__log_call('lookup', '<-(attr=%r)', node)
        self.__cache_meta_hook()
        return node

    def mkdir(self, parent_inode, name, mode, ctx): # {{{3
        if self.isReadonly(): raise FUSEError(errno.EROFS)

        try:
            c = {}
            for key in ctx.__slots__:
                c[ key ] = getattr(ctx, key)

            self.__log_call('mkdir', '->(parent_inode=%i, name=%r, mode=%o, ctx=%r)',
                            parent_inode, name, mode, c)

            nameTable = self.getTable("name")
            inodeTable = self.getTable("inode")
            treeTable = self.getTable("tree")
            # SIZE = name row size + tree row size + inode row size
            size = nameTable.getRowSize(name) + inodeTable.getRowSize() + treeTable.getRowSize()

            inode, parent_ino = self.__insert(parent_inode, name, mode | stat.S_IFDIR, size, ctx)
            self.getManager().getTable("inode").inc_nlinks(parent_ino)

            if not self.isReadonly():
                self.__setattr_mtime(parent_inode)

            return self.__getattr(inode)
        except FUSEError:
            raise
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('mkdir', e, errno.EIO)

    def mknod(self, parent_inode, name, mode, rdev, ctx): # {{{3
        if self.isReadonly(): raise FUSEError(errno.EROFS)

        try:
            c = {}
            for key in ctx.__slots__:
                c[ key ] = getattr(ctx, key)

            self.__log_call('mknod', '->(parent_inode=%i, name=%r, mode=%o, rdev=%i, ctx=%r)',
                            parent_inode, name, mode, rdev, c)

            inode, parent_ino = self.__insert(parent_inode, name, mode, 0, ctx, rdev)
            return self.__getattr(inode)
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('mknod', e, errno.EIO)

    def open(self, inode, flags): # {{{3
        """
        Return filehandler ID
        """
        self.__log_call('open', '->(inode=%i, flags=%o)', inode, flags)
        # Make sure the file exists?

        if not self.isReadonly():
            self.__setattr_atime(inode)

        if flags & os.O_TRUNC:
            if self.isReadonly(): raise FUSEError(errno.EROFS)
            self.__log_call('open', '-- truncate file!')
            row = self.__get_inode_row(inode)
            row["size"] = 0
            self.cached_attrs.set(inode, row, writed=True)

        # Make sure the file is readable and/or writable.
        return inode

    def opendir(self, inode): # {{{3
        self.__log_call('opendir', 'opendir(inode=%i)', inode)
        # Make sure the file exists?
        self.__get_tree_node_by_inode(inode)
        # Make sure the file is readable and/or writable.

        if not self.isReadonly():
            self.__setattr_atime(inode)

        return inode

    def read(self, fh, offset, size): # {{{3
        """
        @param fh: file handler number - inode.id
        @type  fh: int
        """
        try:
            start_time = time()

            self.__log_call('read', '->(fh=%i, offset=%i, size=%i)', fh, offset, size, )

            row = self.__get_inode_row(fh)
            if row["size"] <= offset:
                self.__log_call('read', '-- oversized! inode(size)=%i', row["size"] )
                data = b''
            else:
                if row["size"] < offset + size:
                    size = row["size"] - offset
                    self.__log_call('read', '-- oversized! inode(size)=%i, corrected read size: %i', row["size"], size )
                data = self.__get_block_data_by_offset(fh, offset, size)
            lr = len(data)
            self.bytes_read += lr

            # Too much output
            # self.__log_call('read', 'readed: size=%s, data=%r', len(data), data, )
            self.__log_call('read', '<-readed: size=%s', lr, )

            self.__cache_block_hook()

            self.time_spent_reading += time() - start_time
            return data
        except Exception as e:
            return self.__except_to_status('read', e, code=errno.EIO)

    def readdir(self, fh, offset): # {{{3
        """
        @param fh: file handler number - inode.id
        @type  fh: int
        """
        self.__log_call('readdir', '->(fh=%r, offset=%i)', fh, offset)

        inode = fh

        self.__log_call('readdir', '-- (inode=%r)', inode)

        cur_node = self.__get_tree_node_by_inode(inode)

        self.__log_call('readdir', '-- (node=%r)', cur_node)

        for node in self.getTable("tree").get_children(cur_node["id"], offset):
            #if node["id"] <= offset:
            #    continue
            name = self.getTable("name").get(node["name_id"])
            attrs = self.__getattr(node["inode_id"])
            self.__log_call('readdir', '<-(name=%r, attrs=%r, node=%i)',
                            name, self.__get_inode_row(node["inode_id"]), node["id"])
            yield (name, attrs, node["id"],)


    def readlink(self, inode): # {{{3
        self.__log_call('readlink', '->(inode=%i)', inode)

        target = self.getTable("link").find_by_inode(inode)
        if not target:
            raise FUSEError(errno.ENOENT)
        return target

    def release(self, fh): # {{{3
        self.__log_call('release', '->(fh=%i)', fh)
        #self.__flush_inode_cached_blocks(fh, clean=True)
        self.cached_blocks.expire(fh)
        self.cached_attrs.expire(fh)
        self.__cache_block_hook()
        self.__cache_meta_hook()
        self.__gc_hook()
        return 0

    def releasedir(self, fh):
        self.__log_call('releasedir', '->(fh=%r)', fh)
        self.cached_attrs.expire(fh)
        self.__cache_meta_hook()
        self.__gc_hook()
        return 0

    def removexattr(self, inode, name):
        if self.isReadonly():
            raise FUSEError(errno.EROFS)

        try:
            self.__log_call('removexattr', '->(inode=%i, name=%r)', inode, name)

            xattrs = self.__get_cached_xattrs(inode)
            self.__log_call('removexattr', '--(xattrs=%r)', inode, xattrs)
            if not xattrs:
                raise FUSEError(llfuse.ENOATTR)
            if name not in xattrs:
                raise FUSEError(llfuse.ENOATTR)
            del xattrs[name]
            self.getTable("xattr").update(inode, xattrs)
            self.cached_xattrs.set(inode, xattrs)
            return 0
        except FUSEError:
            raise
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('removexattr', e, errno.ENOENT)


    def rename(self, inode_parent_old, name_old, inode_parent_new, name_new): # {{{3
        if self.isReadonly():
            raise FUSEError(errno.EROFS)

        try:
            self.__log_call('rename', '->(inode_parent_old=%i, name_old=%r, inode_parent_new=%i, name_new=%r)',
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
                if e.errno != errno.ENOENT: raise

            node_old = self.__get_tree_node_by_parent_inode_and_name(inode_parent_old, name_old)

            # Link the new path to the same inode as the old path.
#            self.link(node["inode_id"], inode_parent_new, name_new)

            # Finally unlink the old path.
#            self.__remove(inode_parent_old, name_old)

            node_parent_new = self.__get_tree_node_by_inode(inode_parent_new)
            string_id = self.__intern(name_new)

            treeTable = self.getTable("tree")
            treeTable.rename_inode(node_old["id"], node_parent_new["id"], string_id)

            self.cached_nodes.unset((inode_parent_old, name_old))
            self.cached_names.unset(name_old)

            self.__cache_meta_hook()

            self.__gc_hook()
        except FUSEError:
            raise
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('rename', e, errno.ENOENT)
        return 0

    def rmdir(self, inode_parent, name): # {{{3
        if self.isReadonly():
            raise FUSEError(errno.EROFS)

        try:
            self.__log_call('rmdir', '->(inode_parent=%i, name=%r)', inode_parent, name)

            if not self.isReadonly():
                self.__setattr_mtime(inode_parent)

            self.__remove(inode_parent, name, check_empty=True)
            self.__gc_hook()
            return 0
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('rmdir', e, errno.ENOENT)

    def setattr(self, inode, attr):
        """
        @param  inode:  inode ID
        @type   inode:  int
        @param  attr:   attributes
        @type   attr:   llfuse.EntryAttributes
        """
        if self.isReadonly():
            raise FUSEError(errno.EROFS)

        try:
            v = {}
            for a in attr.__slots__:
                v[a] = getattr(attr, a)

            self.__log_call('setattr', '->(inode=%i, attr=%r)', inode, v)

            row = self.__get_inode_row(inode)
            self.getLogger().debug("-- current row: %r", row)

            update_db = False
            new_data = {}

            # Emulate truncate
            if attr.st_size is not None:
                if row["size"] != attr.st_size:
                    new_data["size"] = attr.st_size
                    if row["size"] > attr.st_size:
                        new_data["truncated"] = True
                    update_db = True

            if attr.st_mode is not None:
                if row["mode"] != attr.st_mode:
                    new_data["mode"] = attr.st_mode
                    update_db = True

            if attr.st_uid is not None:
                if row["uid"] != attr.st_uid:
                    new_data["uid"] = attr.st_uid
                    update_db = True

            if attr.st_gid is not None:
                if row["gid"] != attr.st_gid:
                    new_data["gid"] = attr.st_gid
                    update_db = True

            if attr.st_atime_ns is not None and not self.getOption('noatime'):
                atime_i, atime_ns = self.__get_time_tuple(float(attr.st_atime_ns) / 10**9)
                if row["atime"] != atime_i:
                    new_data["atime"] = atime_i
                    update_db = True
                if row["atime_ns"] != atime_ns:
                    new_data["atime_ns"] = atime_ns
                    update_db = True

            if attr.st_mtime_ns is not None:
                mtime_i, mtime_ns = self.__get_time_tuple(float(attr.st_mtime_ns) / 10**9)
                if row["mtime"] != mtime_i:
                    new_data["mtime"] = mtime_i
                    update_db = True
                if row["mtime_ns"] != mtime_ns:
                    new_data["mtime_ns"] = mtime_ns
                    update_db = True

            if attr.st_ctime_ns is not None:
                ctime_i, ctime_ns = self.__get_time_tuple(float(attr.st_ctime_ns) / 10**9)
                if row["ctime"] != ctime_i:
                    new_data["ctime"] = ctime_i
                    update_db = True
                if row["ctime_ns"] != ctime_ns:
                    new_data["ctime_ns"] = ctime_ns
                    update_db = True
            elif update_db:
                ctime_i, ctime_ns = self.__newctime_tuple()
                new_data["ctime"] = ctime_i
                new_data["ctime_ns"] = ctime_ns
                update_db = True

            if update_db:

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

            new_data = {}

            ctime_i, ctime_ns = self.__newctime_tuple()
            new_data["atime"] = ctime_i
            new_data["atime_ns"] = ctime_ns

            row.update(new_data)
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

            new_data = {}

            ctime_i, ctime_ns = self.__newctime_tuple()
            new_data["ctime"] = ctime_i
            new_data["ctime_ns"] = ctime_ns

            row.update(new_data)
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

            new_data = {}

            ctime_i, ctime_ns = self.__newctime_tuple()
            new_data["mtime"] = ctime_i
            new_data["mtime_ns"] = ctime_ns

            row.update(new_data)
            self.cached_attrs.set(inode, row, writed=True)

            self.__cache_meta_hook()
        except Exception as e:
            raise self.__except_to_status('__setattr_mtime', e, errno.EIO)

    def setxattr(self, inode, name, value):
        if self.isReadonly():
            raise FUSEError(errno.EROFS)

        try:
            self.__log_call('setxattr', '->(inode=%i, name=%r, value=%r)', inode, name, value)

            xattrs = self.__get_cached_xattrs(inode)

            newxattr = False
            if not xattrs:
                newxattr = True
                xattrs = {}

            # Don't do DB write if value not changed
            if xattrs.get(name) != value:
                xattrs[name] = value

                if not newxattr:
                    self.getTable("xattr").update(inode, xattrs)
                else:
                    self.getTable("xattr").insert(inode, xattrs)

            # Update cache ttl
            self.cached_xattrs.set(inode, xattrs)

            return 0
        except FUSEError:
            raise
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('setxattr', e, errno.ENOENT)

    def stacktrace(self):
        self.__log_call('stacktrace', '->()')

    def statfs(self): # {{{3
        try:
            self.__log_call('statfs', '->()')
            # Use os.statvfs() to report the host file system's storage capacity.
            host_fs = os.statvfs(os.path.expanduser( self.getOption("data")) )

            stats = llfuse.StatvfsData()

            subv = Subvolume(self)

            usage = subv.get_apparent_size_fast(self.mounted_subvolume_name)

            # The total number of free blocks available to a non privileged process.
            stats.f_bavail = host_fs.f_bsize * host_fs.f_bavail / self.block_size
            if stats.f_bavail < 0:
                stats.f_bavail = 0

            # The total number of free blocks in the file system.
            stats.f_bfree = host_fs.f_frsize * host_fs.f_bfree / self.block_size

            # stats.f_bfree = stats.f_bavail
            # The total number of blocks in the file system in terms of f_frsize.
            stats.f_blocks = stats.f_bavail + usage / self.block_size
            # stats.f_blocks = int(math.ceil(1.0 * usage / self.block_size))
            if stats.f_blocks < 0:
                stats.f_blocks = 0

            stats.f_bsize = self.block_size # The file system block size in bytes.
            stats.f_favail = 0 # The number of free file serial numbers available to a non privileged process.
            stats.f_ffree = 0 # The total number of free file serial numbers.
            stats.f_files = 0 # The total number of file serial numbers.
            # File system flags. Symbols are defined in the <sys/statvfs.h> header file to refer to bits in this field (see The f_flags field).
            stats.f_frsize = self.block_size # The fundamental file system block size in bytes.
            return stats
        except Exception as e:
            raise self.__except_to_status('statfs', e, errno.EIO)

    def symlink(self, inode_parent, name, target, ctx): # {{{3
        try:
            c = {}
            for key in ctx.__slots__:
                c[ key ] = getattr(ctx, key)

            self.__log_call('symlink', '->(inode_parent=%i, name=%r, target=%r, ctx=%r)',
                            inode_parent, name, target, c)
            if self.isReadonly(): return -errno.EROFS

            # Create an inode to hold the symbolic link.
            inode, parent_ino = self.__insert(inode_parent, name, self.link_mode, len(target), ctx)
            # Save the symbolic link's target.
            self.getTable("link").insert(inode, target)
            attr = self.__getattr(inode)
            self.__cache_meta_hook()
            return attr
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('symlink', e, errno.EIO)

    def unlink(self, parent_inode, name): # {{{3
        try:
            self.__log_call('unlink', '->(parent_inode=%i, name=%r)', parent_inode, name)
            if self.isReadonly():
                raise FUSEError(errno.EROFS)

            self.__setattr_mtime(parent_inode)

            self.__remove(parent_inode, name)
            self.__cache_meta_hook()
            self.__gc_hook()
        except FUSEError:
            raise
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('unlink', e, errno.EIO)

    def write(self, fh, offset, buf): # {{{3
        if self.isReadonly(): raise FUSEError(errno.EROFS)
        try:
            start_time = time()

            # Too much output
            # self.__log_call('write', 'write(fh=%i, offset=%i, buf=%r)', fh, offset, buf)
            self.__log_call('write', '->(fh=%i, offset=%i)', fh, offset)

            #length = len(buf)
            #self.__log_call('write', 'length(buf)=%i', length)

            length = self.__write_block_data_by_offset(fh, offset, buf)

            self.__log_call('write', 'length(writed)=%i', length)

            attrs = self.__get_inode_row(fh)
            if attrs["size"] < offset + length:
                # self.getTable("inode").set_size(fh, offset + length)
                attrs["size"] = offset + length
                self.cached_attrs.set(fh, attrs, writed=True)

            if not self.isReadonly():
                self.__setattr_mtime(fh)

            # self.bytes_written is incremented from release().
            self.__cache_meta_hook()
            self.__cache_block_hook()

            self.time_spent_writing += time() - start_time
            return length
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('write', e, errno.EIO)


    # ---------------------------- Miscellaneous methods: {{{2

    def __get_cached_xattrs(self, inode):
        xattrs = self.cached_xattrs.get(inode)
        if xattrs is None:
            xattrs = self.getTable("xattr").find(inode)
            if xattrs:
                self.cached_xattrs.set(inode, xattrs)
            else:
                self.cached_xattrs.set(inode, False)
        return xattrs

    def __update_mounted_subvolume_time(self):
        t_now = time()
        if t_now - self.subvol_uptate_last_run < 1:
            return self

        self.subvol_uptate_last_run = t_now

        if self.mounted_subvolume and self.getApplication().mountpoint:
            self.getTable('subvolume').update_time(self.mounted_subvolume["id"])
        return self

    def __get_tree_node_by_parent_inode_and_name(self, parent_inode, name):
        self.__log_call('__get_tree_node_by_parent_inode_and_name', '->(parent_inode=%i, name=%r)', parent_inode, name)

        node = self.cached_nodes.get((parent_inode, name))

        if not node:

            start_time = time()

            name_id = self.cached_names.get(name)
            if not name_id:
                name_id = self.getTable("name").find(name)
                if name_id:
                    self.cached_names.set(name, name_id)
            if not name_id:
                self.getLogger().debug("! No name %r found, cant find name.id" % name)
                raise FUSEError(errno.ENOENT)

            par_node = self.__get_tree_node_by_inode(parent_inode)
            if not par_node:
                self.getLogger().debug("! No parent inode %i found, cant get tree node" % parent_inode)
                raise FUSEError(errno.ENOENT)

            node = self.getTable("tree").find_by_parent_name(par_node["id"], name_id)
            if not node:
                self.getLogger().debug("! No node %i and name %i found, cant get tree node" % (par_node["id"], name_id,))
                raise FUSEError(errno.ENOENT)

            self.cached_nodes.set((parent_inode, name), node)

            self.time_spent_caching_nodes += time() - start_time

        return node

    def __get_tree_node_by_inode(self, inode):
        self.__log_call('__get_tree_node_by_inode', '->(inode=%i)', inode)

        node = self.cached_nodes.get(inode)

        if not node:

            start_time = time()

            node = self.getTable("tree").find_by_inode(inode)
            if not node:
                self.getLogger().debug("! No inode %i found, cant get tree node" % inode)
                raise FUSEError(errno.ENOENT)

            self.cached_nodes.set(inode, node)

            self.time_spent_caching_nodes += time() - start_time

        return node

    def __get_inode_row(self, inode_id):
        self.__log_call('__get_inode_row', '->(inode=%i)', inode_id)
        row = self.cached_attrs.get(inode_id)
        if not row:
            start_time = time()

            row = self.getTable("inode").get(inode_id)
            if not row:
                self.getLogger().debug("! No inode %i found, cant get row" % inode_id)
                raise FUSEError(errno.ENOENT)

            self.cached_attrs.set(inode_id, row)

            self.time_spent_caching_nodes += time() - start_time

        self.__log_call('__get_inode_row', '<-(row=%r)', row)
        return row

    def __fill_attr_inode_row(self, row): # {{{3
        self.__log_call('__fill_attr_inode_row', '->(row=%r)', row)

        result = llfuse.EntryAttributes()

        result.entry_timeout = self.getOption("cache_meta_timeout")
        result.attr_timeout = self.getOption("cache_meta_timeout")
        result.st_ino       = int(row["id"])
        # http://stackoverflow.com/questions/11071996/what-are-inode-generation-numbers
        result.generation   = 0
        result.st_nlink     = int(row["nlinks"])
        result.st_mode      = int(row["mode"])
        result.st_uid       = int(row["uid"])
        result.st_gid       = int(row["gid"])
        result.st_rdev      = int(row["rdev"])
        result.st_size      = int(row["size"])
        result.st_atime     = float(row["atime"]) + float(row["atime_ns"]) / 10 ** 9
        result.st_mtime     = float(row["mtime"]) + float(row["mtime_ns"]) / 10 ** 9
        result.st_ctime     = float(row["ctime"]) + float(row["ctime_ns"]) / 10 ** 9
        if hasattr(result, "st_atime_ns"):
            result.st_atime_ns  = int(row["atime"]) * 10**9 + int(row["atime_ns"])
        if hasattr(result, "st_mtime_ns"):
            result.st_mtime_ns  = int(row["mtime"]) * 10**9 + int(row["mtime_ns"])
        if hasattr(result, "st_ctime_ns"):
            result.st_ctime_ns  = int(row["ctime"]) * 10**9 + int(row["ctime_ns"])
        result.st_blksize   = int(self.block_size)
        result.st_blocks    = int(result.st_size / self.block_size)
        return result


    def __getattr(self, inode_id): # {{{3
        self.__log_call('__getattr', '->(inode=%i)', inode_id)
        row = self.__get_inode_row(inode_id)
        return self.__fill_attr_inode_row(row)


    def __get_hash_index_from_cache(self, inode, block_number):
        self.__log_call('__get_hash_index_from_cache', '->(inode=%i, block_number=%i)', inode, block_number)

        hash_id = self.cached_indexes.get(inode, block_number)

        if hash_id is None:

            self.getLogger().debug("get index from DB: inode=%i, number=%i", inode, block_number)

            tableIndex = self.getTable("inode_hash_block")

            hash_id = tableIndex.hash_by_inode_number(inode, block_number)
            if not hash_id:
                # Do hack here... store False to prevent table reread until it stored in cache or deleted
                self.getLogger().debug("-- new index")
                self.cached_indexes.set(inode, block_number, False)
            else:
                self.cached_indexes.set(inode, block_number, hash_id)
        return hash_id

    def __get_block_from_cache(self, inode, block_number):
        self.__log_call('__get_block_from_cache', '->(inode=%i, block_number=%i)', inode, block_number)

        block = self.cached_blocks.get(inode, block_number)

        if block is None:

            self.getLogger().debug("get block from DB: inode=%i, number=%i", inode, block_number)

            block = BytesIO(b"\x00"*self.block_size)

            recompress = False

            hash_id = self.__get_hash_index_from_cache(inode, block_number)
            item = None

            if not hash_id:
                self.getLogger().debug("-- new block")
            else:
                tableBlock = self.getTable("block")

                item = tableBlock.get(hash_id)
                if not item:
                    err_str = "get block from DB: block not found! (inode=%i, block_number=%i, hash_id=%s)" % (inode, block_number, hash_id,)
                    self.getLogger().error(err_str)
                    raise OSError(err_str)

            # XXX: how block can be defragmented away?
            if item:
                tableHCT = self.getTable("hash_compression_type")
                compType = tableHCT.get(hash_id)

                self.getLogger().debug("-- decompress block")
                self.getLogger().debug("-- db size: %s" % len(item["data"]))

                block.seek(0)

                compression = self.getCompressionTypeName(compType["type_id"])

                tryAll = self.getOption('decompress_try_all')

                # Try all decompression methods
                if tryAll:
                    try:
                        bdata = self.__decompress(item["data"], compType["type_id"])
                    except:
                        bdata = False
                    if bdata is False:
                        for type_id in self.getCompressionTypeIds():
                            try:
                                bdata = self.__decompress(item["data"], type_id)
                            except:
                                bdata = False
                            if bdata is not False:
                                # If stored wrong method - recompress
                                if type_id != compType["type_id"]:
                                    self.getLogger().debug("-- Different compression types! Do recompress!")
                                    self.getLogger().debug("----   compressed with: %s" % compression)
                                    compression = self.getCompressionTypeName(type_id)
                                    self.getLogger().debug("---- decompressed with: %s" % compression)
                                    recompress = True
                                break
                        if bdata is False:
                            raise OSError("Can't decompress data block! Data corruption? Original method was: %s (%d)" % (
                                compression, compType["type_id"],))
                    block.write(bdata)

                else:
                    # If it fails - OSError raised
                    block.write(self.__decompress(item["data"], compType["type_id"]))

                if self.getOption('compression_recompress_now') and self.application.isDeprecated(compression):
                    recompress = True
                if self.getOption('compression_recompress_current') and not self.application.isMethodSelected(compression):
                    recompress = True

                if recompress:
                    self.getLogger().debug("-- will recompress block")

                self.getLogger().debug("-- decomp size: %s" % len(block.getvalue()))

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
        if not size:
            return b''

        inblock_offset = offset % self.block_size
        first_block_number = int(floor(1.0 * (offset - inblock_offset) / self.block_size))
        raw_data = BytesIO()

        # if we in the middle of a block by offset and read blocksize - need to read more then one...
        read_blocks = int(ceil(1.0 * (size + inblock_offset) / self.block_size))
        if not read_blocks:
            read_blocks = 1

        self.getLogger().debug("first block number = %s, read blocks = %s inblock offset = %s" % (first_block_number, read_blocks, inblock_offset,))

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

        self.bytes_read += readed_size

        return raw_data.getvalue()

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
        if not size:
            return 0

        inblock_offset = offset % self.block_size
        first_block_number = int(floor(1.0 * (offset - inblock_offset) / self.block_size))

        io_data = BytesIO(block_data)

        write_blocks = int(ceil(1.0 * size / self.block_size))
        if not write_blocks:
            write_blocks = 1

        self.getLogger().debug("first block number = %s, size = %s, write blocks = %s inblock offset = %s" % (first_block_number, size, write_blocks, inblock_offset,))

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

        return writed_size


    def __init_store(self): # {{{3
        # Bug fix: At this point fuse.FuseGetContext() returns uid = 0 and gid = 0
        # which differs from the info returned in later calls. The simple fix is to
        # use Python's os.getuid() and os.getgid() library functions instead of
        # fuse.FuseGetContext().
        self.getLogger().debug("__init_store()")

        optTable = self.getTable("option")
        inited = optTable.get("inited")

        self.getLogger().debug("__init_store(): inited=%r" % inited)

        if not inited:

            nameRoot = constants.ROOT_SUBVOLUME_NAME

            subv = Subvolume(self)
            self.mounted_subvolume = subv.create(nameRoot)

            self.mounted_subvolume_name = nameRoot

            for name in ("block_size",):
                optTable.insert(name, "%i" % self.getOption(name))

            for name in ("hash_function",):
                optTable.insert(name, "%s" % self.getOption(name))

            optTable.insert("mounted_subvolume", self.mounted_subvolume_name)

            from dedupsqlfs.db.migration import DbMigration
            migr = DbMigration(self.manager, self.getLogger())
            # Always use last migration number on new FS
            migr.setLastMigrationNumber()

            optTable.insert("mounted", 1)
            optTable.insert("inited", 1)

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

        return


    def __select_subvolume(self):

        optTable = self.getTable("option")
        subvTable = self.getTable('subvolume')

        self.getLogger().debug("__select_subvolume(1): mounted_subvolume=%r" % self.mounted_subvolume_name)

        if self.mounted_subvolume_name:
            if subvTable.find(self.mounted_subvolume_name):
                pass
            else:
                self.getLogger().warning("__select_subvolume(1.2): subvolume not found, select default")
                self.mounted_subvolume_name = constants.ROOT_SUBVOLUME_NAME
        else:
            check = optTable.get("mounted_subvolume", True)
            if check:
                self.getLogger().debug("__select_subvolume(1.2): subvolume not found, select previous: %r" % check)
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

        self.getLogger().debug("__select_snapshot(2): mounted_subvolume=%r" % self.mounted_subvolume_name)

        return


    def __log_call(self, fun, msg, *args): # {{{3
        # To disable all __log_call() invocations:
        #  :%s/^\(\s\+\)\(self\.__log_call\)/\1#\2
        # To re enable them:
        #  :%s/^\(\s\+\)#\(self\.__log_call\)/\1\2
        if not self.calls_log_filter or fun in self.calls_log_filter:
            self.getLogger().debugv("%s %s" % (fun, msg,), *args)


    def __get_opts_from_db(self): # {{{3
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
            self.getLogger().warning("Ignoring --hash=%r argument, using previously chosen hash function %s instead",
                self.hash_function, hash_function)
            self.hash_function = hash_function
        pass


    def __insert(self, parent_inode, name, mode, size, ctx, rdev=0): # {{{3
        self.getLogger().debug("__insert->(parent_inode=%i,name=%r,mode=%o,size=%i)", parent_inode, name, mode, size)

        nlinks = mode & stat.S_IFDIR and 2 or 1

        inodeTable = self.getTable("inode")
        treeTable = self.getTable("tree")

        t_i, t_ns = self.__newctime_tuple()

        inode_id = inodeTable.insert(
            nlinks, mode, ctx.uid, ctx.gid, rdev, size,
            t_i, t_i, t_i,
            t_ns, t_ns, t_ns
        )

        name_id = self.__intern(name)

        par_node = self.__get_tree_node_by_inode(parent_inode)

        treeTable.insert(par_node["id"], name_id, inode_id)

        self.getLogger().debug("__insert<-(inode=%i,parent_inode=%i)", inode_id, par_node["inode_id"])

        return inode_id, par_node["inode_id"]


    def __intern(self, string): # {{{3
        result = self.cached_names.get(string)
        if not result:
            start_time = time()

            nameTable = self.getTable("name")
            result = nameTable.find(string)
            if not result:
                result = nameTable.insert(string)
            self.cached_names.set(string, result)

            self.time_spent_interning += time() - start_time
        return int(result)


    def __remove(self, parent_inode, name, check_empty=False): # {{{3
        """
        @param  check_empty:    Check is this directory not empty
        @param  check_empty:    bool
        """
        self.getLogger().debug("__remove->(parent_inode=%i,name=%r,check_empty=%r)" % (parent_inode, name, check_empty,))

        cur_node = self.__get_tree_node_by_parent_inode_and_name(parent_inode, name)

        self.getLogger().debug("__remove -- (cur_node=%r)" % (cur_node,))

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
        self.cached_nodes.unset((parent_inode, name))
        self.cached_nodes.unset(cur_node["inode_id"])
        self.cached_names.unset(name)
        self.cached_indexes.expire(cur_node["inode_id"])

        self.__cache_meta_hook()

        return


    def __access(self, inode, mode, ctx): # {{{3
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

        self.getLogger().debug("ctx.uid=%i, ctx.gid=%i", ctx.uid, ctx.gid)

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


    def __newctime(self): # {{{3
        return time()

    def __newctime_tuple(self): # {{{3
        return self.__get_time_tuple( self.__newctime() )

    def newctime_tuple(self): # {{{3
        return self.__newctime_tuple()

    def __get_time_tuple(self, t): # {{{3
        t_ns, t_i = modf(t)
        t_ns = int(t_ns * 10**9)
        return int(t_i), t_ns


    def __hash(self, data): # {{{3
        start_time = time()
        context = hashlib.new(self.hash_function)
        context.update(data)
        digest = context.digest()
        self.time_spent_hashing += time() - start_time
        return digest

    def __decompress(self, block_data, compression_type_id):
        """
        @param block_data: bytes
        @param compression_type_id: int
        @return: bytes
        """
        start_time = time()
        compression = self.getCompressionTypeName( compression_type_id )
        self.getLogger().debug("-- decompress block: type = %s" % compression)
        result = self.application.decompressData(compression, block_data)
        self.time_spent_decompressing += time() - start_time
        return result

    def __print_stats(self): # {{{3
        if self.getLogger().isEnabledFor(logging.INFO) and self.getOption("verbose_stats"):
            self.getLogger().info('-' * 79)
            self.__report_memory_usage()
            self.__report_memory_usage_real()
            self.__report_deduped_usage()
            self.__report_compressed_usage()
            self.__report_throughput()
            self.__report_timings()
            self.__report_database_timings()
            self.__report_database_operations()
            self.getLogger().info(' ' * 79)


    def __report_timings(self): # {{{3
        timings = [
            (self.time_spent_caching_nodes, 'Caching tree nodes'),
            (self.time_spent_interning, 'Interning path components'),
            (self.time_spent_reading, 'Reading data stream'),
            (self.time_spent_writing, 'Writing data stream'),
            (self.time_spent_writing_blocks, 'Writing data blocks (cumulative)'),
            (self.time_spent_writing_blocks - self.time_spent_compressing - self.time_spent_hashing, 'Writing blocks to database'),
            (self.getManager().getTimeSpent(), 'Database operations'),
            (self.time_spent_flushing_writed_block_cache - self.time_spent_writing_blocks, 'Flushing writed block cache'),
            (self.time_spent_flushing_readed_block_cache, 'Flushing readed block cache (cumulative)'),
            (self.time_spent_flushing_writed_block_cache, 'Flushing writed block cache (cumulative)'),
            (self.time_spent_flushing_writedByTime_block_cache, 'Flushing writed block cache (by Time)'),
            (self.time_spent_flushing_writedBySize_block_cache, 'Flushing writed block cache (by Size)'),
            (self.time_spent_flushing_readedByTime_block_cache, 'Flushing readed block cache (by Time)'),
            (self.time_spent_flushing_readedBySize_block_cache, 'Flushing readed block cache (by Size)'),
            (self.time_spent_flushing_block_cache, 'Flushing block cache (cumulative)'),
            (self.time_spent_hashing, 'Hashing data blocks'),
            (self.time_spent_compressing, 'Compressing data blocks'),
            (self.time_spent_decompressing, 'Decompressing data blocks'),
            (self.time_spent_querying_tree, 'Querying the tree')
        ]
        maxdescwidth = max([len(l) for t, l in timings]) + 3
        timings.sort(reverse=True)

        uptime = time() - self.fs_mounted_at
        self.getLogger().info("Filesystem mounted: %s", format_timespan(uptime))

        printed_heading = False
        for timespan, description in timings:
            percentage = 100.0 * timespan / uptime
            if percentage >= 0.1:
                if not printed_heading:
                    self.getLogger().info("Cumulative timings of slowest operations:")
                    printed_heading = True
                self.getLogger().info(
                    " - %-*s%s (%.1f%%)" % (maxdescwidth, description + ':', format_timespan(timespan), percentage))

    def __report_database_timings(self): # {{{3
        if self.getLogger().isEnabledFor(logging.INFO):
            timings = []
            for tn in self.getManager().tables:
                t = self.getTable(tn)

                opTimes = t.getTimeSpent()
                for op, timespan in opTimes.items():
                    timings.append((timespan, 'Table %r - operation %r timings' % (tn, op,),))

            maxdescwidth = max([len(l) for t, l in timings]) + 3
            timings.sort(reverse=True)

            alltime = self.getManager().getTimeSpent()
            self.getLogger().info("Database all operations timings: %s", format_timespan(alltime))

            printed_heading = False
            for timespan, description in timings:
                percentage = 100.0 * timespan / alltime
                if percentage >= 0.1:
                    if not printed_heading:
                        self.getLogger().info("Cumulative timings of slowest tables:")
                        printed_heading = True
                    self.getLogger().info(
                        " - %-*s%s (%.1f%%)" % (maxdescwidth, description + ':', format_timespan(timespan), percentage))

    def __report_database_operations(self): # {{{3
        if self.getLogger().isEnabledFor(logging.INFO):
            counts = []
            allcount = 0
            for tn in self.getManager().tables:
                t = self.getTable(tn)

                opCount = t.getOperationsCount()
                for op, count in opCount.items():
                    counts.append((count, 'Table %r - operation %r count' % (tn, op,),))
                    allcount += count

            maxdescwidth = max([len(l) for t, l in counts]) + 3
            counts.sort(reverse=True)

            self.getLogger().info("Database all operations: %s", allcount)

            printed_heading = False
            for count, description in counts:
                percentage = 100.0 * count / allcount
                if percentage >= 0.1:
                    if not printed_heading:
                        self.getLogger().info("Cumulative count of operations:")
                        printed_heading = True
                    self.getLogger().info(
                        " - %-*s%s (%.1f%%)" % (maxdescwidth, description + ':', count, percentage))

    def __report_memory_usage(self): # {{{3
        memory_usage = get_memory_usage()
        msg = "Current virtual memory usage is " + format_size(memory_usage)
        difference = abs(memory_usage - self.memory_usage)
        if self.memory_usage != 0 and difference:
            direction = self.memory_usage < memory_usage and 'up' or 'down'
            msg += " (%s by %s)" % (direction, format_size(difference))
        self.getLogger().info(msg + '.')
        self.memory_usage = memory_usage

    def __report_memory_usage_real(self): # {{{3
        memory_usage = get_real_memory_usage()
        msg = "Current real memory usage is " + format_size(memory_usage)
        difference = abs(memory_usage - self.memory_usage_real)
        if self.memory_usage_real != 0 and difference:
            direction = self.memory_usage_real < memory_usage and 'up' or 'down'
            msg += " (%s by %s)" % (direction, format_size(difference))
        self.getLogger().info(msg + '.')
        self.memory_usage_real = memory_usage


    def __report_deduped_usage(self): # {{{3
        msg = "Current deduped stream bytes is " + format_size(self.bytes_deduped)
        difference = abs(self.bytes_deduped - self.bytes_deduped_last)
        if self.bytes_deduped_last != 0 and difference:
            direction = self.bytes_deduped_last < self.bytes_deduped and 'up' or 'down'
            msg += " (%s by %s)" % (direction, format_size(difference))
        self.getLogger().info(msg + '.')
        self.bytes_deduped_last = self.bytes_deduped


    def __report_compressed_usage(self): # {{{3
        if self.bytes_written:
            ratio = (self.bytes_written - self.bytes_written_compressed) * 100.0 / self.bytes_written
        else:
            ratio = 0
        msg = "Current stream bytes compression ratio is %.2f%%" % ratio
        difference = abs(ratio - self.compressed_ratio)
        if self.compressed_ratio != 0 and difference:
            direction = self.compressed_ratio < ratio and 'up' or 'down'
            msg += " (%s by %.2f%%)" % (direction, difference)
        msg += " (%s to %s)" % (format_size(self.bytes_written), format_size(self.bytes_written_compressed))
        self.getLogger().info(msg + '.')
        self.compressed_ratio = ratio


    def __report_throughput(self, nbytes=None, nseconds=None, label=None): # {{{3
        if nbytes == None:
            #self.bytes_read, self.time_spent_reading = \
            self.__report_throughput(self.bytes_read, self.time_spent_reading, "read")
            #self.bytes_written, self.time_spent_writing = \
            self.__report_throughput((self.bytes_written + self.bytes_deduped), self.time_spent_writing, "write")
        else:
            if nbytes > 0:
                average = format_size(nbytes / max(1, nseconds))
                self.getLogger().info("Average %s stream speed is %s/s.", label, average)
                # Decrease the influence of previous measurements over time?
                #if nseconds > 60 and nbytes > 1024 ** 2:
                #    return nbytes / 2, nseconds / 2
            return nbytes, nseconds


    def __gc_hook(self): # {{{3
        t_now = time()
        if t_now - self.gc_hook_last_run >= self.gc_interval:
            self.gc_hook_last_run = t_now
            self.__collect_garbage()
            self.__timing_report_hook()
            self.gc_hook_last_run = time()
        return

    def __timing_report_hook(self): # {{{3
        t_now = time()
        if t_now - self.timing_report_last_run >= self.gc_interval:
            self.timing_report_last_run = t_now
            self.__print_stats()
        return


    def __write_block_data(self, inode, block_number, block):
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

        self.getLogger().debug("write block: inode=%s, block number=%s, data length=%s" % (inode, block_number, block_length,))

        index_hash_id = self.__get_hash_index_from_cache(inode, block_number)

        # Second sparse files variant = remove zero-bytes tail
        data_block = data_block.rstrip(b"\x00")

        block_length = len(data_block)

        # First sparse files variant = flush zero-bytes string
        sparsed_block = False
        if block_length == 0:
            sparsed_block = True

        result["writed_size"] = block_length

        self.getLogger().debug("write block: updated data length=%s, sparse=%r" % (block_length, sparsed_block,))

        if (block_length == 0 and index_hash_id) or sparsed_block:
            self.getLogger().debug("write block: remove empty or zero-filled block")
            tableIndex.delete_by_inode_number(inode, block_number)
            self.cached_indexes.expireBlock(inode, block_number)

            result["deleted"] = True

            self.time_spent_writing_blocks += time() - start_time
            return result

        tableHash = self.getTable("hash")
        tableHCT = self.getTable("hash_compression_type")

        hash_value = self.__hash(data_block)
        hash_id = tableHash.find(hash_value)

        result["hash"] = hash_id

        # It is new block now?
        if not hash_id:

            result["new"] = True
            result["data"] = data_block

            self.getLogger().debug("-- insert new block data")
            hash_id = tableHash.insert(hash_value)
            result["hash"] = hash_id

            self.bytes_written += block_length
        else:
            hash_CT = tableHCT.get(hash_id)

            # It may not be at this time because at first time only hashes
            # stored in DB. Compression and indexes are stored later.
            if hash_CT:
                compression = self.getCompressionTypeName(hash_CT["type_id"])

                if self.getOption('compression_recompress_now'):

                    if self.application.isDeprecated(compression):
                        result["recompress"] = True
                        result["data"] = data_block


                if self.getOption('compression_recompress_current'):

                    if not self.application.isMethodSelected(compression):
                        result["recompress"] = True
                        result["data"] = data_block

                if self.getOption('collision_check_enabled'):

                    old_block = self.getTable("block").get(hash_id)

                    old_data = self.__decompress(old_block["data"], hash_CT["type_id"])
                    if old_data != data_block:
                        self.getLogger().error("EEE: weird hashed data collision detected! hash id: %s, value: %r, inode: %s, block-number: %s" % (
                            hash_id, hash_value, inode, block_number
                        ))
                        self.getLogger().warn("Use more strong hashing algo! I'm continue, but you are warned...")
                    old_hash = self.__hash(old_data)
                    if old_hash != hash_value:
                        self.getLogger().error("Decompressed block data hash not equal with stored!")
                        self.getLogger().error("FS data corruption? Something wrong with db layer? I'm done with that!")
                        raise RuntimeError("Data corruption!")

            # Old hash found
            self.bytes_deduped += block_length

        if not index_hash_id:
            tableIndex.insert(
                inode, block_number, hash_id
            )
            self.cached_indexes.set(inode, block_number, hash_id)
        elif index_hash_id != hash_id:
            tableIndex.update(
                inode, block_number, hash_id
            )
            self.cached_indexes.set(inode, block_number, hash_id)
            result["update"] = True

        self.time_spent_writing_blocks += time() - start_time
        return result


    def __flush_old_cached_blocks(self, cached_blocks, writed=False):
        count = 0

        blocksToCompress = {}
        blocksReCompress = {}
        blockSize = {}

        for inode, inode_data in cached_blocks.items():
            for block_number, block_data in inode_data.items():
                if block_data[self.cached_blocks.OFFSET_WRITTEN]:
                    block = block_data[self.cached_blocks.OFFSET_BLOCK]
                    item = self.__write_block_data(int(inode), int(block_number), block)
                    if item["hash"] and (item["new"] or item["recompress"]):
                        blocksToCompress[ item["hash"] ] = item["data"]
                        blocksReCompress[ item["hash"] ] = item["recompress"]
                        blockSize[ item["hash"] ] = item["writed_size"]
                    if writed:
                        count += 1
                else:
                    if not writed:
                        count += 1

        tableBlock = self.getTable("block")
        tableHCT = self.getTable("hash_compression_type")
        tableHSZ = self.getTable("hash_sizes")

        for hash_id, cItem in self.application.compressData(blocksToCompress):
            cdata, cmethod = cItem

            comp_size = len(cdata)

            writed_size = blockSize[ hash_id ]

            cmethod_id = self.getCompressionTypeId(cmethod)

            if blocksReCompress.get(hash_id):
                tableBlock.update(hash_id, cdata)
            else:
                tableBlock.insert(hash_id, cdata)

            hash_CT = tableHCT.get(hash_id)
            if hash_CT:
                if hash_CT["type_id"] != cmethod_id:
                    tableHCT.update(hash_id, cmethod_id)
            else:
                tableHCT.insert(hash_id, cmethod_id)

            hash_SZ = tableHSZ.get(hash_id)
            if hash_SZ:
                if hash_SZ["compressed_size"] != comp_size or hash_SZ["writed_size"] != writed_size:
                    tableHSZ.update(hash_id, writed_size, comp_size)
            else:
                tableHSZ.insert(hash_id, writed_size, comp_size)

            self.bytes_written_compressed += comp_size

        self.time_spent_compressing += self.application.getCompressTool().time_spent_compressing

        return count

    def __cache_block_hook(self): # {{{3

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
            flushed = self.__flush_old_cached_blocks(self.cached_blocks.expired(True), True)
            flushed_writed_blocks += flushed
            flushed_writed_expiredByTime_blocks += flushed

            self.cache_gc_block_write_last_run = time()

            elapsed_time1 = self.cache_gc_block_write_last_run - start_time1
            self.time_spent_flushing_writed_block_cache += elapsed_time1
            self.time_spent_flushing_writedByTime_block_cache += elapsed_time1


        start_time1 = time()
        if start_time1 - self.cache_gc_block_writeSize_last_run >= self.flushBlockSize_interval:
            if self.cached_blocks.isWritedCacheFull():
                flushed = self.__flush_old_cached_blocks(self.cached_blocks.expireByCount(True), True)
                flushed_writed_blocks += flushed
                flushed_writed_expiredBySize_blocks += flushed

            self.cache_gc_block_writeSize_last_run = time()

            elapsed_time1 = self.cache_gc_block_writeSize_last_run - start_time1
            self.time_spent_flushing_writed_block_cache += elapsed_time1
            self.time_spent_flushing_writedBySize_block_cache += elapsed_time1


        start_time1 = time()
        if start_time1 - self.cache_gc_block_read_last_run >= self.flush_interval:
            flushed = self.cached_blocks.expired(False)
            flushed_readed_blocks += flushed
            flushed_readed_expiredByTime_blocks += flushed

            self.cache_gc_block_read_last_run = time()

            elapsed_time1 = self.cache_gc_block_read_last_run - start_time1
            self.time_spent_flushing_readed_block_cache += elapsed_time1
            self.time_spent_flushing_readedByTime_block_cache += elapsed_time1


        start_time1 = time()
        if start_time1 - self.cache_gc_block_readSize_last_run >= self.flushBlockSize_interval:
            if self.cached_blocks.isReadCacheFull():
                flushed = self.cached_blocks.expireByCount(False)
                flushed_readed_blocks += flushed
                flushed_readed_expiredBySize_blocks += flushed

            self.cache_gc_block_readSize_last_run = time()

            elapsed_time1 = self.cache_gc_block_readSize_last_run - start_time1
            self.time_spent_flushing_readed_block_cache += elapsed_time1
            self.time_spent_flushing_readedBySize_block_cache += elapsed_time1


        if flushed_writed_blocks + flushed_readed_blocks > 0:

            self.__commit_changes()

            elapsed_time = time() - start_time

            self.time_spent_flushing_block_cache += elapsed_time

            self.getLogger().debug("Block cache cleanup: flushed %i writed (%i/t, %i/sz), %i readed (%i/t, %i/sz) blocks in %s",
                                  flushed_writed_blocks, flushed_writed_expiredByTime_blocks, flushed_writed_expiredBySize_blocks,
                                  flushed_readed_blocks, flushed_readed_expiredByTime_blocks, flushed_readed_expiredBySize_blocks,
                                  format_timespan(elapsed_time))

        self.__timing_report_hook()

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
        # 3. Put to cache, it will be rehashed and compressed
        self.cached_blocks.set(inode_id, max_block_number, block, writed=True)

        return

    def __cache_meta_hook(self): # {{{3

        self.__update_mounted_subvolume_time()

        flushed_nodes = 0
        flushed_names = 0
        flushed_attrs = 0
        flushed_xattrs = 0
        flushed_indexes = 0

        start_time = time()
        if start_time - self.cache_gc_meta_last_run >= self.flush_interval:

            flushed_nodes = self.cached_nodes.clear()
            flushed_names = self.cached_names.clear()
            flushed_indexes = self.cached_indexes.expired()

            flushed_xattrs = self.cached_xattrs.clear()

            # Just readed...
            flushed_attrs += self.cached_attrs.expired(False)
            # Just writed/updated...
            flushed_attrs += self.__flush_expired_inodes(self.cached_attrs.expired(True))

            self.__commit_changes()

            self.cache_gc_meta_last_run = time()

        if flushed_attrs + flushed_nodes + flushed_names + flushed_indexes + flushed_xattrs > 0:
            elapsed_time = self.cache_gc_meta_last_run - start_time
            self.getLogger().debug("Meta cache cleanup: flushed %i nodes, %i attrs,  %i xattrs, %i names, %i indexes in %s.",
                                  flushed_nodes, flushed_attrs, flushed_xattrs, flushed_names, flushed_indexes,
                                  format_timespan(elapsed_time))

        self.__timing_report_hook()

        return flushed_attrs + flushed_names + flushed_nodes

    def forced_vacuum(self): # {{{3
        if not self.isReadonly():
            start_time = time()
            self.getLogger().debug("Performing data vacuum (this might take a while) ..")
            sz = 0
            dbsz = self.getManager().getFileSize()
            for table_name in self.getManager().tables:
                sz += self.__vacuum_datatable(table_name, True)
            elapsed_time = time() - start_time

            diffSign = sz > 0 and '+' or '-'

            prsz = format_size(abs(sz))

            self.getLogger().info("Total DB size change after vacuum: %s%.2f%% (%s%s)" % (
                diffSign, abs(sz) * 100.0 / dbsz, diffSign, prsz,))

            self.getLogger().debug("Finished data vacuum in %s.", format_timespan(elapsed_time))
        return

    def __collect_garbage(self): # {{{3
        if self.gc_enabled and not self.isReadonly():
            start_time = time()
            self.getLogger().info("Performing garbage collection (this might take a while) ..")
            self.should_vacuum = False
            clean_stats = False
            gc_funcs = [
                self.__collect_strings,
                self.__collect_inodes_all,
                self.__collect_xattrs,
                self.__collect_links,
                self.__collect_indexes,
            ]
            if not self.getOption("gc_fast_enabled"):
                gc_funcs.append(self.__collect_blocks)
            for method in gc_funcs:
                sub_start_time = time()
                msg = method()
                if msg:
                    clean_stats = True
                    elapsed_time = time() - sub_start_time
                    self.getLogger().info(msg, format_timespan(elapsed_time))

            if clean_stats:
                subv = Subvolume(self)
                subv.clean_stats(self.mounted_subvolume_name)

            elapsed_time = time() - start_time
            self.getLogger().info("Finished garbage collection in %s.", format_timespan(elapsed_time))
        return

    def __collect_strings(self): # {{{4

        tableName = self.getTable("name")

        subv = Subvolume(self)
        treeNameIds = subv.prepareTreeNameIds()

        self.getLogger().debug("Clean unused path segments...")

        countNames = tableName.get_count()
        self.getLogger().debug(" path segments: %d" % countNames)

        count = 0
        current = 0
        proc = ""

        maxCnt = 10000
        curBlock = 0

        while True:

            if current == countNames:
                break

            nameIds = tableName.get_name_ids( curBlock, curBlock+maxCnt )

            current += len(nameIds)

            curBlock += maxCnt
            if not nameIds:
                continue

            # SET magick
            to_delete = nameIds - treeNameIds

            id_str = ",".join((str(_id) for _id in to_delete))
            count += tableName.remove_by_ids(id_str)

            p = "%6.2f%%" % (100.0 * current / countNames)
            if p != proc:
                proc = p
                self.getLogger().debug("%s (count=%d)", proc, count)

        if count > 0:
            self.should_vacuum = True
            self.getTable("name").commit()
            self.__vacuum_datatable("name")
            return "Cleaned up %i unused path segment%s in %%s." % (count, count != 1 and 's' or '')
        return

    def __collect_inodes_all(self): # {{{4

        tableInode = self.getTable("inode")
        tableTree = self.getTable("tree")

        self.getLogger().debug("Clean unused inodes (all)...")

        countInodes = tableInode.get_count()
        self.getLogger().debug(" inodes: %d" % countInodes)

        count = 0
        current = 0
        proc = ""

        curBlock = 0
        maxCnt = 10000

        while True:

            if current == countInodes:
                break

            inodeIds = tableInode.get_inode_ids(curBlock, curBlock+maxCnt)
            current += len(inodeIds)

            curBlock += maxCnt
            if not len(inodeIds):
                continue

            treeInodeIds = tableTree.get_inodes_by_inodes(inodeIds)

            # SET magick
            to_delete = inodeIds - treeInodeIds

            count += tableInode.remove_by_ids(to_delete)

            p = "%6.2f%%" % (100.0 * current / countInodes)
            if p != proc:
                proc = p
                self.getLogger().debug("%s (count=%d)", proc, count)

        if count > 0:
            self.should_vacuum = True
            self.getTable("inode").commit()
            self.__vacuum_datatable("inode")
            return "Cleaned up %i unused inode%s in %%s." % (count, count != 1 and 's' or '')
        return


    def __collect_xattrs(self): # {{{4

        tableXattr = self.getTable("xattr")
        tableInode = self.getTable("inode")

        self.getLogger().debug("Clean unused xattrs...")

        countXattrs = tableXattr.get_count()
        self.getLogger().debug(" xattrs: %d" % countXattrs)

        count = 0
        current = 0
        proc = ""

        curBlock = 0
        maxCnt = 10000

        while True:

            if current == countXattrs:
                break

            inodeIds = tableXattr.get_inode_ids(curBlock, curBlock+maxCnt)
            current += len(inodeIds)

            curBlock += maxCnt
            if not inodeIds:
                continue

            xattrInodeIds = tableInode.get_inodes_by_inodes(inodeIds)

            # SET magick
            to_delete = inodeIds - xattrInodeIds

            count += tableXattr.remove_by_ids(to_delete)

            p = "%6.2f%%" % (100.0 * current / countXattrs)
            if p != proc:
                proc = p
                self.getLogger().debug("%s (count=%d)", proc, count)

        if count > 0:
            self.should_vacuum = True
            self.getTable("xattr").commit()
            self.__vacuum_datatable("xattr")
            return "Cleaned up %i unused xattr%s in %%s." % (count, count != 1 and 's' or '')
        return

    def __collect_links(self): # {{{4

        tableLink = self.getTable("link")
        tableInode = self.getTable("inode")

        self.getLogger().debug("Clean unused links...")

        countLinks = tableLink.get_count()
        self.getLogger().debug(" links: %d" % countLinks)

        count = 0
        current = 0
        proc = ""

        curBlock = 0
        maxCnt = 10000

        while True:

            if current == countLinks:
                break

            inodeIds = tableLink.get_inode_ids(curBlock, curBlock+maxCnt)

            current += len(inodeIds)

            curBlock += maxCnt
            if not inodeIds:
                continue

            linkInodeIds = tableInode.get_inodes_by_inodes(inodeIds)

            # SET magick
            to_delete = inodeIds - linkInodeIds

            count += tableLink.remove_by_ids(to_delete)

            p = "%6.2f%%" % (100.0 * current / countLinks)
            if p != proc:
                proc = p
                self.getLogger().debug("%s (count=%d)", proc, count)

        if count > 0:
            self.should_vacuum = True
            self.getTable("link").commit()
            self.__vacuum_datatable("link")
            return "Cleaned up %i unused link%s in %%s." % (count, count != 1 and 's' or '')
        return

    def __collect_indexes(self): # {{{4

        tableIndex = self.getTable("inode_hash_block")
        tableInode = self.getTable("inode")

        self.getLogger().debug("Clean unused block indexes...")

        countInodes = tableIndex.get_count_uniq_inodes()
        self.getLogger().debug(" block inodes: %d" % countInodes)

        count = 0
        countTrunc = 0
        current = 0
        proc = ""

        curBlock = 0
        maxCnt = 10000

        while True:

            if current == countInodes:
                break

            inodeIds = tableIndex.get_inode_ids(curBlock, curBlock+maxCnt)

            current += len(inodeIds)

            curBlock += maxCnt
            if not len(inodeIds):
                continue

            indexInodeIds = tableInode.get_inodes_by_inodes(inodeIds)

            # SET magick
            to_delete = inodeIds - indexInodeIds
            to_trunc = inodeIds - to_delete

            count += tableIndex.remove_by_inodes(to_delete)

            # Slow?
            inodeSizes = tableInode.get_sizes_by_id(to_trunc)
            for inode_id in to_trunc:
                size = inodeSizes.get(inode_id, -1)
                if size < 0:
                    continue

                inblock_offset = size % self.block_size
                max_block_number = int(floor(1.0 * (size - inblock_offset) / self.block_size))

                trunced = tableIndex.delete_by_inode_number_more(inode_id, max_block_number)
                countTrunc += len(trunced)

            p = "%6.2f%%" % (100.0 * current / countInodes)
            if p != proc:
                proc = p
                self.getLogger().debug("%s (count=%d, trunced=%d)", proc, count, countTrunc)

        count += countTrunc

        if count > 0:
            self.should_vacuum = True
            self.getTable("inode_hash_block").commit()
            self.__vacuum_datatable("inode_hash_block")
            return "Cleaned up %i unused index entr%s in %%s." % (count, count != 1 and 'ies' or 'y')
        return

    def __collect_blocks(self): # {{{4

        tableHash = self.getTable("hash")
        tableBlock = self.getTable("block")
        tableHCT = self.getTable("hash_compression_type")
        tableHSZ = self.getTable("hash_sizes")

        subv = Subvolume(self)
        indexHashIds = subv.prepareIndexHashIds()

        self.getLogger().debug("Clean unused data blocks and hashes...")

        countHashes = tableHash.get_count()
        self.getLogger().debug(" hashes: %d" % countHashes)

        count = 0
        current = 0
        proc = ""

        _curBlock = 0
        maxCnt = 10000

        while True:

            if current == countHashes:
                break

            hashIds = tableHash.get_hash_ids(_curBlock, _curBlock+maxCnt)

            current += len(hashIds)

            _curBlock += maxCnt
            if not hashIds:
                continue

            # SET magick
            to_delete = hashIds - indexHashIds

            id_str = ",".join((str(_id) for _id in to_delete))
            count += tableHash.remove_by_ids(id_str)
            tableBlock.remove_by_ids(id_str)
            tableHCT.remove_by_ids(id_str)
            tableHSZ.remove_by_ids(id_str)

            p = "%6.2f%%" % (100.0 * current / countHashes)
            if p != proc:
                proc = p
                self.getLogger().debug("%s (count=%d)", proc, count)

        self.getManager().commit()

        if count > 0:
            self.should_vacuum = True
            self.getTable("hash").commit()
            self.__vacuum_datatable("hash")
            self.getTable("block").commit()
            self.__vacuum_datatable("block")
            self.getTable("hash_compression_type").commit()
            self.__vacuum_datatable("hash_compression_type")
            self.getTable("hash_sizes").commit()
            self.__vacuum_datatable("hash_sizes")
            return "Cleaned up %i unused data block%s and hashes in %%s." % (
                count, count != 1 and 's' or '',
            )
        return


    def __vacuum_datatable(self, tableName, getsize=False): # {{{4
        msg = ""
        sz = 0
        sub_start_time = time()
        if self.should_vacuum and self.gc_vacuum_enabled:
            self.getLogger().debug(" vacuum %s table" % tableName)
            sz += self.getTable(tableName).vacuum()
            msg = "  vacuumed SQLite data store in %s."
        if msg:
            elapsed_time = time() - sub_start_time
            self.getLogger().debug(msg, format_timespan(elapsed_time))
        if getsize:
            return sz
        return msg

    def __commit_changes(self): # {{{3
        if not self.use_transactions:
            self.getManager().commit()
            self.getManager().begin()


    def __rollback_changes(self): # {{{3
        if not self.use_transactions:
            self.getLogger().note('Rolling back changes')
            self.getManager().rollback()


    def __except_to_status(self, method, exception, code=errno.ENOENT): # {{{3
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
