# -*- coding: utf8 -*-

# Imports. {{{1
import sys

# Try to load the required modules from Python's standard library.
from dedupsqlfs.lib import constants

try:
    from io import BytesIO
    import errno
    import hashlib
    import logging
    import math
    import os
    import sqlite3
    import stat
    import time
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
from dedupsqlfs.my_formats import format_size, format_timespan
from dedupsqlfs.get_memory_usage import get_real_memory_usage, get_memory_usage
from dedupsqlfs.lib.cache import CacheTTLseconds
from dedupsqlfs.lib.storage import StorageTTLseconds
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
        self.cache_gc_last_run = time.time()
        self.cache_gc_meta_last_run = time.time()
        self.cache_gc_block_write_last_run = time.time()
        self.cache_gc_block_read_last_run = time.time()
        self.cache_timeout = 5
        self.cache_meta_timeout = 15
        self.cache_block_write_timeout = 5
        self.cache_block_read_timeout = 10
        self.cache_block_write_size = 256*1024*1024
        self.cache_block_read_size = 256*1024*1024

        self.cached_nodes = CacheTTLseconds()
        self.cached_attrs = CacheTTLseconds()

        self.cached_blocks = StorageTTLseconds()

        self.calls_log_filter = []

        self.fs_mounted_at = time.time()
        self.mounted_snapshot = None

        self.gc_enabled = True
        self.gc_umount_enabled = True
        self.gc_vacuum_enabled = False
        self.gc_hook_last_run = time.time()
        self.gc_interval = 60

        self.link_mode = stat.S_IFLNK | 0o777

        self.memory_usage = 0
        self.memory_usage_real = 0
        self.opcount = 0
        self.should_vacuum = False

        self.root_mode = stat.S_IFDIR | 0o755

        self.time_spent_caching_nodes = 0
        self.time_spent_hashing = 0
        self.time_spent_interning = 0
        self.time_spent_querying_tree = 0
        self.time_spent_reading = 0
        self.time_spent_traversing_tree = 0
        self.time_spent_writing = 0
        self.time_spent_writing_blocks = 0
        self.time_spent_flushing_block_cache = 0
        self.time_spent_compressing = 0

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
        if not self.manager:
            from dedupsqlfs.db.manager import DbManager
            self.manager = DbManager(dbname=self.getOption("name"))
            self.manager.setSynchronous(self.getOption("synchronous"))
            self.manager.setAutocommit(self.getOption("use_transactions"))
            self.manager.setBasepath(os.path.expanduser(self.getOption("data")))
        return self.manager

    def getTable(self, table_name):
        return self.getManager().getTable(table_name)

    def getApplication(self):
        return self.application

    def setReadonly(self, flag=True):
        self.getApplication().setReadonly(flag)
        return self

    def isReadonly(self):
        return self.getApplication().isReadonly()

    def getOption(self, key):
        return self.getApplication().getOption(key)

    def getLogger(self):
        return self.getApplication().getLogger()


    def getCompressionTypeName(self, comp_id):
        if not self._compression_types:
            self._compression_types = self.getManager().getTable("compression_type").getAll()
        return self._compression_types[comp_id]

    def getCompressionTypeId(self, name):
        if not self._compression_types_revert:
            self._compression_types_revert = self.getManager().getTable("compression_type").getAllRevert()
        return self._compression_types_revert[name]

    # --------------------------------------------------------------------------------------
    #       FUSE OPERATIONS
    # --------------------------------------------------------------------------------------

    def access(self, inode, mode, ctx): # {{{3
        self.__log_call('access', 'access(inode=%i, mode=%o, ctx=%r)', inode, mode, ctx)
        inode = self.__fix_inode_if_requested_root(inode)
        if mode != os.F_OK and not self.__access(inode, mode, ctx):
            return False
        return True

    def create(self, inode_parent, name, mode, flags, ctx):
        self.__log_call('create', 'create(inode_parent=%i, name=%r, mode=%o, flags=%o, ctx=%r)',
                        inode_parent, name, mode, flags, ctx)
        if self.isReadonly(): return errno.EROFS

        inode_parent = self.__fix_inode_if_requested_root(inode_parent)

        try:
            node = self.__get_tree_node_by_parent_inode_and_name(inode_parent, name)
        except FUSEError as e:
            node = None
            if e.errno != errno.ENOENT:
                raise

        if not node:
            inode, parent_ino = self.__insert(inode_parent, name, mode, 0, ctx)
        else:
            inode = node["inode_id"]

        self.__commit_changes()

        fh = self.open(inode, flags)
        attrs = self.__getattr(inode)

        self.__log_call('create', '->(inode=%i, attrs=%r)',
                        fh, attrs)

        return (fh, attrs,)

    def destroy(self): # {{{3
        try:
            self.__log_call('destroy', 'destroy()')
            self.getLogger().info("Umount file system in process...")
            if not self.getOption("readonly"):
                # Flush all cached blocks
                self.getLogger().info("Flush remaining blocks.")
                self.__flush_old_cached_blocks(self.cached_blocks.clear())

                if self.getOption("gc_umount_enabled"):
                    # Force vacuum on umount
                    self.gc_enabled = True
                    self.__collect_garbage()
                self.getLogger().info("Committing outstanding changes.")
                self.getManager().commit()
            if self.getOption("verbosity"):
                self.__print_stats()
            self.getManager().close()
            return 0
        except Exception as e:
            raise self.__except_to_status('destroy', e, errno.EIO)

    def flush(self, fh):
        try:
            self.__log_call('flush', 'flush(fh=%i)', fh)
            if self.isReadonly(): raise FUSEError(errno.EROFS)
            #self.__flush_inode_cached_blocks(fh, clean=False)
            self.__cache_block_hook()
            self.__commit_changes()
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('flush', e, errno.EIO)

    def forget(self, inode_list):
        try:
            self.__log_call('forget', 'forget(inode_list=%r)', inode_list)
            if self.isReadonly(): raise FUSEError(errno.EROFS)
            # clear block cache
            self.__cache_block_hook()
            #for ituple in inode_list:
            #    for inode in ituple:
            #        self.__flush_inode_cached_blocks(inode, clean=True)
            self.__commit_changes()
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('forget', e, errno.EIO)

    def fsync(self, fh, datasync):
        try:
            self.__log_call('fsync', 'fsync(fh=%i, datasync=%r)', fh, datasync)
            if self.isReadonly(): raise FUSEError(errno.EROFS)
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('fsync', e, errno.EIO)

    def fsyncdir(self, fh, datasync):
        try:
            self.__log_call('fsyncdir', 'fsyncdir(fh=%i, datasync=%r)', fh, datasync)
            if self.isReadonly(): raise FUSEError(errno.EROFS)
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('fsyncdir', e, errno.EIO)

    def getattr(self, inode): # {{{3
        self.__log_call('getattr', 'getattr(inode=%r)', inode)
        inode = self.__fix_inode_if_requested_root(inode)
        result = self.__getattr(inode)
        return result

    def getxattr(self, inode, name): # {{{3
        self.__log_call('getxattr', 'getxattr(inode=%r, name=%r)', inode, name)
        inode = self.__fix_inode_if_requested_root(inode)
        xattrs = self.getTable("xattr").find_by_inode(inode)
        if not xattrs:
            raise FUSEError(llfuse.ENOATTR)
        if name not in xattrs:
            raise FUSEError(llfuse.ENOATTR)
        return xattrs[name]

    def init(self): # {{{3
        try:
            # Process the custom command line options defined in __init__().
            if self.getOption("block_size") is not None:
                self.block_size = self.getOption("block_size")

            if self.getOption("compression_method") is not None:
                self.compression_method = self.getOption("compression_method")
            if self.getOption("hash_function") is not None:
                self.hash_function = self.getOption("hash_function")

            if self.getOption("use_cache") is not None:
                self.cache_enabled = self.getOption("use_cache")
            if self.getOption("cache_timeout") is not None:
                self.cache_timeout = self.getOption("cache_timeout")
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
                self.cached_nodes.set_max_ttl(0)
                self.cached_attrs.set_max_ttl(0)
            else:
                if self.block_size:
                    self.cached_blocks.setBlockSize(self.block_size)
                self.cached_blocks.setMaxWriteTtl(self.cache_block_write_timeout)
                self.cached_blocks.setMaxReadTtl(self.cache_block_read_timeout)
                if self.cache_block_write_size:
                    if self.getOption("memory_limit") and not self.getOption("cache_block_write_size"):
                        if self.cache_block_write_size > 256*self.block_size:
                            self.cache_block_write_size = 256*self.block_size
                    self.cached_blocks.setMaxWriteCacheSize(self.cache_block_write_size)

                if self.cache_block_read_size:
                    if self.getOption("memory_limit") and not self.getOption("cache_block_read_size"):
                        if self.cache_block_read_size > 256*self.block_size:
                            self.cache_block_read_size = 256*self.block_size
                    self.cached_blocks.setMaxReadCacheSize(self.cache_block_read_size)

                self.cached_nodes.set_max_ttl(self.cache_meta_timeout)
                self.cached_attrs.set_max_ttl(self.cache_meta_timeout)

            if self.getOption("synchronous") is not None:
                self.synchronous = self.getOption("synchronous")
            if self.getOption("use_transactions") is not None:
                self.use_transactions = self.getOption("use_transactions")
            # Initialize the logging and database subsystems.
            self.__log_call('init', 'init()')

            # Disable synchronous operation. This is supposed to make SQLite perform
            # MUCH better but it has to be enabled wit --nosync because you might
            # lose data when the file system isn't cleanly unmounted...
            if not self.synchronous and not self.isReadonly():
                self.getLogger().warning("Warning: Disabling synchronous operation, you might lose data..")
                self.getManager().close()
                self.getManager().setSynchronous(self.synchronous)

            self.mounted_snapshot = self.getOption("snapshot_mount")
            if self.mounted_snapshot:
                self.mounted_snapshot = self.mounted_snapshot.encode('utf8')
                if not self.mounted_snapshot.startswith(b'@'):
                    self.mounted_snapshot = b'@' + self.mounted_snapshot

            if not self.isReadonly():
                self.__init_store()
            self.__select_snapshot()
            self.__get_opts_from_db()
            # Make sure the hash function is (still) valid (since the database was created).

            try:
                # Get a reference to the hash function.
                hashlib.new(self.hash_function)
            except:
                self.getLogger().critical("Error: The selected hash function %r doesn't exist!", self.hash_function)
                sys.exit(1)

            # Select the compression method (if any) after potentially reading the
            # configured block size that was used to create the database (see the
            # set_block_size() call).
            #self.__select_compress_method(options, silent)
            return 0
        except Exception as e:
            self.__except_to_status('init', e, errno.EIO)
            # Bug fix: Break the mount point when initialization failed with an
            # exception, because self.conn might not be valid, which results in
            # an internal error message for every FUSE API call...
            os._exit(1)

    def link(self, inode, new_parent_inode, new_name): # {{{3
        self.__log_call('link', 'link(inode=%r, parent_inode=%r, new_name=%r)', inode, new_parent_inode, new_name)
        if self.isReadonly(): raise FUSEError(errno.EROFS)

        inode = self.__fix_inode_if_requested_root(inode)
        new_parent_inode = self.__fix_inode_if_requested_root(new_parent_inode)

        parent_node = self.__get_tree_node_by_inode(new_parent_inode)
        string_id = self.__intern(new_name)

        treeTable = self.getTable("tree")
        inodeTable = self.getTable("inode")

        treeTable.insert(parent_node["id"], string_id, inode)
        inodeTable.inc_nlinks(inode)
        if inodeTable.get_mode(inode) & stat.S_IFDIR:
            inodeTable.inc_nlinks(parent_node["inode_id"])

        self.__commit_changes()
        return self.__getattr(inode)

    def listxattr(self, inode):
        try:
            self.__log_call('listxattr', 'listxattr(inode=%r)', inode)
            if self.isReadonly(): raise FUSEError(errno.EROFS)
            inode = self.__fix_inode_if_requested_root(inode)
            xattrs = self.getTable("xattr").find_by_inode(inode)
            if not xattrs:
                return []
            return xattrs.keys()
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('listxattr', e, errno.EIO)

    def lookup(self, parent_inode, name):
        self.__log_call('lookup', 'lookup(parent_inode=%r, name=%r)', parent_inode, name)

        parent_inode = self.__fix_inode_if_requested_root(parent_inode)

        node = self.__get_tree_node_by_parent_inode_and_name(parent_inode, name)
        attr = self.__getattr(node["inode_id"])

        self.__log_call('lookup', '->(node=%r, attr=%r)', node, attr)

        self.__cache_meta_hook()
        return attr

    def mkdir(self, parent_inode, name, mode, ctx): # {{{3
        try:
            self.__log_call('mkdir', 'mkdir(parent_inode=%i, name=%r, mode=%o, ctx=%r)',
                            parent_inode, name, mode, ctx)
            if self.isReadonly(): raise FUSEError(errno.EROFS)

            parent_inode = self.__fix_inode_if_requested_root(parent_inode)

            inode, parent_ino = self.__insert(parent_inode, name, mode | stat.S_IFDIR, len(name) + 4 + 13*4 + 5*4 + 5*4, ctx)
            self.getManager().getTable("inode").inc_nlinks(parent_ino)
            self.__commit_changes()
            return self.__getattr(inode)
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('mkdir', e, errno.EIO)

    def mknod(self, parent_inode, name, mode, rdev, ctx): # {{{3
        try:
            self.__log_call('mknod', 'mknod(parent_inode=%i, name=%r, mode=%o, rdev=%i, ctx=%r)',
                            parent_inode, name, mode, rdev, ctx)
            if self.isReadonly(): return -errno.EROFS

            parent_inode = self.__fix_inode_if_requested_root(parent_inode)

            inode, parent_ino = self.__insert(parent_inode, name, mode, 0, ctx, rdev)
            self.__commit_changes()
            return self.__getattr(inode)
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('mknod', e, errno.EIO)

    def open(self, inode, flags): # {{{3
        """
        Return filehandler ID
        """
        self.__log_call('open', 'open(inode=%i, flags=%o)', inode, flags)
        if self.isReadonly(): raise FUSEError(errno.EROFS)
        # Make sure the file exists?

        inode = self.__fix_inode_if_requested_root(inode)

        node = self.__get_tree_node_by_inode(inode)
        if not node:
            raise FUSEError(errno.ENOENT)
        # Make sure the file is readable and/or writable.
        return node["inode_id"]

    def opendir(self, inode): # {{{3
        self.__log_call('opendir', 'opendir(inode=%i)', inode)
        # Make sure the file exists?

        inode = self.__fix_inode_if_requested_root(inode)

        node = self.__get_tree_node_by_inode(inode)
        if not node:
            raise FUSEError(errno.ENOENT)
        # Make sure the file is readable and/or writable.
        return node["inode_id"]

    def read(self, fh, offset, size): # {{{3
        """
        @param fh: file handler number - inode.id
        @type  fh: int
        """
        try:
            start_time = time.time()

            self.__log_call('read', 'read(fh=%i, offset=%i, size=%i)', fh, offset, size, )

            data = self.__get_block_data_by_offset(fh, offset, size)
            self.bytes_read += len(data)

            # Too much output
            # self.__log_call('read', 'readed: size=%s, data=%r', len(data), data, )
            self.__log_call('read', 'readed: size=%s', len(data), )

            self.__cache_block_hook()

            self.time_spent_reading += time.time() - start_time
            return data
        except Exception as e:
            return self.__except_to_status('read', e, code=errno.EIO)

    def readdir(self, fh, offset): # {{{3
        """
        @param fh: file handler number - inode.id
        @type  fh: int
        """
        inode = fh

        self.__log_call('readdir', 'readdir(%r, %i)', inode, offset)

        cur_node = self.__get_tree_node_by_inode(inode)

        for node in self.getTable("tree").get_children(cur_node["id"]):
            if node["id"] <= offset:
                continue
            name = self.getTable("name").get(node["name_id"])
            attrs = self.__getattr(node["inode_id"])
            self.__log_call('readdir', '->(name=%r, attrs=%r, node=%i)', name, attrs, node["id"])
            yield (name, attrs, node["id"],)


    def readlink(self, inode): # {{{3
        self.__log_call('readlink', 'readlink(inode=%i)', inode)

        inode = self.__fix_inode_if_requested_root(inode)

        target = self.getTable("link").find_by_inode(inode)
        if not target:
            raise FUSEError(errno.ENOENT)
        return target

    def release(self, fh): # {{{3
        self.__log_call('release', 'release(fh=%i)', fh)
        #self.__flush_inode_cached_blocks(fh, clean=True)
        self.__cache_block_hook()
        self.__cache_meta_hook()
        self.__gc_hook()
        return 0

    def releasedir(self, fh):
        self.__log_call('releasedir', 'releasedir(fh=%r)', fh)
        self.__cache_meta_hook()
        self.__gc_hook()
        return 0

    def removexattr(self, inode, name):
        try:
            self.__log_call('removexattr', 'removexattr(inode=%i, name=%r)', inode, name)
            if self.isReadonly():
                raise FUSEError(errno.EROFS)

            inode = self.__fix_inode_if_requested_root(inode)

            xattrs = self.getTable("xattr").find_by_inode(inode)
            if not xattrs:
                raise FUSEError(llfuse.ENOATTR)
            if name not in xattrs:
                raise FUSEError(llfuse.ENOATTR)
            del xattrs[name]
            self.getTable("xattr").update(inode, xattrs)
            self.__commit_changes()
            return 0
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('rmdir', e, errno.ENOENT)


    def rename(self, inode_parent_old, name_old, inode_parent_new, name_new): # {{{3
        try:
            self.__log_call('rename', 'rename(inode_parent_old=%i, name_old=%r, inode_parent_new=%i, name_new=%r)',
                            inode_parent_old, name_old, inode_parent_new, name_new)
            if self.isReadonly():
                raise FUSEError(errno.EROFS)

            inode_parent_old = self.__fix_inode_if_requested_root(inode_parent_old)
            inode_parent_new = self.__fix_inode_if_requested_root(inode_parent_new)

            node = self.__get_tree_node_by_parent_inode_and_name(inode_parent_old, name_old)

            # Try to remove the existing target path (if if exists).
            # NB: This also makes sure target directories are empty.
            try:
                node = self.__get_tree_node_by_parent_inode_and_name(inode_parent_new, name_new)
                if node:
                    self.unlink(inode_parent_new, name_new)
            except FUSEError as e:
                # Ignore errno.ENOENT, re raise other exceptions.
                if e.errno != errno.ENOENT: raise

            # Link the new path to the same inode as the old path.
            self.link(node["inode_id"], inode_parent_new, name_new)
            # Finally unlink the old path.
            self.unlink(inode_parent_old, name_old)
            self.__gc_hook()
            self.__cache_meta_hook()
            self.__commit_changes()
            return 0
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('rename', e, errno.ENOENT)

    def rmdir(self, inode_parent, name): # {{{3
        try:
            self.__log_call('rmdir', 'rmdir(inode_parent=%i, name=%r)', inode_parent, name)
            if self.isReadonly():
                raise FUSEError(errno.EROFS)

            inode_parent = self.__fix_inode_if_requested_root(inode_parent)

            self.__remove(inode_parent, name, check_empty=True)
            self.__gc_hook()
            self.__commit_changes()
            return 0
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('rmdir', e, errno.ENOENT)

    def setattr(self, inode, attr):
        """
        @param  inode:  inode ID
        @type   inode:  int
        @param  attr:   attributes
        @type   attr:   fuse.EntryAttributes
        """
        try:
            self.__log_call('setattr', 'setattr(inode=%i, attr=%r)', inode, attr)
            if self.isReadonly(): return -errno.EROFS

            inode = self.__fix_inode_if_requested_root(inode)

            row = self.__get_inode_row(inode)

            set_ctime = False
            update_db = False
            new_data = {}

            if attr.st_mode is not None:
                if row["mode"] != attr.st_mode:
                    new_data["mode"] = attr.st_mode
                    set_ctime = True

            if attr.st_uid is not None:
                if row["uid"] != attr.st_uid:
                    new_data["uid"] = attr.st_uid
                    set_ctime = True

            if attr.st_gid is not None:
                if row["gid"] != attr.st_gid:
                    new_data["gid"] = attr.st_gid
                    set_ctime = True

            if attr.st_atime is not None:
                atime_i, atime_ns = self.__get_time_tuple(attr.st_atime)
                if row["atime"] != atime_i or row["atime_ns"] != atime_ns:
                    new_data["atime"] = atime_i
                    new_data["atime_ns"] = atime_i
                    set_ctime = True

            if attr.st_mtime is not None:
                mtime_i, mtime_ns = self.__get_time_tuple(attr.st_mtime)
                if row["mtime"] != mtime_i or row["mtime_ns"] != mtime_ns:
                    new_data["mtime"] = mtime_i
                    new_data["mtime_ns"] = mtime_i
                    set_ctime = True

            if attr.st_ctime is not None:
                ctime_i, ctime_ns = self.__get_time_tuple(attr.st_ctime)
                if row["ctime"] != ctime_i or row["ctime_ns"] != ctime_ns:
                    new_data["ctime"] = ctime_i
                    new_data["ctime_ns"] = ctime_i
                    update_db = True
            elif set_ctime:
                ctime_i, ctime_ns = self.__newctime_tuple()
                new_data["ctime"] = ctime_i
                new_data["ctime_ns"] = ctime_ns
                update_db = True

            if update_db:

                self.getLogger().debug("times: atime=%r, mtime=%r, ctime=%r",
                                       attr.st_atime, attr.st_mtime, attr.st_ctime)
                self.getLogger().debug("new attrs: %r", new_data)

                self.getTable("inode").update_data(inode, new_data)
                self.__commit_changes()

                row.update(new_data)
                self.cached_attrs.set(inode, row)

            self.__cache_meta_hook()

            return self.__fill_attr_inode_row(row)
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('setattr', e, errno.EIO)

    def setxattr(self, inode, name, value):
        try:
            self.__log_call('setxattr', 'setxattr(inode=%i, name=%r, value=%r)', inode, name, value)
            if self.isReadonly():
                raise FUSEError(errno.EROFS)

            inode = self.__fix_inode_if_requested_root(inode)

            xattrs = self.getTable("xattr").find_by_inode(inode)

            newxattr = False
            if not xattrs:
                newxattr = True
                xattrs = {}
            xattrs[name] = value

            if not newxattr:
                self.getTable("xattr").update(inode, xattrs)
            else:
                self.getTable("xattr").insert(inode, xattrs)

            self.__commit_changes()
            return 0
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('rmdir', e, errno.ENOENT)

    def stacktrace(self):
        self.__log_call('stacktrace', 'stacktrace()')

    def statfs(self): # {{{3
        try:
            self.__log_call('statfs', 'statfs()')
            # Use os.statvfs() to report the host file system's storage capacity.
            # host_fs = os.statvfs(self.getOption("data"))
            stats = llfuse.StatvfsData()

            apparent_size = self.getApparentSize()

            # The total number of free blocks available to a non privileged process.
            # stats.f_bavail = host_fs.f_bsize * host_fs.f_bavail / self.block_size
            stats.f_bavail = (apparent_size - self.getManager().getFileSize()) / self.block_size
            if stats.f_bavail < 0:
                stats.f_bavail = 0
            # The total number of free blocks in the file system.
            # stats.f_bfree = host_fs.f_frsize * host_fs.f_bfree / self.block_size
            stats.f_bfree = stats.f_bavail
            # The total number of blocks in the file system in terms of f_frsize.
            # stats.f_blocks = host_fs.f_frsize * host_fs.f_blocks / self.block_size
            stats.f_blocks = apparent_size / self.block_size
            # stats.f_blocks = self.getTable("block").count()
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
            self.__log_call('symlink', 'symlink(inode_parent=%i, name=%r, target=%r, ctx=%r)',
                            inode_parent, name, target, ctx)
            if self.isReadonly(): return -errno.EROFS

            inode_parent = self.__fix_inode_if_requested_root(inode_parent)

            # Create an inode to hold the symbolic link.
            inode, parent_ino = self.__insert(inode_parent, name, self.link_mode, len(target), ctx)
            # Save the symbolic link's target.
            self.getTable("link").insert(inode, target)
            self.__commit_changes()
            attr = self.__getattr(inode)
            self.__cache_meta_hook()
            return attr
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('symlink', e, errno.EIO)

    def unlink(self, parent_inode, name): # {{{3
        try:
            self.__log_call('unlink', 'unlink(parent_inode=%i, name=%r)', parent_inode, name)
            if self.isReadonly():
                raise FUSEError(errno.EROFS)

            parent_inode = self.__fix_inode_if_requested_root(parent_inode)

            self.__remove(parent_inode, name)
            self.__gc_hook()
            self.__commit_changes()
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('unlink', e, errno.ENOENT)

    def write(self, fh, offset, buf): # {{{3
        try:
            start_time = time.time()

            # Too much output
            # self.__log_call('write', 'write(fh=%i, offset=%i, buf=%r)', fh, offset, buf)
            self.__log_call('write', 'write(fh=%i, offset=%i)', fh, offset)

            #length = len(buf)
            #self.__log_call('write', 'length(buf)=%i', length)

            length = self.__write_block_data_by_offset(fh, offset, buf)

            self.__log_call('write', 'length(writed)=%i', length)

            attrs = self.__get_inode_row(fh)
            if attrs["size"] < offset + length:
                self.getTable("inode").set_size(fh, offset + length)
                attrs["size"] = offset + length
                self.cached_attrs.set(fh, attrs)

            # self.bytes_written is incremented from release().
            self.__cache_meta_hook()
            self.__cache_block_hook()
            self.__commit_changes()

            self.time_spent_writing += time.time() - start_time
            return length
        except Exception as e:
            self.__rollback_changes()
            raise self.__except_to_status('write', e, errno.EIO)


    # ---------------------------- Miscellaneous methods: {{{2

    def getApparentSize(self, use_subvol=True):
        manager = self.getManager()

        curTree = manager.getTable("tree").getCursor()
        curInode = manager.getTable("inode").getCursor()

        if use_subvol:
            curTree.execute("SELECT inode_id FROM tree WHERE subvol_id=?", (manager.getTable("tree").getSelectedSubvolume(),))
        else:
            curTree.execute("SELECT inode_id FROM tree")

        apparent_size = 0
        while True:
            treeItem = curTree.fetchone()
            if not treeItem:
                break

            curInode.execute("SELECT `size` FROM `inode` WHERE id=?", (treeItem["inode_id"],))
            apparent_size += curInode.fetchone()["size"]
        return apparent_size

    def __fix_inode_if_requested_root(self, inode):
        if inode == llfuse.ROOT_INODE and not self.getOption("disable_subvolumes"):
            if self.mounted_snapshot:
                node = self.__get_tree_node_by_parent_inode_and_name(inode, self.mounted_snapshot)
                if node:
                    return node["inode_id"]
        return inode

    def __update_mounted_subvolume_time(self):
        if self.mounted_snapshot and not self.getOption("disable_subvolumes"):
            node = self.__get_tree_node_by_parent_inode_and_name(llfuse.ROOT_INODE, self.mounted_snapshot)
            if node:
                self.getTable('subvolume').update_time(node["id"])
        return self

    def __get_tree_node_by_parent_inode_and_name(self, parent_inode, name):

        node = None
        if self.cache_enabled:
            node = self.cached_nodes.get((parent_inode, name))

        if not node:

            start_time = time.time()

            name_id = self.getTable("name").find(name)
            if not name_id:
                self.getLogger().debug("! No name %r found, cant get name_id" % name)
                raise FUSEError(errno.ENOENT)

            par_node = self.__get_tree_node_by_inode(parent_inode)
            if not par_node:
                self.getLogger().debug("! No parent inode %i found, cant get tree node" % parent_inode)
                raise FUSEError(errno.ENOENT)

            node = self.getTable("tree").find_by_parent_name(par_node["id"], name_id)
            if not node:
                self.getLogger().debug("! No node %i and name %i found, cant get tree node" % (par_node["id"], name_id,))
                raise FUSEError(errno.ENOENT)

            if self.cache_enabled:
                self.cached_nodes.set((parent_inode, name), node)

            self.time_spent_caching_nodes += time.time() - start_time

        return node

    def __get_tree_node_by_inode(self, inode):
        node = None
        if self.cache_enabled:
            node = self.cached_nodes.get(inode)

        if not node:

            start_time = time.time()

            node = self.getTable("tree").find_by_inode(inode)
            if not node:
                self.getLogger().debug("! No inode %i found, cant get tree node" % inode)
                raise FUSEError(errno.ENOENT)

            if self.cache_enabled:
                self.cached_nodes.set(inode, node)

            self.time_spent_caching_nodes += time.time() - start_time

        return node

    def __get_inode_row(self, inode_id):
        row = None
        if self.cache_enabled:
            row = self.cached_attrs.get(inode_id)
        if not row:
            start_time = time.time()

            row = self.getTable("inode").get(inode_id)
            if not row:
                self.getLogger().debug("! No inode %i found, cant get row" % inode_id)
                raise FUSEError(errno.ENOENT)

            if self.cache_enabled:
                self.cached_attrs.set(inode_id, row)

            self.time_spent_caching_nodes += time.time() - start_time

        return row

    def __fill_attr_inode_row(self, row): # {{{3
        result = llfuse.EntryAttributes()

        result.entry_timeout = self.getOption("cache_timeout")
        result.attr_timeout = self.getOption("cache_timeout")
        result.st_ino       = int(row["id"])
        # http://stackoverflow.com/questions/11071996/what-are-inode-generation-numbers
        result.generation   = 0
        result.st_nlink     = int(row["nlinks"])
        result.st_mode      = int(row["mode"])
        result.st_uid       = int(row["uid"])
        result.st_gid       = int(row["gid"])
        result.st_rdev      = int(row["rdev"])
        result.st_size      = int(row["size"])
        result.st_atime     = float(row["atime"]) + float(row["atime_ns"]) / 10**9
        result.st_mtime     = float(row["mtime"]) + float(row["mtime_ns"]) / 10**9
        result.st_ctime     = float(row["ctime"]) + float(row["ctime_ns"]) / 10**9
        result.st_blksize   = int(self.block_size)
        result.st_blocks    = int(result.st_size / self.block_size)
        return result


    def __getattr(self, inode_id): # {{{3
        row = self.__get_inode_row(inode_id)
        self.__cache_meta_hook()
        return self.__fill_attr_inode_row(row)


    def __get_block_from_cache(self, inode, block_number):

        block = self.cached_blocks.get(inode, block_number)

        if block is None:

            self.getLogger().debug("get block from DB")

            tableIndex = self.getTable("inode_hash_block")
            tableBlock = self.getTable("block")

            block_index = tableIndex.get_by_inode_number(inode, block_number)
            if not block_index:
                self.getLogger().debug("-- new block")
                block = BytesIO()
            else:
                item = tableBlock.get(block_index["hash_id"])

                self.getLogger().debug("-- decompress block")
                self.getLogger().debug("-- db size: %s" % len(item["data"]))

                block = self.__decompress(item["data"], item["compression_type_id"])

                self.getLogger().debug("-- decomp size: %s" % len(block.getvalue()))

            self.cached_blocks.set(inode, block_number, block)
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
        inblock_offset = offset % self.block_size
        first_block_number = int(math.floor((offset - inblock_offset) / self.block_size))
        raw_data = BytesIO()

        read_blocks = int(math.ceil(1.0 * size / self.block_size))

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

            self.getLogger().debug("block number = %s, read size = %s, block size = %s" % (n, read_size, len(block.getvalue())))

            raw_data.write(block.read(read_size))
            readed_size += read_size

        self.bytes_read += readed_size

        return raw_data.getvalue()

    def __decompress(self, block_data, compression_type_id):
        compression = self.getCompressionTypeName( compression_type_id )
        self.getLogger().debug("-- decompress block: type = %s" % compression)
        return BytesIO(self.getApplication().decompressData(compression, block_data))

    def __write_block_data_by_offset(self, inode, offset, block_data):
        """
        @param inode: Inode ID
        @type inode: int

        @param offset: in file offset
        @type offset: int

        @param block_data: Data buffer - length can by more than block_size
        @type block_data: bytes
        """
        inblock_offset = offset % self.block_size
        first_block_number = int(math.floor((offset - inblock_offset) / self.block_size))

        io_data = BytesIO(block_data)

        size = len(block_data)

        write_blocks = int(math.ceil(1.0 * size / self.block_size))

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

            self.getLogger().debug("block number = %s, write size = %s, block size = %s" % (n, write_size, len(block.getvalue())))

            block.write(io_data.read(write_size))

            self.cached_blocks.set(inode, n + first_block_number, block, writed=True)

            writed_size += write_size

        return writed_size


    def __init_store(self): # {{{3
        # Bug fix: At this point fuse.FuseGetContext() returns uid = 0 and gid = 0
        # which differs from the info returned in later calls. The simple fix is to
        # use Python's os.getuid() and os.getgid() library functions instead of
        # fuse.FuseGetContext().

        manager = self.getManager()
        optTable = manager.getTable("option")
        inited = optTable.get("inited")

        if not inited:

            inodeTable = manager.getTable("inode")
            nameTable = manager.getTable("name")
            treeTable = manager.getTable("tree")

            uid, gid = os.getuid(), os.getgid()
            t_i, t_ns = self.__newctime_tuple()
            nameRoot = b''

            name_id = nameTable.insert(nameRoot)
            # Directory size: name-row-size + inode-row-size + index-row-size + tree-row-size
            sz = len(nameRoot) + 4 + 13*4 + 5*4 + 5*4
            inode_id = inodeTable.insert(2, self.root_mode, uid, gid, 0, sz, t_i, t_i, t_i, t_ns, t_ns, t_ns)
            treeTable.insert(None, name_id, inode_id)

            nameRoot = constants.ROOT_SUBVOLUME_NAME

            # Need to setup option for right subvolume handling...
            # Disable subvolumes handlers for raw access

            opt_save = self.getApplication().getOption("disable_subvolumes")
            self.getApplication().setOption("disable_subvolumes", True)

            snap = Subvolume(self)
            snap.create(nameRoot)

            self.getApplication().setOption("disable_subvolumes", opt_save)

            self.mounted_snapshot = nameRoot

            for name in ("block_size",):
                optTable.insert(name, "%i" % self.getOption(name))

            for name in ("hash_function",):
                optTable.insert(name, "%s" % self.getOption(name))

            optTable.insert("mounted_snapshot", self.mounted_snapshot)

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


    def __select_snapshot(self):
        manager = self.getManager()
        optTable = manager.getTable("option")

        self.getLogger().debug("__select_snapshot(1): mounted_snapshot=%r" % self.mounted_snapshot)

        if self.mounted_snapshot:
            node =  self.__get_tree_node_by_parent_inode_and_name(llfuse.ROOT_INODE, self.mounted_snapshot)
            if node:

                if not self.getOption("disable_subvolumes"):
                    self.getTable('subvolume').mount_time(node["id"])

                #self.setReadonly(True)

                pass
            else:
                self.mounted_snapshot = constants.ROOT_SUBVOLUME_NAME
        elif not self.getOption("disable_subvolumes"):
            self.mounted_snapshot = constants.ROOT_SUBVOLUME_NAME

        if self.mounted_snapshot:
            optTable.update("mounted_snapshot", self.mounted_snapshot)

            node =  self.__get_tree_node_by_parent_inode_and_name(llfuse.ROOT_INODE, self.mounted_snapshot)
            if node:
                self.getTable('tree').selectSubvolume(node["id"])

        self.getLogger().debug("__select_snapshot(2): mounted_snapshot=%r" % self.mounted_snapshot)

        return


    def __log_call(self, fun, msg, *args): # {{{3
        # To disable all __log_call() invocations:
        #  :%s/^\(\s\+\)\(self\.__log_call\)/\1#\2
        # To re enable them:
        #  :%s/^\(\s\+\)#\(self\.__log_call\)/\1\2
        if self.calls_log_filter == [] or fun in self.calls_log_filter:
            self.getLogger().debug(msg, *args)


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
        nlinks = mode & stat.S_IFDIR and 2 or 1

        manager = self.getManager()
        inodeTable = manager.getTable("inode")
        treeTable = manager.getTable("tree")

        t_i, t_ns = self.__newctime_tuple()

        inode_id = inodeTable.insert(
            nlinks, mode, ctx.uid, ctx.gid, rdev, size,
            t_i, t_i, t_i,
            t_ns, t_ns, t_ns
        )
        name_id = self.__intern(name)

        par_node = self.__get_tree_node_by_inode(parent_inode)

        treeTable.insert(par_node["id"], name_id, inode_id)

        self.getLogger().debug("__insert->(inode=%i,parent_inode=%i)", inode_id, par_node["inode_id"])

        return inode_id, par_node["inode_id"]


    def __intern(self, string): # {{{3
        start_time = time.time()
        nameTable = self.getTable("name")
        result = nameTable.find(string)
        if not result:
            result = nameTable.insert(string)
        self.time_spent_interning += time.time() - start_time
        return int(result)


    def __remove(self, parent_inode, name, check_empty=False): # {{{3
        """
        @param  check_empty:    Check is this directory not empty
        @param  check_empty:    bool
        """

        cur_node = self.__get_tree_node_by_parent_inode_and_name(parent_inode, name)

        treeTable = self.getTable("tree")
        inodeTable = self.getTable("inode")

        # Make sure directories are empty before deleting them to avoid orphaned inodes.
        if check_empty:
            tree_inodes = treeTable.get_children_inodes(cur_node["id"])
            inodes = inodeTable.count_nlinks_by_ids(tree_inodes)
            if inodes:
                raise FUSEError(errno.ENOTEMPTY)
            else:
                inodeTable.dec_nlinks(cur_node["inode_id"])

        treeTable.delete(cur_node["id"])
        inodeTable.dec_nlinks(cur_node["inode_id"])

        # Inodes with nlinks = 0 are purged periodically from __collect_garbage() so
        # we don't have to do that here.
        if inodeTable.get_mode(cur_node["inode_id"]) & stat.S_IFDIR:
            par_node = self.__get_tree_node_by_inode(parent_inode)
            inodeTable.dec_nlinks(par_node["inode_id"])
            self.cached_attrs.unset(parent_inode)
            self.cached_nodes.unset(parent_inode)

        self.cached_attrs.unset(cur_node["inode_id"])
        self.cached_nodes.unset((parent_inode, name))
        self.cached_nodes.unset(cur_node["inode_id"])

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

        o = ctx.uid == attrs['uid'] or ctx.uid == 0 # access by same user id? Or ROOT
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
        return time.time()

    def __newctime_tuple(self): # {{{3
        return self.__get_time_tuple( self.__newctime() )

    def __get_time_tuple(self, t): # {{{3
        t_ns, t_i = math.modf(t)
        t_ns = int(t_ns * 10**9)
        return int(t_i), t_ns


    def __hash(self, data): # {{{3
        start_time = time.time()
        context = hashlib.new(self.hash_function)
        context.update(data)
        digest = context.digest()
        self.time_spent_hashing += time.time() - start_time
        return digest

    def __compress(self, data): # {{{3
        start_time = time.time()
        cdata, cmethod = self.getApplication().compressData(data)
        self.time_spent_compressing += time.time() - start_time
        return cdata, cmethod


    def __print_stats(self): # {{{3
        self.getLogger().info('-' * 79)
        self.__report_memory_usage()
        self.__report_memory_usage_real()
        self.__report_deduped_usage()
        self.__report_compressed_usage()
        self.__report_throughput()
        self.__report_timings()
        self.getLogger().info(' ' * 79)


    def __report_timings(self): # {{{3
        if self.getLogger().isEnabledFor(logging.INFO):
            timings = [
                (self.time_spent_caching_nodes, 'Caching tree nodes'),
                (self.time_spent_interning, 'Interning path components'),
                (self.time_spent_writing, 'Writing data stream'),
                (self.time_spent_writing_blocks, 'Writing data blocks (cumulative)'),
                (self.time_spent_writing_blocks - self.time_spent_compressing - self.time_spent_hashing, 'Writing blocks to database'),
                (self.time_spent_flushing_block_cache - self.time_spent_writing_blocks, 'Flushing block cache'),
                (self.time_spent_hashing, 'Hashing data blocks'),
                (self.time_spent_compressing, 'Compressing data blocks'),
                (self.time_spent_querying_tree, 'Querying the tree')
            ]
            maxdescwidth = max([len(l) for t, l in timings]) + 3
            timings.sort(reverse=True)

            uptime = time.time() - self.fs_mounted_at
            self.getLogger().info("Filesystem mounted: %s", format_timespan(uptime))

            printed_heading = False
            for timespan, description in timings:
                percentage = 100.0 * timespan / uptime
                if percentage >= 0.01:
                    if not printed_heading:
                        self.getLogger().info("Cumulative timings of slowest operations:")
                        printed_heading = True
                    self.getLogger().info(
                        " - %-*s%s (%.2f%%)" % (maxdescwidth, description + ':', format_timespan(timespan), percentage))

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
        if time.time() - self.gc_hook_last_run >= self.gc_interval:
            self.gc_hook_last_run = time.time()
            self.__collect_garbage()
            self.__print_stats()
            self.gc_hook_last_run = time.time()
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
        start_time = time.time()

        tableIndex = self.getTable("inode_hash_block")
        tableBlock = self.getTable("block")
        tableHash = self.getTable("hash")

        block.seek(0)
        data_block = block.getvalue()

        block_length = len(data_block)

        hash_value = self.__hash(data_block)
        hash_id = tableHash.find(hash_value)

        block_index = tableIndex.get_by_inode_number(inode, block_number)

        self.getLogger().debug("write block: inode=%s, block number = %s, data length = %s" % (inode, block_number, block_length))

        # It is new block now?
        if not hash_id:

            cdata, cmethod = self.__compress(data_block)

            hash_count = 0
            if block_index:
                hash_count = tableIndex.get_count_hash(block_index["hash_id"])

            cdata_length = len(cdata)

            self.getLogger().debug("-- compression: method=%s, new data length = %s" % (cmethod, cdata_length))

            if hash_count == 1:
                self.getLogger().debug("-- update one block data")

                hash_id = block_index["hash_id"]
                tableHash.update(hash_id, hash_value)
                tableBlock.update(hash_id, self.getCompressionTypeId(cmethod), cdata)
            else:
                hash_id = tableHash.insert(hash_value)
                tableBlock.insert(hash_id, self.getCompressionTypeId(cmethod), cdata)

            self.bytes_written += block_length
            self.bytes_written_compressed += cdata_length
        else:
            #hash_count = tableIndex.get_count_hash(hash_id)
            #
            #if block_index:
            #    old_block = self.getTable("block").get(hash_id)
            #    old_data = self.__decompress(old_block["data"], old_block["compression_type_id"]).getvalue()
            #    old_hash = self.__hash(old_data)
            #    if old_hash != hash_value:
            #        self.getLogger().error("-- weird hashed data collision detected! old=%s, writing=%s" % (
            #            old_hash,
            #            hash_value
            #        ))
            #
            #self.getLogger().debug("-- deduped by hash = %s, count in index db = %s" % (hash_id, hash_count,))

            # Old hash found
            self.bytes_deduped += block_length

        if not block_index:
            tableIndex.insert(inode, block_number, hash_id, block_length)
        elif block_index["hash_id"] != hash_id:
            tableIndex.update(inode, block_number, hash_id, block_length)

        self.time_spent_writing_blocks += time.time() - start_time
        return


    def __flush_old_cached_blocks(self, cached_blocks, writed=False):
        count = 0
        for inode, inode_data in cached_blocks.items():
            for block_number, block_data in inode_data.items():
                if block_data["w"]:
                    block = block_data.get("block")
                    self.__write_block_data(inode, block_number, block)
                    if writed:
                        count += 1
                else:
                    if not writed:
                        count += 1
        return count

    def __cache_block_hook(self): # {{{3

        if not self.cache_enabled:
            return

        start_time = time.time()
        flushed_writed_blocks = 0
        flushed_readed_blocks = 0

        if time.time() - self.cache_gc_block_write_last_run >= self.cache_block_write_timeout:
            # start_time = time.time()
            # self.getLogger().debug("Performing writed block cache cleanup (this might take a while) ..")

            # size = len(self.cached_blocks)
            flushed_writed_blocks += self.__flush_old_cached_blocks(self.cached_blocks.expired(True), True)
            # self.getLogger().debug(" flushed %i of %i blocks", count, size)

            self.cache_gc_block_write_last_run = time.time()

            # elapsed_time = time.time() - start_time
            # self.getLogger().debug("Finished writed block cache cleanup in %s.", format_timespan(elapsed_time))

        if self.cached_blocks.isWritedCacheFull():
            #start_time = time.time()
            #self.getLogger().debug("Performing writed block cache cleanup (this might take a while) ..")

            #size = len(self.cached_blocks)
            flushed_writed_blocks += self.__flush_old_cached_blocks(self.cached_blocks.expireByCount(True), True)
            #self.getLogger().debug(" flushed %i of %i blocks", count, size)

            #elapsed_time = time.time() - start_time
            #self.getLogger().debug("Finished writed block cache cleanup in %s.", format_timespan(elapsed_time))

        if time.time() - self.cache_gc_block_read_last_run >= self.cache_block_read_timeout:
            #start_time = time.time()
            #self.getLogger().debug("Performing readed block cache cleanup (this might take a while) ..")

            #size = len(self.cached_blocks)
            flushed_readed_blocks += self.cached_blocks.expired(False)
            #self.getLogger().debug(" flushed %i of %i blocks", count, size)

            self.cache_gc_block_read_last_run = time.time()

            #elapsed_time = time.time() - start_time
            #self.getLogger().debug("Finished readed block cache cleanup in %s.", format_timespan(elapsed_time))

        if self.cached_blocks.isReadCacheFull():
            #start_time = time.time()
            #self.getLogger().debug("Performing readed block cache cleanup (this might take a while) ..")

            #size = len(self.cached_blocks)
            flushed_readed_blocks += self.cached_blocks.expireByCount(False)
            #self.getLogger().debug(" flushed %i of %i blocks", count, size)

            #elapsed_time = time.time() - start_time
            #self.getLogger().debug("Finished writed block cache cleanup in %s.", format_timespan(elapsed_time))

        elapsed_time = time.time() - start_time

        self.time_spent_flushing_block_cache += elapsed_time

        if flushed_writed_blocks + flushed_readed_blocks > 0:
            self.getLogger().info("Block cache cleanup: flushed %i writed, %i readed blocks in %s",
                                  flushed_writed_blocks, flushed_readed_blocks, format_timespan(elapsed_time))

        return

    def __cache_meta_hook(self): # {{{3

        if not self.cache_enabled:
            return

        start_time = time.time()

        flushed_nodes = 0
        flushed_attrs = 0

        if time.time() - self.cache_gc_meta_last_run >= self.cache_meta_timeout:

            size = len(self.cached_nodes)
            self.cached_nodes.clear()
            flushed_nodes = size - len(self.cached_nodes)

            size = len(self.cached_attrs)
            self.cached_attrs.clear()
            flushed_attrs = size - len(self.cached_attrs)

            self.cache_gc_meta_last_run = time.time()

        elapsed_time = time.time() - start_time
        if flushed_attrs + flushed_nodes > 0:
            self.getLogger().info("Nodes cache cleanup: flushed %i nodes, %i attrs in %s.",
                                  flushed_nodes, flushed_attrs,
                                  format_timespan(elapsed_time))
        return

    def forced_vacuum(self): # {{{3
        if not self.isReadonly():
            start_time = time.time()
            self.getLogger().info("Performing data vacuum (this might take a while) ..")
            for table_name in self.getManager().tables:
                self.__vacuum_datatable(table_name)
            elapsed_time = time.time() - start_time
            self.getLogger().info("Finished data vacuum in %s.", format_timespan(elapsed_time))
        return

    def __collect_garbage(self): # {{{3
        if self.gc_enabled and not self.isReadonly():
            start_time = time.time()
            self.getLogger().info("Performing garbage collection (this might take a while) ..")
            self.should_vacuum = False
            for method in self.__collect_strings, \
                          self.__collect_inodes, \
                          self.__collect_inodes_all, \
                          self.__collect_xattrs, \
                          self.__collect_links, \
                          self.__collect_indexes, \
                          self.__collect_blocks:
                sub_start_time = time.time()
                msg = method()
                if msg:
                    elapsed_time = time.time() - sub_start_time
                    self.getLogger().info(msg, format_timespan(elapsed_time))
            elapsed_time = time.time() - start_time
            self.getLogger().info("Finished garbage collection in %s.", format_timespan(elapsed_time))
        return

    def __collect_strings(self): # {{{4
        curTree = self.getTable("tree").getCursor()
        curName = self.getTable("name").getCursor()
        curName2 = self.getTable("name").getCursor(True)

        self.getLogger().info("Clean unused path segments...")

        curName.execute("SELECT COUNT(id) as cnt FROM `name`")

        countNames = curName.fetchone()["cnt"]
        self.getLogger().info(" path segments: %d" % countNames)

        current = 0
        proc = ""

        curName.execute("SELECT id FROM `name`")

        if self.getOption("memory_limit"):
            maxCnt = 1000
        else:
            maxCnt = 10000

        self.getLogger().info(" rows per iteration: %d" % maxCnt)

        count = 0
        while True:

            nameIds = tuple("%s" % nameItem["id"] for nameItem in curName.fetchmany(maxCnt))
            if not nameIds:
                break
            current += len(nameIds)

            curTree.execute("SELECT name_id FROM `tree` WHERE name_id IN (%s)" % (",".join(nameIds),))
            treeNames = curTree.fetchall()
            treeNameIds = tuple("%s" % nameItem["name_id"] for nameItem in treeNames)

            to_delete = ()
            for name_id in nameIds:
                if name_id not in treeNameIds:
                    to_delete += (name_id,)

            if to_delete:
                curName2.execute("DELETE FROM `name` WHERE id IN (%s)" % (",".join(to_delete),))
                count += curName2.rowcount

            p = "%6.2f%%" % (100.0 * current / countNames)
            if p != proc:
                proc = p
                self.getLogger().info("%s (count=%d)", proc, count)

        if count > 0:
            self.getTable("name").commit()
            self.should_vacuum = True
            self.__vacuum_datatable("name")
            return "Cleaned up %i unused path segment%s in %%s." % (count, count != 1 and 's' or '')
        return


    def __collect_inodes(self): # {{{4
        self.getLogger().info("Clean unused inodes...")
        count = self.getTable("inode").remove_by_nlinks()
        self.getManager().commit()
        if count > 0:
            self.should_vacuum = True
            self.__vacuum_datatable("inode")
            return "Cleaned up %i unused inode%s in %%s." % (count, count != 1 and 's' or '')
        return

    def __collect_inodes_all(self): # {{{4
        curTree = self.getTable("tree").getCursor()
        curInode = self.getTable("inode").getCursor()
        curInode2 = self.getTable("inode").getCursor(True)

        self.getLogger().info("Clean unused inodes (all)...")

        curInode.execute("SELECT COUNT(id) as cnt FROM `inode`")

        countInodes = curInode.fetchone()["cnt"]
        self.getLogger().info(" inodes: %d" % countInodes)

        current = 0
        proc = ""

        curInode.execute("SELECT id FROM `inode`")

        if self.getOption("memory_limit"):
            maxCnt = 1000
        else:
            maxCnt = 10000

        self.getLogger().info(" rows per iteration: %d" % maxCnt)

        count = 0
        while True:

            inodeIds = tuple("%s" % inodeItem["id"] for inodeItem in curInode.fetchmany(maxCnt))
            if not inodeIds:
                break
            current += len(inodeIds)

            curTree.execute("SELECT inode_id FROM `tree` WHERE inode_id IN (%s)" % (",".join(inodeIds),))
            treeInodes = curTree.fetchall()
            treeInodeIds = tuple("%s" % inodeItem["inode_id"] for inodeItem in treeInodes)

            to_delete = ()
            for inode_id in inodeIds:
                if inode_id not in treeInodeIds:
                    to_delete += (inode_id,)

            if to_delete:
                curInode2.execute("DELETE FROM `inode` WHERE id IN (%s)" % (",".join(to_delete),))
                count += curInode2.rowcount

            p = "%6.2f%%" % (100.0 * current / countInodes)
            if p != proc:
                proc = p
                self.getLogger().info("%s (count=%d)", proc, count)

        if count > 0:
            self.getTable("inode").commit()
            self.should_vacuum = True
            self.__vacuum_datatable("inode")
            return "Cleaned up %i unused inode%s in %%s." % (count, count != 1 and 's' or '')
        return


    def __collect_xattrs(self): # {{{4
        curInode = self.getTable("inode").getCursor()
        curXattr = self.getTable("xattr").getCursor()
        curXattr2 = self.getTable("xattr").getCursor(True)

        self.getLogger().info("Clean unused xattrs...")

        curXattr.execute("SELECT COUNT(inode_id) as cnt FROM `xattr`")

        countXattrs = curXattr.fetchone()["cnt"]
        self.getLogger().info(" xattrs: %d" % countXattrs)

        current = 0
        proc = ""

        curXattr.execute("SELECT inode_id FROM `xattr`")

        if self.getOption("memory_limit"):
            maxCnt = 1000
        else:
            maxCnt = 10000

        self.getLogger().info(" rows per iteration: %d" % maxCnt)

        count = 0
        while True:

            inodeIds = tuple("%s" % xattrItem["inode_id"] for xattrItem in curXattr.fetchmany(maxCnt))
            if not inodeIds:
                break
            current += len(inodeIds)

            curInode.execute("SELECT id FROM `inode` WHERE id IN (%s)" % (",".join(inodeIds),))
            xattrInodes = curInode.fetchall()
            xattrInodeIds = tuple("%s" % inodeItem["id"] for inodeItem in xattrInodes)

            to_delete = ()
            for inode_id in inodeIds:
                if inode_id not in xattrInodeIds:
                    to_delete += (inode_id,)

            if to_delete:
                curXattr2.execute("DELETE FROM `xattr` WHERE inode_id IN (%s)" % (",".join(to_delete),))
                count += curXattr2.rowcount

            p = "%6.2f%%" % (100.0 * current / countXattrs)
            if p != proc:
                proc = p
                self.getLogger().info("%s (count=%d)", proc, count)

        if count > 0:
            self.getTable("xattr").commit()
            self.should_vacuum = True
            self.__vacuum_datatable("xattr")
            return "Cleaned up %i unused xattr%s in %%s." % (count, count != 1 and 's' or '')
        return

    def __collect_links(self): # {{{4
        curInode = self.getTable("inode").getCursor()
        curLink = self.getTable("link").getCursor()
        curLink2 = self.getTable("link").getCursor(True)

        self.getLogger().info("Clean unused links...")

        curLink.execute("SELECT COUNT(inode_id) as cnt FROM `link`")

        countLinks = curLink.fetchone()["cnt"]
        self.getLogger().info(" links: %d" % countLinks)

        current = 0
        proc = ""

        curLink.execute("SELECT inode_id FROM `link`")

        if self.getOption("memory_limit"):
            maxCnt = 1000
        else:
            maxCnt = 10000

        self.getLogger().info(" rows per iteration: %d" % maxCnt)

        count = 0
        while True:

            inodeIds = tuple("%s" % linkItem["inode_id"] for linkItem in curLink.fetchmany(maxCnt))
            if not inodeIds:
                break
            current += len(inodeIds)

            curInode.execute("SELECT id FROM `inode` WHERE id IN (%s)" % (",".join(inodeIds),))
            linkInodes = curInode.fetchall()
            linkInodeIds = tuple("%s" % inodeItem["id"] for inodeItem in linkInodes)

            to_delete = ()
            for inode_id in inodeIds:
                if inode_id not in linkInodeIds:
                    to_delete += (inode_id,)

            if to_delete:
                curLink2.execute("DELETE FROM `link` WHERE inode_id IN (%s)" % (",".join(to_delete),))
                count += curLink2.rowcount

            p = "%6.2f%%" % (100.0 * current / countLinks)
            if p != proc:
                proc = p
                self.getLogger().info("%s (count=%d)", proc, count)

        if count > 0:
            self.getTable("link").commit()
            self.should_vacuum = True
            self.__vacuum_datatable("link")
            return "Cleaned up %i unused link%s in %%s." % (count, count != 1 and 's' or '')
        return

    def __collect_indexes(self): # {{{4
        curInode = self.getTable("inode").getCursor()
        curIndex = self.getTable("inode_hash_block").getCursor()
        curIndex2 = self.getTable("inode_hash_block").getCursor(True)

        self.getLogger().info("Clean unused block indexes...")

        curIndex.execute("SELECT COUNT(inode_id) as cnt FROM (SELECT inode_id FROM `inode_hash_block` GROUP BY inode_id) as _")

        countInodes = curIndex.fetchone()["cnt"]
        self.getLogger().info(" block inodes: %d" % countInodes)

        current = 0
        proc = ""

        curIndex.execute("SELECT inode_id FROM `inode_hash_block` GROUP BY inode_id")

        if self.getOption("memory_limit"):
            maxCnt = 1000
        else:
            maxCnt = 10000

        self.getLogger().info(" rows per iteration: %d" % maxCnt)

        count = 0
        while True:

            inodeIds = tuple("%s" % indexItem["inode_id"] for indexItem in curIndex.fetchmany(maxCnt))
            if not inodeIds:
                break
            current += len(inodeIds)

            curInode.execute("SELECT id FROM `inode` WHERE id IN (%s)" % (",".join(inodeIds),))
            indexInodes = curInode.fetchall()
            indexInodeIds = tuple("%s" % inodeItem["id"] for inodeItem in indexInodes)

            to_delete = ()
            for inode_id in inodeIds:
                if inode_id not in indexInodeIds:
                    to_delete += (inode_id,)

            if to_delete:
                curIndex2.execute("DELETE FROM `inode_hash_block` WHERE inode_id IN (%s)" % (",".join(to_delete),))
                count += curIndex2.rowcount

            p = "%6.2f%%" % (100.0 * current / countInodes)
            if p != proc:
                proc = p
                self.getLogger().info("%s (count=%d)", proc, count)

        if count > 0:
            self.getTable("inode_hash_block").commit()
            self.should_vacuum = True
            self.__vacuum_datatable("inode_hash_block")
            return "Cleaned up %i unused index entr%s in %%s." % (count, count != 1 and 'ies' or 'y')
        return

    def __collect_blocks(self): # {{{4
        curHash = self.getTable("hash").getCursor()
        curHash2 = self.getTable("hash").getCursor(True)
        curBlock = self.getTable("block").getCursor()
        curIndex = self.getTable("inode_hash_block").getCursor()

        self.getLogger().info("Clean unused data blocks and hashes...")

        curHash.execute("SELECT COUNT(id) AS cnt FROM `hash`")

        countHashes = curHash.fetchone()["cnt"]
        self.getLogger().info(" hashes: %d" % countHashes)

        current = 0
        proc = ""

        curHash.execute("SELECT id FROM `hash`")

        if self.getOption("memory_limit"):
            maxCnt = 1000
        else:
            maxCnt = 10000

        self.getLogger().info(" rows per iteration: %d" % maxCnt)

        count = 0
        while True:

            hashIds = tuple("%s" % hashItem["id"] for hashItem in curHash.fetchmany(maxCnt))
            if not hashIds:
                break
            current += len(hashIds)

            curIndex.execute("SELECT hash_id FROM `inode_hash_block` WHERE hash_id IN (%s) GROUP BY hash_id" % (",".join(hashIds),))
            indexHashes = curIndex.fetchall()
            indexHashIds = tuple("%s" % hashItem["hash_id"] for hashItem in indexHashes)

            to_delete = ()
            for hash_id in hashIds:
                if hash_id not in indexHashIds:
                    to_delete += (hash_id,)

            if to_delete:
                curBlock.execute("DELETE FROM `block` WHERE hash_id IN (%s)" % (",".join(to_delete),))
                curHash2.execute("DELETE FROM `hash` WHERE id IN (%s)" % (",".join(to_delete),))
                count += curHash2.rowcount

            p = "%6.2f%%" % (100.0 * current / countHashes)
            if p != proc:
                proc = p
                self.getLogger().info("%s (count=%d)", proc, count)

        self.getManager().commit()

        if count > 0:
            self.should_vacuum = True
            self.getTable("hash").commit()
            self.__vacuum_datatable("hash")
            self.getTable("block").commit()
            self.__vacuum_datatable("block")
            return "Cleaned up %i unused data block%s and hashes in %%s." % (
                count, count != 1 and 's' or '',
            )
        return


    def __vacuum_datatable(self, tableName): # {{{4
        msg = ""
        sub_start_time = time.time()
        if self.should_vacuum and self.gc_vacuum_enabled:
            self.getLogger().info(" vacuum %s table" % tableName)
            self.getTable(tableName).vacuum()
            msg = "  vacuumed SQLite data store in %s."
        if msg:
            elapsed_time = time.time() - sub_start_time
            self.getLogger().info(msg, format_timespan(elapsed_time))
        return msg

    def __commit_changes(self, nested=False): # {{{3
        self.__update_mounted_subvolume_time()
        if self.use_transactions and not nested:
            self.getManager().commit()


    def __rollback_changes(self, nested=False): # {{{3
        if self.use_transactions and not nested:
            self.getLogger().info('Rolling back changes')
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
            # Convert the exception to a FUSE error code.
        if isinstance(exception, OSError):
            return FUSEError(exception.errno)
        else:
            return FUSEError(code)
