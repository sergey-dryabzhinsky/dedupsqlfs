# -*- coding: utf8 -*-

# Imports. {{{1

import sys

# Try to load the required modules from Python's standard library.
try:
    import errno
    import os
    import stat
    import time
    import traceback
    import subprocess
except ImportError as e:
    msg = "Error: Failed to load one of the required Python modules! (%s)\n"
    sys.stderr.write(msg % str(e))
    sys.exit(1)

# Try to load bundled Python FUSE binding.
try:
    import _llfuse as _m
    from _m import module as llfuse
    from llfuse import FUSEError
except ImportError:
    sys.stderr.write("Error: Bundled The Python FUSE binding isn't compiled!\n" + \
        "If you're on Ubuntu try running `apt-get install libfuse-dev'\n"+
        " and `cd lid-dynload && python3 setup.py build_ext clean`.\n")
    # Try to load the Python FUSE binding.
    try:
        import llfuse as fuse
        from llfuse import FUSEError
    except ImportError:
        sys.stderr.write("Error: The Python FUSE binding isn't installed!\n" + \
            "If you're on Ubuntu try running `sudo -i apt-get install python3-pip'\n"+
            " and `sudo pip3 install llfuse`.\n")
        sys.exit(1)

fv = fuse.__version__.split('.')
if int(fv[0]) < 1 and int(fv[1]) < 42:
    sys.stderr.write(
        "Error: The Python FUSE binding v0.42+ isn't installed!\n" + \
        "If you're on Ubuntu try running `sudo -i apt-get install python3-pip'\n"+
        " and `sudo pip3 install llfuse==1.3.8`.\n")
    sys.exit(1)

# Local modules that are mostly useful for debugging.
from dedupsqlfs.my_formats import format_size
from dedupsqlfs.proc import pid_exists
from dedupsqlfs.fuse.helpers.logger import DDSFlogger
from dedupsqlfs.fuse.compress.mp import MultiProcCompressTool, BaseCompressTool
from dedupsqlfs.fuse.compress.mt import MultiThreadCompressTool
from dedupsqlfs.lib import constants
from subprocess import Popen

# Storage for options and DB interface
# Implements FUSE interface
class DedupFS(object): # {{{1

    def __init__(self, operations, mountpoint, options, *args, **kw):  # {{{2
        """
        @type operations: dedupsqlfs.fuse.operations.DedupOperations
        
        @ivar _cache_flusher_proc
        @type _cache_flusher_proc: Popen
        """

        self.options = dict(vars(options))

        self._readonly = False

        self._cache_flusher_proc = None

        self.mountpoint = mountpoint
        self.operations = operations
        self.operations.setApplication(self)

        # Initialize a Logger() object to handle logging.
        self.logger = DDSFlogger(application=self)

        kwargs = kw
        kwargs.update(self.parseFuseOptions(self.getOption("mountoption")))

        self._opts = []
        for key, value in kwargs.items():
            if value in (True, False,):
                self._opts.append(key)
                self.options[ key ] = True
            else:
                self._opts.append("%s=%s" % (key, value,))
                self.options[ key ] = value

        self.getLogger().debug("DedupFS options: %r", self.options)
        self.getLogger().debug("DedupFS mount options: %r", self._opts)
        self.getLogger().debug("DedupFS mountpoint: %r", self.mountpoint)

        if self.getOption("multi_cpu") == "process":
            self._compressTool = MultiProcCompressTool()
        elif self.getOption("multi_cpu") == "thread":
            self._compressTool = MultiThreadCompressTool()
        else:
            self._compressTool = BaseCompressTool()
        self._compressTool.setOption("cpu_limit", self.getOption("cpu_limit"))
        if self._compressTool.checkCpuLimit() <= 1:
            self._compressTool = BaseCompressTool()

        pass


    def startCacheFlusher(self):
        if not self.mountpoint:
            return
        if self.isReadonly():
            return
        if not self.getOption('use_cache_flusher'):
            return

        # Find real program path
        # Cache flusher may be not installed with "common" programs
        mf_path = sys.argv[0]
        while os.path.islink(mf_path):
            mf_path = os.path.abspath(os.readlink(mf_path))

        cf_path = os.path.join(os.path.dirname(mf_path), 'cache_flusher')
        if not os.path.isfile(cf_path):
            self.getLogger().warning("Can't find cache flushing helper! By path: %r", cf_path)
            return

        self._cache_flusher_proc = Popen(
            [ cf_path, self.mountpoint ],
            stderr=open(os.path.devnull, "w"),
            stdout=open(os.path.devnull, "w"),
        )
        return


    def stopCacheFlusher(self):
        if not self.mountpoint:
            return
        if self.isReadonly():
            return
        if not self._cache_flusher_proc:
            return
        if not self.getOption('use_cache_flusher'):
            return

        self._cache_flusher_proc.terminate()
        self._cache_flusher_proc.wait()
        return


    def checkIfLocked(self):
        lockFile = self.getOption('lock_file')
        if lockFile:
            if os.path.exists(lockFile):
                f = open(lockFile, 'r')
                lines = f.readlines()
                f.close()

                mypid = os.getpid()

                lockPid = 0
                try:
                    lockPid = int(lines[0].strip())
                except:
                    pass

                if not lockPid:
                    # Broken lock-file?
                    return False

                if not pid_exists(lockPid):
                    # Process is gone?
                    return False

                return mypid != lockPid

        return False


    def addLockMessage(self, message):
        lockFile = self.getOption('lock_file')
        if lockFile:
            try:
                f = open(lockFile, 'w')
                f.write("%s\n%s\n" % (os.getpid(), message,))
                f.flush()
                f.close()
            except:
                self.getLogger().warning("DedupFS::addLockMessage - can't write to %r", lockFile)
                pass


    def preInit(self):
        self.addLockMessage("pre-init")

        # Do migrations here, before fs.init callback
        self.operations.getManager()

        self._fixCompressionOptions()
        self._compressTool.init(self.getLogger())
        return


    def _checkNotUnmounted(self):
        self.addLockMessage("check-not-unmounted")

        # Do migrations here, before fs.init callback
        manager = self.operations.getManager()

        is_mounted = manager.getTable('option').get('mounted')
        self.getLogger().debug("DedupFS::preInit - FS flag mounted = %r", is_mounted)
        if is_mounted and int(is_mounted):
            self.getLogger().critical("Error: Seems like filesystem was not unmounted correctly! Run defragmentation!")

            self.postDestroy()

            sys.exit(1)
        return


    def postDestroy(self):
        lockFile = self.getOption('lock_file')
        if lockFile:
            try:
                if not self.checkIfLocked():
                    # remove only if locked by US
                    os.unlink(lockFile)
            except:
                self.getLogger().warning("DedupFS::postDestroy can't remove %r", lockFile)
                pass
        self._compressTool.stop()
        self.operations.getManager().close()
        return


    def onSignalUmount(self, signum=None, frame=None, error=False):
        if signum:
            # Real signal, try to umount FS
            self.getLogger().warning("Catch signal %r! Try to umount FS!", signum)
            try:
                ret = subprocess.Popen(["umount", self.mountpoint], stdout=open(os.devnull, "w+"), stderr=open(os.devnull, "w+")).wait()
                if ret == 0:
                    return
            except:
                pass

        # Or simulate umount on errors
        try:
            if error:
                self.getLogger().warning("Try to umount FS manual!")
                fuse.close(False)
            else:
                fuse.close(True)
        except:
            pass

        try:
            subprocess.Popen(["umount", "-l", self.mountpoint], stdout=open(os.devnull, "w+"), stderr=open(os.devnull, "w+")).wait()
        except:
            pass

        self.postDestroy()
        return


    def setupSignals(self):
        import signal

        signal.signal(signal.SIGINT, self.onSignalUmount)
        signal.signal(signal.SIGTERM, self.onSignalUmount)

        if hasattr(signal, "SIGABRT"):
            signal.signal(signal.SIGABRT, self.onSignalUmount)
        if hasattr(signal, "SIGPWR"):
            signal.signal(signal.SIGPWR, self.onSignalUmount)
        if hasattr(signal, "SIGQUIT"):
            signal.signal(signal.SIGQUIT, self.onSignalUmount)

        return

    def main(self):

        self.preInit()
        self._checkNotUnmounted()

        self.setupSignals()

        error = False
        ex = None
        try:
            fuse.init(self.operations, self.mountpoint, self._opts)
            fuse.main(workers=1, handle_signals=False)
        except Exception as e:
            error = True
            ex = e
            self.getLogger().error(traceback.format_exc())

        self.onSignalUmount(error=error)
        if ex:
            raise ex

    def parseFuseOptions(self, mountoptions):
        if not mountoptions:
            mountoptions = []
        args = {}
        for optstr in mountoptions:
            opts = optstr.split(",")
            for optstr in opts:
                opt = optstr.split("=")
                if len(opt) > 1:
                    args[opt[0]] = opt[1]
                else:
                    args[opt[0]] = True
        return args

    def getOption(self, key):
        return self.options.get(key)

    def setOption(self, key, value):
        self.options[key] = value
        return self

    def getLogger(self):
        return self.logger

    def setReadonly(self, flag=True):
        self._readonly = flag == True
        return self

    def isReadonly(self):
        return self._readonly

    def appendCompression(self, name):
        self._compressTool.appendCompression(name)
        return self

    def _fixCompressionOptions(self):

        defaultLevel = self.getOption("compression_level")

        methods = self.getOption("compression")
        self.getLogger().debug("_fixCompressionOptions - Compression methods = %r", methods)
        if methods and type(methods) in (tuple, list,):
            methods = list(methods)
            auto_type = None
            for i in range(len(methods)):
                method = methods[i]
                level = defaultLevel
                if method and method.find(":") != -1:
                    method, level = method.split(":")
                    methods[i] = method
                if method in (constants.COMPRESSION_TYPE_BEST, constants.COMPRESSION_TYPE_FAST, constants.COMPRESSION_TYPE_DEFAULT,):
                    auto_type = method
                    if method == constants.COMPRESSION_TYPE_BEST:
                        defaultLevel = constants.COMPRESSION_LEVEL_BEST
                    if method == constants.COMPRESSION_TYPE_FAST:
                        defaultLevel = constants.COMPRESSION_LEVEL_FAST
                    if method == constants.COMPRESSION_TYPE_DEFAULT:
                        defaultLevel = constants.COMPRESSION_LEVEL_DEFAULT
                    continue
                if method == constants.COMPRESSION_TYPE_NONE:
                    continue
                self.getLogger().debug("_fixCompressionOptions - Compression method %r set level %r",  method, level)
                self._compressTool.getCompressor(method).setCustomCompressionLevel(level)
            if auto_type:
                methods = [auto_type]
            self.getLogger().debug("_fixCompressionOptions - Compression methods after fix = %r", methods)
            self._compressTool.setOption("compression", methods)

        self._compressTool.setOption("compression_minimal_ratio", self.getOption("compression_minimal_ratio"))
        self._compressTool.setOption("compression_minimal_size", self.getOption("compression_minimal_size"))
        self._compressTool.setOption("compression_level", defaultLevel)
        self._compressTool.setOption("compression_forced", self.getOption("compression_forced"))

        return


    def getCompressTool(self):
        return self._compressTool

    def compressData(self, dataBlocks):
        return self._compressTool.compressData(dataBlocks)

    def decompressData(self, method, compressedBlock):
        return self._compressTool.decompressData(method, compressedBlock)

    def isDeprecated(self, method):
        return self._compressTool.isDeprecated(method)

    def isMethodSelected(self, method):
        return self._compressTool.isMethodSelected(method)

    def setDataDirectory(self, data_dir_path):
        self.operations.getManager().setBasepath(data_dir_path)
        return self

    def saveCompressionMethods(self, methods=None):
        if methods and type(methods) in (list, tuple, set,):
            manager = self.operations.getManager()
            table = manager.getTable("compression_type")
            for m in methods:

                if m and m.find(":") != -1:
                    m, level = m.split(":")

                m_id = table.find(m)
                if not m_id:
                    table.insert(m)
            manager.commit()
        return self

    # Miscellaneous methods: {{{2

    def report_disk_usage(self): # {{{3

        from dedupsqlfs.fuse.subvolume import Subvolume

        subv = Subvolume(self.operations)

        manager = self.operations.getManager()

        self.getLogger().info("--" * 39)

        tableSubvol = manager.getTable("subvolume", True)

        disk_usage = 0
        disk_usage += manager.getTable("hash", True).getFileSize()
        disk_usage += manager.getTable("hash_compression_type", True).getFileSize()
        disk_usage += manager.getTable("hash_sizes", True).getFileSize()
        disk_usage += manager.getTable("compression_type", True).getFileSize()
        disk_usage += manager.getTable("name", True).getFileSize()
        disk_usage += manager.getTable("name_pattern_option", True).getFileSize()
        disk_usage += manager.getTable("option", True).getFileSize()
        disk_usage += manager.getTable("subvolume", True).getFileSize()
        disk_usage += manager.getTable("block", True).getFileSize()

        apparentSize = 0
        dataSize = 0
        uniqueSize = 0
        compressedSize = 0
        compressedUniqueSize = 0
        compMethods = {}

        for subvol_id in tableSubvol.get_ids():

            subvol = tableSubvol.get(subvol_id)
            apparentSize += subv.get_apparent_size_fast(subvol["name"])

            disk_usage += manager.getTable("inode_%s" % subvol["id"], True).getFileSize()
            disk_usage += manager.getTable("inode_option_%s" % subvol["id"], True).getFileSize()
            disk_usage += manager.getTable("inode_hash_block_%s" % subvol["id"], True).getFileSize()
            disk_usage += manager.getTable("link_%s" % subvol["id"], True).getFileSize()
            disk_usage += manager.getTable("xattr_%s" % subvol["id"], True).getFileSize()
            disk_usage += manager.getTable("tree_%s" % subvol["id"], True).getFileSize()


        tableHCT = manager.getTable('hash_compression_type', True)
        tableHS = manager.getTable('hash_sizes', True)

        hashCount = subv.prepareIndexHashIdCount()

        hashIds = tuple(hashCount.keys())
        current = 0
        pageSize = 20000

        while True:

            items = hashIds[current:current+pageSize]
            if not len(items):
                break

            current += pageSize

            hash_ids = ",".join(set(str(item) for item in items))

            hashTypes = tableHCT.get_types_by_hash_ids(hash_ids)
            hashSizes = tableHS.get_sizes_by_hash_ids(hash_ids)

            for hash_id in items:

                hash_cnt = hashCount[ hash_id ]

                method = self.operations.getCompressionTypeName(hashTypes[ hash_id ])
                compMethods[ method ] = compMethods.get(method, 0) + 1

                hszItem = hashSizes[ hash_id ]

                uniqueSize += hszItem[0]
                compressedUniqueSize += hszItem[1]

                dataSize += hszItem[0]*hash_cnt
                compressedSize += hszItem[1]*hash_cnt

        sparseSize = apparentSize - dataSize
        dedupSize = dataSize - uniqueSize

        count_all = 0
        comp_types = {}

        for method, cnt in compMethods.items():
            count_all += cnt
            comp_types[ cnt ] = method

        if apparentSize:
            self.getLogger().info("Apparent size is %s, unique %s.",
                             format_size(apparentSize), format_size(uniqueSize)
            )

            self.getLogger().info("Deduped size is %s, ratio is %.2f%%.",
                             format_size(dedupSize),
                             100.0 * dedupSize / apparentSize)

            self.getLogger().info("Sparse size is %s, ratio is %.2f%%.",
                             format_size(sparseSize),
                             100.0 * sparseSize / apparentSize)

            self.getLogger().info("Databases take up %s, ratio is %.2f%%.",
                             format_size(disk_usage),
                             100.0 * disk_usage / uniqueSize)
            self.getLogger().info("Compressed data take up %s:\n- unique %s,\n- saved apparent space is %.2f%%,\n- use of database space: %.2f%%).",
                             format_size(compressedSize), format_size(compressedUniqueSize),
                             100.0 * (apparentSize - compressedSize) / apparentSize,
                             100.0 * compressedUniqueSize / disk_usage,
            )
            self.getLogger().info("Meta data and indexes take up %s:\n- ratio over apparent is %.2f%%,\n- use of database space: %.2f%%).",
                             format_size(disk_usage - compressedUniqueSize),
                             100.0 * (disk_usage - compressedUniqueSize) / uniqueSize,
                             100.0 * (disk_usage - compressedUniqueSize) / disk_usage,
            )

        else:
            self.getLogger().info("Apparent size is %s.",
                             format_size(apparentSize)
            )
            self.getLogger().info("Compressed size is %s.",
                             format_size(compressedSize)
            )
            self.getLogger().info("Databases take up %s.",
                             format_size(disk_usage)
            )

        if compressedSize:
            self.getLogger().info("--" * 39)
            self.getLogger().info("Compression by types:")

            keys = list(comp_types.keys())
            keys.sort(reverse=True)

            for key in keys:
                compression = comp_types[key]
                self.getLogger().info(" %8s used by %.2f%% blocks",
                    compression, 100.0 * int(key) / count_all
                )

        return

# vim: ts=4 sw=4 et
