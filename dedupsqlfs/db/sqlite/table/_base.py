# -*- coding: utf8 -*-

__author__ = 'sergey'

from time import time
import os
import sys
import subprocess
import platform

from dedupsqlfs.db.sqlite.row import dict_factory
from dedupsqlfs.log import logging
from dedupsqlfs.my_formats import format_size
from dedupsqlfs.lib import constants
from dedupsqlfs.fs import which

class Table( object ):

    _conn = None
    _curr = None

    _db_file_path = None
    _table_name = None
    _table_file_name = None
    _manager = None
    _autocommit = True

    _last_time = None
    _time_spent = None
    _op_count = None

    _log = None

    # default start page size for SQLite db file
    _page_size = 512

    _compressed = False

    _compressed_prog = None


    def __init__(self, manager):
        if self._table_name is None:
            raise AttributeError("Define non-empty class variable '_table_name'")
        self._manager = manager
        self._time_spent = {}
        self._op_count = {}
        pass

    def getLogger(self):
        if not self._log:
            self._log = self.getManager().getLogger()
        if not self._log:
            self._log = logging.getLogger(self.__class__.__name__)
            self._log.setLevel(logging.ERROR)
            self._log.addHandler(logging.StreamHandler(sys.stderr))
        return self._log

    def getOperationsCount(self):
        return self._op_count

    def getAllOperationsCount(self):
        s = 0
        for op, c in self._op_count.items():
            s += c
        return s

    def incOperationsCount(self, op):
        if not (op in self._op_count):
            self._op_count[ op ] = 0
        self._op_count[ op ] += 1
        return self

    def getTimeSpent(self):
        return self._time_spent

    def getAllTimeSpent(self):
        s = 0
        for op, t in self._time_spent.items():
            s += t
        return s

    def incOperationsTimeSpent(self, op, start_time):
        if not (op in self._time_spent):
            self._time_spent[ op ] = 0
        self._time_spent[ op ] += time() - start_time
        return self

    def startTimer(self):
        self._last_time = time()
        return self

    def stopTimer(self, op):
        # Too slow =(
        # caller = inspect.currentframe().f_back
        # op = inspect.getframeinfo(caller)[2]

        self.incOperationsCount(op)
        self.incOperationsTimeSpent(op, self._last_time)

        self._last_time = None
        return self

    def getName(self):
        return self._table_name

    def setName(self, tableName):
        self._table_name = tableName
        return self

    def getFileName(self):
        if self._table_file_name:
            return self._table_file_name
        return self._table_name

    def setFileName(self, tableFileName):
        self._table_file_name = tableFileName
        return self

    def getManager(self):
        return self._manager

    def getDbFilePath(self):
        if not self._db_file_path:
            self._db_file_path = os.path.join(
                self.getManager().getBasePath(),
                self.getManager().getDbName(),
                "%s.sqlite3" % self.getFileName()
            )
            self._db_file_path = os.path.abspath(self._db_file_path)
        return self._db_file_path

    def calcFilePageSize(self):
        db_path = self.getDbFilePath()

        pageSize = self._page_size
        if os.path.isfile(db_path):
            fileSize = os.path.getsize(db_path)
        else:
            fileSize = 0
        filePageSize = fileSize * 1.05 / 2 / 1000 / 1000 / 1000
        while pageSize < filePageSize:
            pageSize *= 2
            if pageSize >= 64 * 1024:
                break
        return pageSize


    def setCompressionProg(self, prog):
        if prog in (None, constants.COMPRESSION_PROGS_NONE,):
            self._compressed_prog = None
            return self
        if not prog in constants.COMPRESSION_PROGS:
            raise ValueError("Compression program %r nt supported!")
        self._compressed_prog = prog
        return self


    def _decompress(self):
        db_path = self.getDbFilePath()

        if os.path.exists(db_path):
            return False

        found = False
        ext_path = db_path
        for ext, progs in constants.COMPRESSION_PROGS_EXT.items():
            if os.path.exists(db_path + ext):
                ext_path = db_path + ext

                for prog in progs:
                    # No progs needed found?
                    if which(prog) == False:
                        continue

                    opts = constants.COMPRESSION_PROGS[prog]
                    if not opts["can-decomp"]:
                        continue

                    found = True

                    cmd = [prog,]
                    cmd.extend(opts["decomp"])
                    cmd.append(ext_path)
                    subprocess.Popen(cmd).wait()
                    self._compressed = True
                    self._compressed_prog = prog

                    break
            if found:
                break

        if not found and ext_path != db_path:
            raise RuntimeError("Can't decompress sqlite database: %r. No programs found!" % (ext_path,))

        return found

    def setCompressed(self, flag=True):
        self._compressed = flag
        return self

    def getCompressed(self):
        return self._compressed

    def _compress(self):
        db_path = self.getDbFilePath()

        if not os.path.exists(db_path):
            return False
        else:
            if self._compressed and self._compressed_prog not in (None, constants.COMPRESSION_PROGS_NONE,):
                opts = constants.COMPRESSION_PROGS[ self._compressed_prog ]
                cmd = [self._compressed_prog]
                cmd.extend(opts["comp"])
                cmd.append(db_path)
                subprocess.Popen(cmd).wait()
        return True

    def connect( self ):
        import sqlite3

        db_path = self.getDbFilePath()

        db_dir = os.path.dirname(db_path)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)

        self._decompress()

        pageSize = self.calcFilePageSize()

        cacheSize = 64*1024*1024 / pageSize

        conn = sqlite3.connect(db_path)

        conn.row_factory = dict_factory
        conn.text_factory = bytes

        # Set journal mode early as possible
        conn.execute("PRAGMA journal_mode=WAL")

        # Dirty hack again for pypy
        isPyPy = platform.python_implementation() == 'PyPy'
        if isPyPy:
            # Something wrong with sqlite3 module in pypy3
            # Works correctly only with autocommit
            self.getManager().setAutocommit(True)

        if not self.getManager().getAutocommit():
            conn.execute("PRAGMA read_uncommitted=ON")
            conn.isolation_level = "DEFERRED"
        else:
            conn.isolation_level = None

        # We don't expect many connections here
        conn.execute('PRAGMA locking_mode=EXCLUSIVE')
        if not self.getManager().getSynchronous():
            conn.execute("PRAGMA synchronous=OFF")
        else:
            conn.execute("PRAGMA synchronous=NORMAL")

        conn.execute("PRAGMA temp_store=FILE")
        conn.execute("PRAGMA max_page_count=2147483646")
        conn.execute("PRAGMA page_size=%i" % pageSize)
        conn.execute("PRAGMA cache_size=%i" % cacheSize)

        self._conn = conn
        return

    def getConnection(self):
        if self._conn is None:
            self.connect()
        return self._conn

    def getCursor(self, new=False):
        cur = self._curr
        if new:
            cur = self.getConnection().cursor()
            return cur
        if not self._curr:
            cur = self._curr = self.getConnection().cursor()
        return cur

    def getPageSize(self):
        result = self.getConnection().execute('PRAGMA page_size').fetchone()
        # print("%s::getPageSize()=%r" % (self.getName(), result,))
        return result["page_size"]

    def getPageCount(self):
        result = self.getConnection().execute('PRAGMA page_count').fetchone()
        # print("%s::getPageCount()=%r" % (self.getName(), result,))
        return result["page_count"]

    def hasTable(self):
        result = self.getConnection().execute("SELECT name FROM sqlite_master WHERE type = 'table';").fetchall()
        has = False
        for item in result:
            if item["name"].decode() == self.getName():
                has = True
                break
        return has

    def hasField(self, fname):
        """
        Table has field?
        :param fname: Field name
        :type  fname: str
        :return: bool
        """
        result = self.getConnection().execute("PRAGMA table_info('%s');" % self.getName()).fetchall()
        has = False
        for item in result:
            if item["name"].decode() == fname:
                has = True
                break
        return has

    def getFileSize(self):
        db_path = self.getDbFilePath()
        if os.path.isfile(db_path):
            return os.stat(db_path).st_size

        for prog, opts in constants.COMPRESSION_PROGS.items():
            if os.path.isfile(db_path + opts["ext"]):
                return os.stat(db_path + opts["ext"]).st_size
        return 0

    def getSize(self):
        return self.getPageSize() * self.getPageCount()

    def clean( self ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("TRUNCATE `%s`" % self.getName())
        self.stopTimer("clean")
        return self

    def create( self ):
        raise NotImplemented

    def begin( self ):
        if not self.getManager().getAutocommit():
            cur = self.getCursor()
            cur.execute("BEGIN")
        return self

    def commit(self):
        if not self.getManager().getAutocommit():
            self.startTimer()
            cur = self.getCursor()
            try:
                cur.execute("COMMIT")
            except Exception as e:
                self.getLogger().debug("EEE: Exception on commit? %s" % e)
                pass
            self.stopTimer("commit")
        return self

    def rollback(self):
        if not self.getManager().getAutocommit():
            self.startTimer()
            self.getConnection().rollback()
            self.stopTimer("rollback")
        return self

    def vacuum(self):
        self.startTimer()
        self.close(True)

        # VACUUM breaks on huge DB
        # Dump/Load DB
        # Warning! Need 2+x space!

        self._decompress()

        fn = self.getDbFilePath()
        if not os.path.isfile(fn):
            # Nothing to do
            return 0

        pageSize = self.calcFilePageSize()

        oldSize = os.path.getsize(fn)

        bkp_fn = fn + ".bkp"

        os.rename(fn, bkp_fn)

        p1 = subprocess.Popen(["sqlite3", bkp_fn, ".dump"], stdout=subprocess.PIPE)
        p2 = subprocess.Popen([
            "sqlite3",
            "-cmd",
            "PRAGMA page_size=%i; PRAGMA synchronous=OFF; PRAGMA max_page_count=2147483646;" % pageSize,
            fn
        ], stdin=p1.stdout, stdout=open(os.devnull,"w"))

        ret = p1.wait() > 0 or p2.wait() > 0
        if ret:
            if os.path.isfile(fn):
                os.unlink(fn)
            os.rename(bkp_fn, fn)
            self.getLogger().error("Can't dump sqlite3 db: %s" % self.getName())
            return self

        os.unlink(bkp_fn)

        newSize = os.path.getsize(fn)

        diffSign = ''
        if newSize > oldSize:
            diffSign = '+'
        elif newSize < oldSize:
            diffSign = '-'

        if diffSign:
            sz = format_size(abs(newSize - oldSize))
            self.getLogger().debug("DB table '%s' size change after vacuum: %s%.2f%% (%s%s)" % (
                self.getName(), diffSign, abs(newSize - oldSize) * 100.0 / oldSize, diffSign, sz,))
        else:
            self.getLogger().debug("DB table '%s' size not changed after vacuum." % self.getName())

        self._compress()

        self.stopTimer("vacuum")
        return newSize - oldSize

    def close(self, nocompress=False):
        if self._curr:
            self._curr.close()
            self._curr = None
        if self._conn:
            self._conn.close()
            self._conn = None
        if not nocompress:
            self._compress()
        return self

    def createIndexIfNotExists(self, indexName, fieldList, unique=False, indexSizes=None):
        """
        @param indexName: Index name
        @param fieldList: List of table fields for index
        @param unique: Is index unique?
        @param indexSizes: Sizes for CHAR/BINARY/BLOB/TEXT indexes. How many starting symbols to index

        @type indexName: str
        @type fieldList: list|tuple
        @type unique: bool
        @type indexSizes: dict
        """
        if not len(fieldList):
            raise ValueError("Define table field list for index!")

        cur = self.getCursor()

        cur.execute(
            "PRAGMA index_info(`%s`);" %
            (self.getName()+"_" + indexName,)
        )
        row = cur.fetchone()

        exists = (row is not None) and (len(row) > 0)

        if not exists:
            _u = ""
            if unique:
                _u = "UNIQUE"

            _f = ()

            if not indexSizes or type(indexSizes) is not dict:
                indexSizes = {}

            for field in fieldList:

                _isz = ""
                sz = indexSizes.get(field, 0)
                if sz > 0:
                    _isz = "(%d)" % sz

                _f += ("`%s`%s" % (field, _isz,),)

            _f = ",".join(_f)

            cur.execute(
                "CREATE "+_u+" INDEX `%s` " % (self.getName() + "_" + indexName)+
                " ON `%s` " % self.getName()+
                "("+_f+")")

        return exists

    def drop(self):
        self.startTimer()
        self.close()
        fn = self.getDbFilePath()
        if os.path.isfile(fn):
            os.unlink(fn)

        for prog, opts in constants.COMPRESSION_PROGS.items():
            if os.path.isfile(fn + opts["ext"]):
                os.unlink(fn + opts["ext"])

        self.stopTimer("drop")
        return self

    pass
