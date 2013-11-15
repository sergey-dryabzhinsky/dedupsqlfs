# -*- coding: utf8 -*-

__author__ = 'sergey'

from time import time
import os

# Too slow =(
# import inspect

from dedupsqlfs.db import dict_factory

class Table( object ):

    _conn = None
    _curr = None

    _db_file_path = None
    _table_name = None
    _manager = None
    _autocommit = True

    _last_time = None
    _time_spent = None
    _op_count = None

    def __init__(self, manager):
        if self._table_name is None:
            raise AttributeError("Define non-empty class variable '_table_name'")
        self._manager = manager
        self._time_spent = {}
        self._op_count = {}
        pass

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

    def getManager(self):
        return self._manager

    def getDbFilePath(self):
        if not self._db_file_path:
            self._db_file_path = os.path.join(
                self.getManager().getBasePath(),
                self.getManager().getDbName(),
                "%s.sqlite3" % self.getName()
            )
            self._db_file_path = os.path.abspath(self._db_file_path)
        return self._db_file_path

    def connect( self ):
        import sqlite3

        db_path = self.getDbFilePath()

        db_dir = os.path.dirname(db_path)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)

        isNew = False
        pageSize = 512
        if os.path.isfile(db_path):
            fileSize = os.path.getsize(db_path)
        else:
            isNew = True
            fileSize = 0
        filePageSize = fileSize / 2147483646.0 * 1.05
        while pageSize < filePageSize:
            pageSize *= 2

        cacheSize = 16*1024*1024 / pageSize

        conn = sqlite3.connect(db_path)

        conn.row_factory = dict_factory
        conn.text_factory = bytes

        conn.execute('PRAGMA locking_mode=EXCLUSIVE')
        if not self.getManager().getSynchronous():
            conn.execute("PRAGMA synchronous=OFF")

        conn.execute("PRAGMA temp_store=FILE")
        conn.execute("PRAGMA max_page_count=2147483646")
        conn.execute("PRAGMA page_size=%i" % pageSize)
        conn.execute("PRAGMA cache_size=%i" % cacheSize)
        conn.execute("PRAGMA journal_mode=WAL")

        if not self.getManager().getAutocommit():
            conn.execute("PRAGMA read_uncommitted=ON")
            conn.isolation_level = "EXCLUSIVE"
        else:
            conn.isolation_level = None

        self._conn = conn

        if isNew:
            self.create()
        return

    def getConnection(self):
        if self._conn is None:
            self.connect()
        return self._conn

    def getCursor(self, new=False):
        if new:
            return self.getConnection().cursor()
        if not self._curr:
            self._curr = self.getConnection().cursor()
        return self._curr

    def getPageSize(self):
        result = self.getConnection().execute('PRAGMA page_size').fetchone()
        # print("%s::getPageSize()=%r" % (self.getName(), result,))
        return result["page_size"]

    def getPageCount(self):
        result = self.getConnection().execute('PRAGMA page_count').fetchone()
        # print("%s::getPageCount()=%r" % (self.getName(), result,))
        return result["page_count"]

    def getFileSize(self):
        db_path = self.getDbFilePath()
        if os.path.isfile(db_path):
            return os.stat(db_path).st_size
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
            except:
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
        cur = self.getCursor()
        cur.execute("VACUUM")
        self.stopTimer("vacuum")
        return self

    def close(self):
        if self._curr:
            self._curr.close()
            self._curr = None
        if self._conn:
            self._conn.close()
            self._conn = None
        return self

    pass
