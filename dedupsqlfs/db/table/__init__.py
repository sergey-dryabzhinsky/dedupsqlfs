# -*- coding: utf8 -*-

__author__ = 'sergey'

import os

from dedupsqlfs.db import dict_factory

class Table( object ):

    _conn = None
    _curr = None

    _db_file_path = None
    _table_name = None
    _manager = None
    _autocommit = True

    def __init__(self, manager):
        if self._table_name is None:
            raise AttributeError("Define non-empty class variable '_table_name'")
        self._manager = manager
        pass

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

        conn = sqlite3.connect(db_path)

        conn.row_factory = dict_factory
        conn.text_factory = bytes

        conn.execute('PRAGMA locking_mode=EXCLUSIVE')
        if not self.getManager().getSynchronous():
            conn.execute("PRAGMA synchronous=OFF")

        conn.execute("PRAGMA temp_store=FILE")
        conn.execute("PRAGMA max_page_count=2147483646")
        conn.execute("PRAGMA page_size=1024")
        conn.execute("PRAGMA journal_mode=WAL")

        if not self.getManager().getAutocommit():
            conn.execute("PRAGMA read_uncommitted=ON")
            conn.isolation_level = "DEFERRED"
        else:
            conn.isolation_level = None

        self._conn = conn

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
        cur = self.getCursor()
        cur.execute("TRUNCATE `%s`" % self.getName())
        return self

    def create( self ):
        raise NotImplemented

    def commit(self):
        if not self.getManager().getAutocommit():
            self.getConnection().commit()
        return self

    def rollback(self):
        if not self.getManager().getAutocommit():
            self.getConnection().rollback()
        return self

    def vacuum(self):
        cur = self.getCursor()
        cur.execute("VACUUM")
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
