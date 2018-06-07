# -*- coding: utf8 -*-

__author__ = 'sergey'

from time import time
import sys
import pymysql
import pymysql.cursors
from dedupsqlfs.log import logging
from dedupsqlfs.my_formats import format_size

class Table( object ):

    _conn = None
    _curr = None

    # InnoDB, MyISAM, Aria, TokuDB
    _engine = "MyISAM"
    # Only InnoDB, TokuDB
    _compressed = True
    # Only for TokuDB: default, zlib, fast, quicklz, small, lzma, uncompressed
    _toku_compression = "small"
    # Only InnoDB
    _key_block_size = 8

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
        self._engine = manager.getTableEngine()
        pass

    def getLogger(self):
        if not self._log:
            self._log = self.getManager().getLogger()
        if not self._log:
            self._log = logging.getLogger(self.__class__.__name__)
            self._log.setLevel(logging.ERROR)
            self._log.addHandler(logging.StreamHandler(sys.stderr))
        return self._log

    def _getCreationAppendString(self):
        _cs = " Engine=" + self._engine
        if self._engine == "InnoDB":
            _cs += " KEY_BLOCK_SIZE=%d" % self._key_block_size
            if self._compressed:
                _cs += " ROW_FORMAT=COMPRESSED"
            _cs += ";"
        if self._engine == "TokuDB":
            if not self._compressed:
                _cs += " COMPRESSION=tokudb_uncompressed;"
            elif self._toku_compression:
                _cs += " COMPRESSION=tokudb_%s;" % self._toku_compression
        if self._engine == "Aria":
            _cs += " TRANSACTIONAL=0 PAGE_CHECKSUM=0 TABLE_CHECKSUM=0;"
        if self._engine == "MyISAM":
            _cs += " CHECKSUM=0;"
        return _cs

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
        self.incOperationsCount(op)
        self.incOperationsTimeSpent(op, self._last_time)

        self._last_time = None
        return self

    def getName(self):
        return self._table_name

    def setName(self, tableName):
        self._table_name = tableName
        return self

    def getManager(self):
        """
        @rtype L{dedupsqlfs.db.mysql.manager.DbManager}
        """
        return self._manager

    def connect( self ):
        return

    def getConnection(self):
        return self.getManager().getConnection()

    def getCursor(self, new=False):
        return self.getManager().getCursor(new)

    def getPageSize(self):
        return 0

    def getPageCount(self):
        return 0

    def getFileSize(self):
        return self.getSize()

    def getSize(self):
        cursor_type = pymysql.cursors.DictCursor
        conn = self.getManager().getConnection(True)
        cur = conn.cursor(cursor_type)
        cur.execute("SELECT SUM( `data_length` + `index_length` ) AS `size` FROM `information_schema`.`TABLES` WHERE `table_schema`=%s AND `table_name`=%s GROUP BY `table_name`", (self.getManager().getDbName(), self.getName()))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return int(row["size"])
        return 0

    def hasTable(self):
        cur = self.getCursor()

        cur.execute(
            "SELECT COUNT(1) AS `TableIsThere` "+
            "FROM `INFORMATION_SCHEMA`.`STATISTICS` "+
            "WHERE `table_schema` = %s "+
            "AND   `table_name`   = %s;",
            (self.getManager().getDbName(), self.getName())
        )
        row = cur.fetchone()

        exists = (row is not None) and int(row['TableIsThere']) > 0

        return exists

    def hasField(self, fname):
        cur = self.getCursor()

        cur.execute(
            "SELECT COUNT(1) AS `FieldIsThere` " +
            "FROM `INFORMATION_SCHEMA`.`COLUMNS` " +
            "WHERE `TABLE_SCHEMA` = %s " +
            "AND   `TABLE_NAME`   = %s " +
            "AND   `COLUMN_NAME`  = %s;",
            (self.getManager().getDbName(), self.getName(), fname,)
        )
        row = cur.fetchone()

        exists = (row is not None) and int(row['FieldIsThere']) > 0

        return exists

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
            "SELECT COUNT(1) AS `IndexIsThere` "+
            "FROM `INFORMATION_SCHEMA`.`STATISTICS` "+
            "WHERE `table_schema` = %s "+
            "AND   `table_name`   = %s "+
            "AND   `index_name`   = %s;",
            (self.getManager().getDbName(), self.getName(), self.getName()+"_" + indexName)
        )
        row = cur.fetchone()

        exists = (row is not None) and int(row['IndexIsThere']) > 0

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
                "ALTER TABLE `%s` " % self.getName()+
                "ADD "+_u+" INDEX `%s` " % (self.getName() + "_" + indexName)+
                "("+_f+")")

        return exists

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

        oldSize = self.getSize()

        cur = self.getCursor()
        cur.execute("OPTIMIZE TABLE `%s`" % self.getName())

        newSize = self.getSize()

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

        self.stopTimer("vacuum")
        return newSize - oldSize

    def close(self, nocompress=False):
        return self

    def drop(self):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("DROP TABLE `%s`" % self.getName())
        self.stopTimer("drop")
        return self

    pass
