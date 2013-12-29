# -*- coding: utf8 -*-

__author__ = 'sergey'

from time import time
import pymysql
import pymysql.cursors

class Table( object ):

    _conn = None
    _curr = None

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
        self.incOperationsCount(op)
        self.incOperationsTimeSpent(op, self._last_time)

        self._last_time = None
        return self

    def getName(self):
        return self._table_name

    def getManager(self):
        """
        @rtype L{dedupsqlfs.db.mysql.manager.DbManager}
        """
        return self._manager

    def connect( self ):
        cur = self.getCursor()
        cur.execute("SHOW TABLES LIKE '%s'" % self.getName())
        row = cur.fetchone()
        if not row:
            self.create()
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

    def createIndexIfNotExists(self, indexName, fieldList, unique=False):
        if not len(fieldList):
            raise ValueError("Define table field list for index!")

        cur = self.getCursor()

        cur.execute(
            "SELECT COUNT(1) INTO `IndexIsThere` "+
            "FROM `INFORMATION_SCHEMA`.`STATISTICS` "+
            "WHERE `table_schema` = %s "+
            "AND   `table_name`   = %s "+
            "AND   `index_name`   = %s;",
            (self.getManager().getDbName(), self.getName(), self.getName()+"_" + indexName)
        )
        row = cur.fetchone()

        exists = not row or not int(row["IndexIsThere"])

        if exists:
            _u = ""
            if unique:
                _u = "UNIQUE"

            _f = ()
            for field in fieldList:
                _f += "`%s`" % field

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
        cur.execute("OPTIMIZE")
        self.stopTimer("vacuum")
        return self

    def close(self):
        return self

    pass
