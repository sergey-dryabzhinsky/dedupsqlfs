# -*- coding: utf8 -*-

__author__ = 'sergey'

from time import time
from dedupsqlfs.db.table import Table

class TableSubvolume( Table ):

    _table_name = "subvolume"

    def create( self ):
        self.startTimer()
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "node_id INTEGER PRIMARY KEY, "+
                "created_at INTEGER NOT NULL, "+
                "mounted_at INTEGER, "+
                "updated_at INTEGER"+
            ");"
        )
        self.stopTimer()
        return

    def insert( self, node_id, created_at, mounted_at=None, updated_at=None ):
        """
        :param node_id: int         - tree node
        :param name_id: int         - node name
        :param created_at: int      - creation time
        :param mounted_at: int|None - subvolume mounted
        :param updated_at: int|None - subvolume updated
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute("INSERT INTO `%s`(node_id, created_at, mounted_at, updated_at) " % self._table_name+
                    "VALUES (?, ?, ?, ?)", (node_id, created_at, mounted_at, updated_at))
        self.commit()
        self.stopTimer()
        return node_id

    def mount_time(self, node_id, mtime=None):
        self.startTimer()
        if mtime is None:
            mtime = int(time())
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET mounted_at=? WHERE node_id=? " % self._table_name,
                    (mtime, node_id,))
        self.commit()
        self.stopTimer()
        return cur.rowcount

    def update_time(self, node_id, utime=None):
        self.startTimer()
        if utime is None:
            utime = int(time())
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET updated_at=? WHERE node_id=? " % self._table_name,
                    (utime, node_id,))
        self.commit()
        self.stopTimer()
        return cur.rowcount

    def delete(self, node_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("DELETE FROM `%s` WHERE node_id=?" % self._table_name, (node_id,))
        item = cur.rowcount
        self.stopTimer()
        return item

    def get(self, node_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE node_id=?" % self._table_name, (node_id,))
        item = cur.fetchone()
        self.stopTimer()
        return item

    def fetch(self, limit=None, offset=None, order="created_at"):
        self.startTimer()
        cur = self.getCursor()

        query = "SELECT * FROM `%s`" % self._table_name
        if order:
            query += "ORDER BY `%s`" % order
        if limit is not None:
            query += " LIMIT %d" % limit
            if offset is not None:
                query += " OFFSET %d" % offset

        cur.execute(query)
        items = cur.fetchall()
        self.stopTimer()
        return items

    pass
