# -*- coding: utf8 -*-

__author__ = 'sergey'

from time import time
from dedupsqlfs.db.sqlite.table import Table

class TableSubvolume( Table ):

    _table_name = "subvolume"

    def create( self ):
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
        self.stopTimer('insert')
        return node_id

    def mount_time(self, node_id, mtime=None):
        self.startTimer()
        if mtime is None:
            mtime = int(time())
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET mounted_at=? WHERE node_id=? " % self._table_name,
                    (mtime, node_id,))
        self.stopTimer('mount_time')
        return cur.rowcount

    def update_time(self, node_id, utime=None):
        self.startTimer()
        if utime is None:
            utime = int(time())
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET updated_at=? WHERE node_id=? " % self._table_name,
                    (utime, node_id,))
        self.stopTimer('update_time')
        return cur.rowcount

    def delete(self, node_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("DELETE FROM `%s` WHERE node_id=?" % self._table_name, (node_id,))
        item = cur.rowcount
        self.stopTimer('delete')
        return item

    def get(self, node_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE node_id=?" % self._table_name, (node_id,))
        item = cur.fetchone()
        self.stopTimer('get')
        return item

    pass
