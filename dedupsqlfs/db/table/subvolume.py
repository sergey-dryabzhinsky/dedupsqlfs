# -*- coding: utf8 -*-

__author__ = 'sergey'

from time import time
from dedupsqlfs.db.table import Table

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
                "updated_at INTEGER)"+
            ");"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS subvol_created ON `%s` (" % self._table_name+
                "created_at"+
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
        cur = self.getCursor()
        cur.execute("INSERT INTO `%s`(node_id, created_at, mounted_at, updated_at) " % self._table_name+
                    "VALUES (?, ?, ?, ?)", (node_id, created_at, mounted_at, updated_at))
        self.commit()
        return node_id

    def mount_time(self, node_id):
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET mounted_at=? WHERE node_id=? " % self._table_name,
                    (int(time()), node_id,))
        self.commit()
        return self

    def update_time(self, node_id):
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET updated_at=? WHERE node_id=? " % self._table_name,
                    (int(time()), node_id,))
        self.commit()
        return self

    def delete(self, node_id):
        cur = self.getCursor()
        cur.execute("DELETE FROM `%s` WHERE node_id=?" % self._table_name, (node_id,))
        item = cur.rowcount
        return item

    def get(self, node_id):
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE node_id=?" % self._table_name, (node_id,))
        item = cur.fetchone()
        return item

    def fetch(self, limit=None, offset=None, order="created_at"):
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
        return items

    pass
