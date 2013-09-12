# -*- coding: utf8 -*-

__author__ = 'sergey'

import sqlite3
from dedupsqlfs.db.table import Table

class TableHash( Table ):

    _table_name = "hash"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "+
                "compression_type_id INTEGER NOT NULL, "+
                "hash BLOB NOT NULL UNIQUE"+
            ")"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS b_compression_type ON `%s` (" % self._table_name+
                "compression_type_id"+
            ");"
        )
        return

    def insert( self, compression_type_id, value):
        cur = self.getCursor()
        bvalue = sqlite3.Binary(value)
        cur.execute("INSERT INTO `%s`(compression_type_id, hash) VALUES (?,?)" % self._table_name,
                    (compression_type_id, bvalue,))
        item = cur.lastrowid
        self.commit()
        return item

    def update( self, item_id, compression_type_id, value ):
        """
        @return: count updated rows
        @rtype: int
        """
        cur = self.getCursor()
        bvalue = sqlite3.Binary(value)
        cur.execute("UPDATE `%s` SET compression_type_id=?, hash=? WHERE id=?" % self._table_name,
                    (compression_type_id, bvalue, item_id))
        count = cur.rowcount
        self.commit()
        return count

    def get( self, item_id ):
        cur = self.getCursor()
        cur.execute("SELECT hash FROM `%s` WHERE id=?" % self._table_name, (item_id,))
        item = cur.fetchone()
        if item:
            item = item["hash"]
        return item

    def fetch( self, item_id ):
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE id=?" % self._table_name, (item_id,))
        item = cur.fetchone()
        return item

    def find( self, value ):
        cur = self.getCursor()
        bvalue = sqlite3.Binary(value)
        cur.execute("SELECT id FROM `%s` WHERE hash=?" % self._table_name, (bvalue,))
        item = cur.fetchone()
        if item:
            item = item["id"]
        return item

    def count_compression_type( self ):
        cur = self.getCursor()
        cur.execute("SELECT COUNT(compression_type_id) AS cnt,compression_type_id FROM `%s` GROUP BY compression_type_id" % self._table_name)
        items = cur.fetchall()
        return items

    pass
