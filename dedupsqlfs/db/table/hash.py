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
                "hash BLOB NOT NULL, "+
                "UNIQUE(hash) " +
            ")"
        )
        return

    def insert( self, value):
        self.startTimer()
        cur = self.getCursor()
        bvalue = sqlite3.Binary(value)
        cur.execute("INSERT INTO `%s`(hash) VALUES (?)" % self._table_name,
                    (bvalue,))
        item = cur.lastrowid
        self.commit()
        self.stopTimer('insert')
        return item

    def update( self, item_id, value ):
        """
        @return: count updated rows
        @rtype: int
        """
        self.startTimer()
        cur = self.getCursor()
        bvalue = sqlite3.Binary(value)
        cur.execute("UPDATE `%s` SET hash=? WHERE id=?" % self._table_name,
                    (bvalue, item_id))
        count = cur.rowcount
        self.commit()
        self.stopTimer('update')
        return count

    def get( self, item_id ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT hash FROM `%s` WHERE id=?" % self._table_name, (item_id,))
        item = cur.fetchone()
        if item:
            item = item["hash"]
        self.stopTimer('get')
        return item

    def find( self, value ):
        self.startTimer()
        cur = self.getCursor()
        bvalue = sqlite3.Binary(value)
        cur.execute("SELECT id FROM `%s` WHERE hash=?" % self._table_name, (bvalue,))
        item = cur.fetchone()
        if item:
            item = item["id"]
        self.stopTimer('find')
        return item

    pass
