# -*- coding: utf8 -*-

__author__ = 'sergey'

import sqlite3
from dedupsqlfs.db.sqlite.table import Table

class TableBlock( Table ):

    # Use bigger block for data
    _page_size = 8192

    _table_name = "block"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "hash_id INTEGER PRIMARY KEY, "+
                "data BLOB NOT NULL"+
            ");"
        )
        return

    def insert( self, hash_id, data):
        """
        :param data: bytes
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        bdata = sqlite3.Binary(data)

        cur.execute("INSERT INTO `%s`(hash_id, data) VALUES (?,?)" % self._table_name,
                    (hash_id, bdata,))
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update( self, hash_id, data):
        """
        :param data: bytes
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        bdata = sqlite3.Binary(data)

        cur.execute("UPDATE `%s` SET data=? WHERE hash_id=?" % self._table_name,
                    (bdata, hash_id,))
        count = cur.rowcount
        self.stopTimer('update')
        return count

    def get( self, hash_id):
        """
        :param hash_id: int
        :return: Row
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE hash_id=?" % self._table_name, (hash_id,))
        item = cur.fetchone()
        self.stopTimer('get')
        return item

    def remove_by_ids(self, id_str):
        self.startTimer()
        count = 0
        if id_str:
            cur = self.getCursor()
            cur.execute("DELETE FROM `%s` " % self.getName()+
                        " WHERE `hash_id` IN (%s)" % (id_str,))
            count = cur.rowcount
        self.stopTimer('remove_by_ids')
        return count

    pass
