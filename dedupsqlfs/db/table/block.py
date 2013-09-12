# -*- coding: utf8 -*-

__author__ = 'sergey'

import sqlite3
from dedupsqlfs.db.table import Table

class TableBlock( Table ):

    _table_name = "block"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "hash_id INTEGER UNIQUE, "+
                "data BLOB NOT NULL"+
            ");"
        )
        return

    def insert( self, hash_id, data):
        """
        :param data: bytes
        :return: int
        """
        cur = self.getCursor()

        bdata = sqlite3.Binary(data)

        cur.execute("INSERT INTO `%s`(hash_id, data) VALUES (?,?)" % self._table_name,
                    (hash_id, bdata,))
        item = cur.lastrowid
        self.commit()
        return item

    def update( self, hash_id, data):
        """
        :param data: bytes
        :return: int
        """
        cur = self.getCursor()

        bdata = sqlite3.Binary(data)

        cur.execute("UPDATE `%s` SET data=? WHERE hash_id=?" % self._table_name,
                    (bdata, hash_id,))
        count = cur.rowcount
        self.commit()
        return count

    def get( self, hash_id):
        """
        :param hash_id: int
        :return: Row
        """
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE hash_id=?" % self._table_name, (hash_id,))
        item = cur.fetchone()
        return item

    pass
