# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableBlock( Table ):

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

        cur.execute("INSERT INTO `%s`(hash_id, data) VALUES (%%s,%%s)" % self._table_name,
                    (hash_id, data,))
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

        cur.execute("UPDATE `%s` SET data=%%s WHERE hash_id=%%s" % self._table_name,
                    (data, hash_id,))
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
        cur.execute("SELECT * FROM `%s` WHERE hash_id=%%s" % self._table_name, (hash_id,))
        item = cur.fetchone()
        self.stopTimer('get')
        return item

    pass
