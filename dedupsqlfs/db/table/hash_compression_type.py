# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.table import Table

class TableHashCompressionType( Table ):

    _table_name = "hash_compression_type"

    def create( self ):
        self.startTimer()
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "hash_id INTEGER, "+
                "compression_type_id INTEGER NOT NULL, "+
                "UNIQUE(hash_id) "+
            ");"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS hct_compression_type ON `%s` (" % self._table_name+
                "compression_type_id"+
            ");"
        )
        self.stopTimer()
        return

    def insert( self, hash_id, compression_type_id):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("INSERT INTO `%s`(hash_id, compression_type_id) VALUES (?,?)" % self._table_name,
                    (hash_id, compression_type_id,))
        item = cur.lastrowid
        self.commit()
        self.stopTimer()
        return item

    def update( self, hash_id, compression_type_id):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("UPDATE `%s` SET compression_type_id=? WHERE hash_id=?" % self._table_name,
                    (compression_type_id, hash_id,))
        count = cur.rowcount
        self.commit()
        self.stopTimer()
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
        self.stopTimer()
        return item

    def count_compression_type( self ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT COUNT(compression_type_id) AS cnt,compression_type_id FROM `%s` GROUP BY compression_type_id" % self._table_name)
        items = cur.fetchall()
        self.stopTimer()
        return items

    pass
