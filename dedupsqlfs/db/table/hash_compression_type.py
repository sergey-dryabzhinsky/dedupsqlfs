# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.table import Table

class TableHashCompressionType( Table ):

    _table_name = "hash_compression_type"

    def create( self ):
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
            "CREATE INDEX IF NOT EXISTS hash_compression_type ON `%s` (" % self._table_name+
                "compression_type_id"+
            ");"
        )
        return

    def insert( self, hash_id, compression_type_id):
        """
        :return: int
        """
        cur = self.getCursor()

        cur.execute("INSERT INTO `%s`(hash_id, compression_type_id) VALUES (?,?)" % self._table_name,
                    (hash_id, compression_type_id,))
        item = cur.lastrowid
        self.commit()
        return item

    def update( self, hash_id, compression_type_id):
        """
        :return: int
        """
        cur = self.getCursor()

        cur.execute("UPDATE `%s` SET compression_type_id=? WHERE hash_id=?" % self._table_name,
                    (compression_type_id, hash_id,))
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

    def count_compression_type( self ):
        cur = self.getCursor()
        cur.execute("SELECT COUNT(compression_type_id) AS cnt,compression_type_id FROM `%s` GROUP BY compression_type_id" % self._table_name)
        items = cur.fetchall()
        return items

    pass
