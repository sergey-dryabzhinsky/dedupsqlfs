# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.table import Table

class TableHashBlockSize( Table ):

    _table_name = "hash_block_size"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "hash_id INTEGER, "+
                "real_size INTEGER NOT NULL, "+
                "comp_size INTEGER NOT NULL, "+
                "UNIQUE(hash_id) " +
            ");"
        )
        return

    def insert( self, hash_id, real_size, comp_size):
        """
        :return: int
        """
        cur = self.getCursor()

        cur.execute("INSERT INTO `%s`(hash_id, real_size, comp_size) VALUES (?,?,?)" % self._table_name,
                    (hash_id, real_size, comp_size,))
        item = cur.lastrowid
        self.commit()
        return item

    def update( self, hash_id, real_size, comp_size):
        """
        :return: int
        """
        cur = self.getCursor()

        cur.execute("UPDATE `%s` SET real_size=?, comp_size=? WHERE hash_id=?" % self._table_name,
                    (real_size, comp_size, hash_id,))
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
