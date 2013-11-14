# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.table import Table

class TableHashBlockSize( Table ):

    _table_name = "hash_block_size"

    def create( self ):
        self.startTimer()
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
        self.stopTimer()
        return

    def insert( self, hash_id, real_size, comp_size):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("INSERT INTO `%s`(hash_id, real_size, comp_size) VALUES (?,?,?)" % self._table_name,
                    (hash_id, real_size, comp_size,))
        item = cur.lastrowid
        self.commit()
        self.stopTimer()
        return item

    def update( self, hash_id, real_size, comp_size):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("UPDATE `%s` SET real_size=?, comp_size=? WHERE hash_id=?" % self._table_name,
                    (real_size, comp_size, hash_id,))
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

    pass
