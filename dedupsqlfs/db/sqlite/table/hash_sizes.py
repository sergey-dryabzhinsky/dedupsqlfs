# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.sqlite.table import Table

class TableHashSizes( Table ):

    _table_name = "hash_sizes"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "hash_id INTEGER PRIMARY KEY, "+
                "`compressed_size` INTEGER NOT NULL, "+
                "`real_size` INTEGER NOT NULL "+
            ");"
        )
        return

    def insert( self, hash_id, real_size, compressed_size):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("INSERT INTO `%s`(hash_id, `compressed_size`, `real_size`) VALUES (?,?,?)" % self._table_name,
                    (hash_id, compressed_size, real_size,))
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update( self, hash_id, real_size, compressed_size):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("UPDATE `%s` SET compressed_size=?, real_size=? WHERE hash_id=?" % self._table_name,
                    (compressed_size, real_size, hash_id,))
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

    def remove_by_ids(self, hash_ids):
        self.startTimer()
        count = 0
        id_str = ",".join(hash_ids)
        if id_str:
            cur = self.getCursor()
            cur.execute("DELETE FROM `%s` " % self.getName()+
                        " WHERE `hash_id` IN (%s)" % (id_str,))
            count = cur.rowcount
        self.stopTimer('remove_by_ids')
        return count

    pass
