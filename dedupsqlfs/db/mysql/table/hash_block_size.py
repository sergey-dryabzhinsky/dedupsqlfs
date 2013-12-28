# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableHashBlockSize( Table ):

    _table_name = "hash_block_size"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "hash_id BIGINT UNSIGNED PRIMARY KEY, "+
                "real_size INT UNSIGNED NOT NULL, "+
                "comp_size INT UNSIGNED NOT NULL "+
            ");"
        )
        return

    def insert( self, hash_id, real_size, comp_size):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute(
            "INSERT INTO %(table_name)s (hash_id, real_size, comp_size) "+
            "VALUES (%(hash_id)s, %(real_size)s, %(comp_size)s)",
            {
                "table_name": self.getName(),
                "hash_id": hash_id,
                "real_size": real_size,
                "comp_size": comp_size
            }
        )
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update( self, hash_id, real_size, comp_size):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute(
            "UPDATE %(table_name)s SET real_size=%(real_size)s, comp_size=%(comp_size)s WHERE hash_id=%(id)s",
            {
                "table_name": self.getName(),
                "real_size": real_size,
                "comp_size": comp_size,
                "id": hash_id
            }
        )
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
        cur.execute(
            "SELECT * FROM %(table_name)s WHERE hash_id=%(id)s",
            {
                "table_name": self.getName(),
                "id": hash_id
            }
        )
        item = cur.fetchone()
        self.stopTimer('get')
        return item

    pass
