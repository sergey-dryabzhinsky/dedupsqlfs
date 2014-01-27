# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.sqlite.table import Table

class TableHashCompressionType( Table ):

    _table_name = "hash_compression_type"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "hash_id INTEGER PRIMARY KEY, "+
                "type_id INTEGER NOT NULL "+
            ");"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS hct_type ON `%s` (" % self._table_name+
                "type_id"+
            ");"
        )
        return

    def insert( self, hash_id, type_id):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("INSERT INTO `%s`(hash_id, type_id) VALUES (?,?)" % self._table_name,
                    (hash_id, type_id,))
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update( self, hash_id, type_id):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("UPDATE `%s` SET type_id=? WHERE hash_id=?" % self._table_name,
                    (type_id, hash_id,))
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

    def count_compression_type( self ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT COUNT(type_id) AS cnt, type_id FROM `%s` GROUP BY type_id" % self._table_name)
        items = cur.fetchall()
        self.stopTimer('count_compression_type')
        return items

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
