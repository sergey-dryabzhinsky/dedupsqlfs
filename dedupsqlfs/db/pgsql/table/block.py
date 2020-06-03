# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableBlock( Table ):

    _table_name = "block"

    _compressed = True
    _toku_compression = "fast"
    _key_block_size = 16

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "`hash_id` BIGINT UNSIGNED PRIMARY KEY, "+
                "`data` MEDIUMBLOB NOT NULL"+
            ")"+
            self._getCreationAppendString()
        )
        return

    def insert( self, hash_id, data):
        """
        :param data: bytes
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            " (`hash_id`, `data`) VALUES (%(hash_id)s, %(data)s)",
            {
                'hash_id': hash_id,
                'data': data,
            }
        )
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

        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `data`=%(data)s WHERE `hash_id`=%(hash_id)s",
            {
                'data': data,
                'hash_id': hash_id,
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
            "SELECT * FROM `%s` " % self.getName()+
            " WHERE `hash_id`=%(hash_id)s",
            {
                'hash_id': hash_id,
            }
        )
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
