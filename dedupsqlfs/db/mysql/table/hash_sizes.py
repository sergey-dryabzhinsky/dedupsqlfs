# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableHashSizes( Table ):

    _table_name = "hash_sizes"

    def create( self ):
        cur = self.getCursor()

        # Create table
        cur.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "`hash_id` BIGINT UNSIGNED PRIMARY KEY, "+
                "`compressed_size` INT UNSIGNED NOT NULL, "+
                "`real_size` INT UNSIGNED NOT NULL "+
            ")"+
            self._getCreationAppendString()
        )
        return

    def insert( self, hash_id, real_size, compressed_size):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            " (`hash_id`, `compressed_size`, `real_size`) VALUES (%(id)s, %(cs)s, %(rs)s)",
            {
                "id": hash_id,
                "cs": compressed_size,
                "rs": real_size
            }
        )
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update( self, hash_id, real_size, compressed_size):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute(
            "UPDATE `%s` " % self.getName() +
            " SET `compressed_size`=%(cs)s, `real_size`=%(rs)s WHERE `hash_id`=%(id)s",
            {
                "cs": compressed_size,
                "rs": real_size,
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
            "SELECT * FROM `%s` " % self.getName()+
            " WHERE `hash_id`=%(id)s",
            {
                "id": hash_id
            }
        )
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
