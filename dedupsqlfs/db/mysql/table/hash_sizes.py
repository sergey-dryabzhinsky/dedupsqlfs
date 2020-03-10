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
                "`writed_size` INT UNSIGNED NOT NULL, "+
                "`compressed_size` INT UNSIGNED NOT NULL "+
            ")"+
            self._getCreationAppendString()
        )
        return

    def insert( self, hash_id, writed_size, compressed_size):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            " (`hash_id`, `writed_size`, `compressed_size`) VALUES (%(id)s, %(ws)s, %(cs)s)",
            {
                "id": hash_id,
                "ws": writed_size,
                "cs": compressed_size
            }
        )
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update( self, hash_id, writed_size, compressed_size):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute(
            "UPDATE `%s` " % self.getName() +
            " SET `compressed_size`=%(cs)s, `writed_size`=%(ws)s WHERE `hash_id`=%(id)s",
            {
                "cs": compressed_size,
                "ws": writed_size,
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

    def get_sizes_by_hash_ids(self, id_str):
        self.startTimer()
        items = {}
        if id_str:
            cur = self.getCursor()
            cur.execute("SELECT * FROM `%s` " % self.getName()+
                        " WHERE `hash_id` IN (%s)" % (id_str,))
            for _i in cur:
                items[ _i["hash_id"] ] = (_i["writed_size"], _i["compressed_size"],)

        self.stopTimer('get_sizes_by_hash_ids')
        return items

    def get_median_compressed_size(self):
        self.startTimer()
        self.stopTimer('get_median_compressed_size')
        return 0

    def get_mean_compressed_size(self):
        self.startTimer()
        self.stopTimer('get_mean_compressed_size')
        return 0

    pass
