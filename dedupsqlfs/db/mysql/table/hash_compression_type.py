# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableHashCompressionType( Table ):

    _table_name = "hash_compression_type"

    def create( self ):
        cur = self.getCursor()

        # Create table
        cur.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "`hash_id` BIGINT UNSIGNED PRIMARY KEY, "+
                "`type_id` SMALLINT UNSIGNED NOT NULL "+
            ")"+
            self._getCreationAppendString()
        )
        return

    def insert( self, hash_id, type_id):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            " (`hash_id`, `type_id`) VALUES (%(id)s, %(type)s)",
            {
                "id": hash_id,
                "type": type_id
            }
        )
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update( self, hash_id, type_id):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute(
            "UPDATE `%s` " % self.getName() +
            " SET `type_id`=%(type)s WHERE `hash_id`=%(id)s",
            {
                "type": type_id,
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

    def get_types_by_hash_ids(self, id_str):
        self.startTimer()
        items = {}
        if id_str:
            cur = self.getCursor()
            cur.execute("SELECT * FROM `%s` " % self.getName()+
                        " WHERE `hash_id` IN (%s)" % (id_str,))
            for _i in cur:
                items[ _i["hash_id"] ] = _i["type_id"]

        self.stopTimer('get_types_by_hash_ids')
        return items

    pass
