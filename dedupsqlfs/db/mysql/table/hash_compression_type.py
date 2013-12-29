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
                "`type_id` INT UNSIGNED NOT NULL "+
            ");"
        )

        self.createIndexIfNotExists("type", ("type_id",))
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

    def count_compression_type( self ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT COUNT(`type_id`) AS `cnt`, `type_id` FROM `%s` GROUP BY `type_id`" % self.getName()
        )
        items = cur.fetchall()
        self.stopTimer('count_compression_type')
        return items

    pass
