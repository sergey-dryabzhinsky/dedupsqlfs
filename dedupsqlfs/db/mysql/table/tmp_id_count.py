# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableTmpIdCount( Table ):

    _engine = "MEMORY"

    _table_name = "tmp_id_count"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "`id` BIGINT UNSIGNED PRIMARY KEY,"+
                "`cnt` BIGINT UNSIGNED"+
            ")"+
            self._getCreationAppendString()
        )
        return

    def insert( self, some_id):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            " (`id`,`cnt`) VALUES (%(id)s, 1)",
            {
                "id": some_id,
            }
        )

        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def find( self, some_id):
        """
        :param some_id: int
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT `id` FROM `%s` " % self.getName()+
            " WHERE `id`=%(id)s",
            {
                "id": some_id
            }
        )
        item = cur.fetchone()
        self.stopTimer('find')
        return item

    def inc( self, some_id):
        """
        :param some_id: int
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE FROM `%s` " % self.getName()+
            " SET `cnt`=`cnt`+1 WHERE `id`=%(id)s",
            {
                "id": some_id
            }
        )
        item = cur.fetchone()
        self.stopTimer('inc')
        return item

    pass
