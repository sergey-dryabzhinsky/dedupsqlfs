# -*- coding: utf8 -*-

__author__ = 'sergey'

import pickle
from dedupsqlfs.db.mysql.table import Table

class TableTmpIds( Table ):

    _table_name = "tmp_ids"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "`id` BIGINT UNSIGNED PRIMARY KEY"+
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
            " (`id`) VALUES (%(id)s)",
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

    def get_ids_by_ids(self, ids):
        self.startTimer()
        ret_ids = ()
        id_str = ",".join(ids)
        if id_str:
            cur = self.getCursor()
            cur.execute("SELECT `id` FROM `%s` " % self.getName()+
                        " WHERE `id` IN (%s)" % (id_str,))
            ret_ids = tuple(str(item["id"]) for item in cur)
        self.stopTimer('get_ids_by_ids')
        return ret_ids

    pass
