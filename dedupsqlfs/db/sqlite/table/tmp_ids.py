# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.sqlite.table import Table

class TableTmpIds( Table ):

    _table_name = "tmp_ids"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "id INTEGER PRIMARY KEY"+
            ");"
        )
        return

    def insert( self, some_id):
        """
        :param values: dict | None
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("INSERT INTO `%s`(id) VALUES (?)" % self.getName(), (
            some_id,
        ))

        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def find( self, some_id):
        """
        :param inode: int
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT `id` FROM `%s` WHERE `id`=?" % self.getName(), (
            some_id,
        ))
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
            ret_ids = set(str(item["id"]) for item in iter(cur.fetchone,None))
        self.stopTimer('get_ids_by_ids')
        return ret_ids

    pass
