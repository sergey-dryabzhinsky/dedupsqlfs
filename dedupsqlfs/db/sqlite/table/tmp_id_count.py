# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.sqlite.table import Table

class TableTmpIdCount( Table ):

    _table_name = "tmp_id_count"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "id INTEGER PRIMARY KEY,"+
                "cnt INTEGER"+
            ");"
        )
        return

    def insert( self, some_id):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("INSERT INTO `%s`(id, cnt) VALUES (?,1)" % self.getName(), (
            some_id,
        ))

        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def insertCnt( self, some_id, cnt):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("INSERT INTO `%s`(id, cnt) VALUES (?,?)" % self.getName(), (
            some_id, cnt
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

    def inc( self, some_id):
        """
        :param inode: int
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET `cnt`=`cnt`+1 WHERE `id`=?" % self.getName(), (
            some_id,
        ))
        item = cur.fetchone()
        self.stopTimer('inc')
        return item

    pass
