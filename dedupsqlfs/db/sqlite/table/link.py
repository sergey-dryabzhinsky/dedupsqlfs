# -*- coding: utf8 -*-

__author__ = 'sergey'

import sqlite3
from dedupsqlfs.db.sqlite.table import Table

class TableLink( Table ):

    _table_name = "link"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "inode_id INTEGER PRIMARY KEY, "+
                "target BLOB NOT NULL"+
            ");"
        )
        return

    def insert( self, inode, target):
        """
        :param target: bytes
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        btarget = sqlite3.Binary(target)

        cur.execute("INSERT INTO `%s`(inode_id, target) VALUES (?, ?)" % self._table_name, (
            inode, btarget,
        ))
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def find_by_inode( self, inode):
        """
        :param inode: int
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT target FROM `%s` WHERE inode_id=?" % self._table_name, (
            inode,
        ))
        item = cur.fetchone()
        if item:
            item = item["target"]
        self.stopTimer('find_by_inode')
        return item

    def get_count(self):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT COUNT(1) as `cnt` FROM `%s`" % self.getName())
        item = cur.fetchone()
        if item:
            item = item["cnt"]
        else:
            item = 0
        self.stopTimer('get_count')
        return item

    def get_inode_ids(self, start_id, end_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT `inode_id` FROM `%s` " % self.getName()+
                    " WHERE `inode_id`>=? AND `inode_id`<?", (start_id, end_id,))
        nameIds = tuple(str(item["inode_id"]) for item in iter(cur.fetchone, None))
        self.stopTimer('get_inode_ids')
        return nameIds

    def remove_by_ids(self, inode_ids):
        self.startTimer()
        count = 0
        id_str = ",".join(inode_ids)
        if id_str:
            cur = self.getCursor()
            cur.execute("DELETE FROM `%s` " % self.getName()+
                        " WHERE `inode_id` IN (%s)" % (id_str,))
            count = cur.rowcount
        self.stopTimer('remove_by_ids')
        return count

    pass
