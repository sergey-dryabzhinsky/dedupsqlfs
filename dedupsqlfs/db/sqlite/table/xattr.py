# -*- coding: utf8 -*-

__author__ = 'sergey'

import sqlite3
import pickle
from dedupsqlfs.db.sqlite.table import Table

class TableInodeXattr( Table ):

    _table_name = "xattr"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "inode_id INTEGER PRIMARY KEY, "+
                "data BLOB NOT NULL"+
            ");"
        )
        return

    def insert( self, inode, values):
        """
        :param values: dict | None
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        if values:
            bvalues = sqlite3.Binary(pickle.dumps(values))
        else:
            bvalues = values
        cur.execute("INSERT INTO `%s`(inode_id, data) VALUES (?, ?)" % self._table_name, (
            inode, bvalues
        ))

        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update( self, inode, values):
        """
        :param target: bytes
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        bvalues = sqlite3.Binary(pickle.dumps(values))

        cur.execute("UPDATE `%s` SET `data`=? WHERE `inode_id`=?" % self._table_name, (
            bvalues, inode,
        ))
        item = cur.rowcount
        self.stopTimer('update')
        return item

    def find_by_inode( self, inode):
        """
        :param inode: int
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT `data` FROM `%s` WHERE `inode_id`=?" % self._table_name, (
            inode,
        ))
        item = cur.fetchone()
        if item:
            item = pickle.loads(item["data"])
        self.stopTimer('find_by_inode')
        return item

    def get_count(self):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT COUNT(`inode_id`) as `cnt` FROM `%s`" % self.getName())
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
        nameIds = tuple(str(item["inode_id"]) for item in cur.fetchall())
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
