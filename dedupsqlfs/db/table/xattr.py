# -*- coding: utf8 -*-

__author__ = 'sergey'

import sqlite3
import pickle
from dedupsqlfs.db.table import Table

class TableInodeXattr( Table ):

    _table_name = "xattr"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "inode_id INTEGER NOT NULL, "+
                "data BLOB NOT NULL, "+
                "UNIQUE(inode_id) " +
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
        self.commit()
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
        self.commit()
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

    pass
