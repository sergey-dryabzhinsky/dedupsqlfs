# -*- coding: utf8 -*-

__author__ = 'sergey'

import pickle
from dedupsqlfs.db.mysql.table import Table

class TableInodeXattr( Table ):

    _table_name = "xattr"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "`inode_id` BIGINT UNSIGNED PRIMARY KEY, "+
                "`data` BLOB NOT NULL"+
            ")"+
            self._getCreationAppendString()
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
            bvalues = pickle.dumps(values)
        else:
            bvalues = values

        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            " (`inode_id`, `data`) VALUES (%(inode)s, %(data)s)",
            {
                "inode": inode,
                "data": bvalues
            }
        )

        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update( self, inode, values):
        """
        :param target: bytes|None
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        if values:
            bvalues = pickle.dumps(values)
        else:
            bvalues = values

        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `data`=%(data)s WHERE `inode_id`=%(inode)s",
            {
                "date": bvalues,
                "inode": inode
            }
        )
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
        cur.execute(
            "SELECT `data` FROM `%s` " % self.getName()+
            " WHERE `inode_id`=%(inode)s",
            {
                "inode": inode
            }
        )
        item = cur.fetchone()
        if item:
            item = pickle.loads(item["data"])
        self.stopTimer('find_by_inode')
        return item

    pass
