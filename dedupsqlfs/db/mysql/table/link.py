# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableLink( Table ):

    _table_name = "link"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "`inode_id` BIGINT UNSIGNED PRIMARY KEY, "+
                "`target` BLOB NOT NULL"+
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

        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            " (`inode_id`, `target`) VALUES (%(inode)s, %(target)s)",
            {
                "inode": inode,
                "target": target
            }
        )
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
        cur.execute(
            "SELECT `target` FROM `%s` " % self.getName()+
            " WHERE `inode_id`=%(inode)s",
            {
                "inode": inode
            }
        )
        item = cur.fetchone()
        if item:
            item = item["target"]
        self.stopTimer('find_by_inode')
        return item

    pass
