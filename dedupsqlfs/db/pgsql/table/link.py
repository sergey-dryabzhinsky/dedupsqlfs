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
            ")"+
            self._getCreationAppendString()
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
                    " WHERE `inode_id`>=%s AND `inode_id`<%s", (start_id, end_id,))
        nameIds = set(str(item["inode_id"]) for item in cur)
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
