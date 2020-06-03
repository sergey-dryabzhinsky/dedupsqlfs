# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableInodeOption( Table ):

    _table_name = "inode_option"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "`inode` BIGINT UNSIGNED PRIMARY KEY, "+
                "`block_size` INT UNSIGNED NOT NULL, "+
                "`compression` TEXT NOT NULL"+
            ")" +
            self._getCreationAppendString()
        )
        return

    def insert( self, inode, block_size, compression ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            "(`inode`, `block_size`,`compression`) VALUES (%(inode)s, %(size)s, %(comp)s)",
            {
                "inode": inode,
                "size": block_size,
                "comp": compression
            }
        )
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update( self, inode, block_size, compression ):
        """
        @return: count updated rows
        @rtype: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `block_size`=%(size)s, `compression`=%(comp)s WHERE `inode`=%(inode)s",
            {
                "size": block_size,
                "comp": compression,
                "inode": inode
            }
        )
        count = cur.rowcount
        self.stopTimer('update')
        return count

    def update_compression( self, inode, compression ):
        """
        @return: count updated rows
        @rtype: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `compression`=%(comp)s WHERE `inode`=%(inode)s",
            {
                "comp": compression,
                "inode": inode
            }
        )
        count = cur.rowcount
        self.stopTimer('update_compression')
        return count

    def update_blockSize( self, inode, block_size ):
        """
        @return: count updated rows
        @rtype: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `block_size`=%(size)s WHERE `inode`=%(inode)s",
            {
                "size": block_size,
                "inode": inode
            }
        )
        count = cur.rowcount
        self.stopTimer('update_blockSize')
        return count

    def get( self, inode ):
        """
        @param inode: int
        @return: None or tuple
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s`" % self.getName() + " WHERE `inode`=%(inode)s", {"inode":inode})
        item = cur.fetchone()
        self.stopTimer('get')
        return item

    pass
