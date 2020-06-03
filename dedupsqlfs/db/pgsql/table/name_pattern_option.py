# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableNamePatternOption( Table ):

    _table_name = "name_pattern_option"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "`pattern` VARBINARY(250) NOT NULL PRIMARY KEY, "+
                "`block_size` INT UNSIGNED NULL, "+
                "`compression` TEXT NULL"+
            ")" +
            self._getCreationAppendString()
        )
        return

    def insert( self, pattern, block_size, compression ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            "(`pattern`, `block_size`,`compression`) VALUES (%(pattern)s, %(size)s, %(comp)s)",
            {
                "pattern": pattern,
                "size": block_size,
                "comp": compression
            }
        )
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update( self, pattern, block_size, compression ):
        """
        @return: count updated rows
        @rtype: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `block_size`=%(size)s, `compression`=%(comp)s WHERE `pattern`=%(pattern)s",
            {
                "size": block_size,
                "comp": compression,
                "pattern": pattern
            }
        )
        count = cur.rowcount
        self.stopTimer('update')
        return count

    def update_compression( self, pattern, compression ):
        """
        @return: count updated rows
        @rtype: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `compression`=%(comp)s WHERE `pattern`=%(pattern)s",
            {
                "comp": compression,
                "pattern": pattern
            }
        )
        count = cur.rowcount
        self.stopTimer('update_compression')
        return count

    def update_blockSize( self, pattern, block_size ):
        """
        @return: count updated rows
        @rtype: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `block_size`=%(size)s WHERE `pattern`=%(pattern)s",
            {
                "size": block_size,
                "pattern": pattern
            }
        )
        count = cur.rowcount
        self.stopTimer('update_blockSize')
        return count

    def getAll( self ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s`" % self._table_name)
        items = cur.fetchall()
        opts = {}
        for item in items:
            opts[ item["pattern"] ] = {
                "block_size": item["block_size"],
                "compression": item["compression"]
            }
        self.stopTimer('getAll')
        return opts

    pass
