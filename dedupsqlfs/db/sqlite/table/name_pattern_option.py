# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.sqlite.table import Table

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
            ")"
        )
        return

    def insert( self, pattern, block_size, compression ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            "(`pattern`, `block_size`,`compression`) VALUES (?, ?, ?)",
            (pattern, block_size, compression)
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
            " SET `block_size`=?, `compression`=? WHERE `pattern`=?",
            (block_size, compression, pattern)
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
            " SET `compression`=? WHERE `pattern`=?",
            (compression, pattern)
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
            " SET `block_size`=? WHERE `pattern`=?",
            (block_size, pattern)
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
