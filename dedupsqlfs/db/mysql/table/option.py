# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableOption( Table ):

    _table_name = "option"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "`name` VARCHAR(64) NOT NULL PRIMARY KEY, "+
                "`value` TEXT NULL"+
            ")" +
            self._getCreationAppendString()
        )
        return

    def insert( self, name, value ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            "(`name`, `value`) VALUES (%(name)s, %(value)s)",
            {
                "name": name,
                "value": value
            }
        )
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update( self, name, value ):
        """
        @return: count updated rows
        @rtype: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `value`=%(value)s WHERE `name`=%(name)s",
            {
                "value": value,
                "name": name
            }
        )
        count = cur.rowcount
        self.stopTimer('update')
        return count

    def get( self, name ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT `value` FROM `%s` " % self.getName()+
            " WHERE `name`=%(name)s",
            {
                "name": name
            }
        )
        item = cur.fetchone()
        if item:
            item = item["value"]
        self.stopTimer('get')
        return item

    def getAll( self ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s`" % self._table_name)
        items = cur.fetchall()
        opts = {}
        for item in items:
            opts[ item["name"] ] = item["value"]
        self.stopTimer('getAll')
        return opts

    pass
