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
                "name TEXT NOT NULL PRIMARY KEY, "+
                "value TEXT NULL"+
            ")"
        )
        return

    def insert( self, name, value ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "INSERT INTO %(table_name)s (name, value) VALUES (%(name)s, %(name)s)",
            {
                "table_name": self.getName(),
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
            "UPDATE %(table_name)s SET value=%(value)s WHERE name=%(name)s",
            {
                "table_name": self.getName(),
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
            "SELECT value FROM %(table_name)s WHERE name=%(name)s",
            {
                "table_name": self.getName(),
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
