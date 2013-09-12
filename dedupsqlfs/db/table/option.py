# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.table import Table

class TableOption( Table ):

    _table_name = "option"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "name TEXT NOT NULL PRIMARY KEY, "+
                "value TEXT NULL"+
            ")"
        )
        return

    def insert( self, name, value ):
        cur = self.getCursor()
        cur.execute("INSERT INTO `%s`(name, value) VALUES (?, ?)" % self._table_name, (name, value))
        item = cur.lastrowid
        self.commit()
        return item

    def update( self, name, value ):
        """
        @return: count updated rows
        @rtype: int
        """
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET value=? WHERE name=?" % self._table_name, (value, name))
        count = cur.rowcount
        self.commit()
        return count

    def get( self, name ):
        cur = self.getCursor()
        cur.execute("SELECT value FROM `%s` WHERE name=:name" % self._table_name,
                {"name": name}
        )
        item = cur.fetchone()
        if item:
            item = item["value"].decode()
        return item

    def getAll( self ):
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s`" % self._table_name)
        items = cur.fetchall()
        opts = {}
        for item in items:
            opts[ item["name"].decode() ] = item["value"].decode()
        return opts

    pass
