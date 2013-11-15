# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.table import Table

class TableCompressionType( Table ):

    _table_name = "compression_type"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "+
                "value TEXT NOT NULL, "+
                "UNIQUE(value) "
            ")"
        )
        return

    def insert( self, value ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("INSERT INTO `%s`(value) VALUES (?)" % self._table_name, (value,))
        item = cur.lastrowid
        self.commit()
        self.stopTimer('insert')
        return item

    def update( self, item_id, value ):
        """
        @return: count updated rows
        @rtype: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET value=? WHERE id=?" % self._table_name, (value, item_id))
        count = cur.rowcount
        self.commit()
        self.stopTimer('update')
        return count

    def get( self, item_id ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT value FROM `%s` WHERE id=?" % self._table_name, (item_id,))
        item = cur.fetchone()
        if item:
            item = item["value"].decode()
        self.stopTimer('get')
        return item

    def find( self, value ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT id FROM `%s` WHERE value=?" % self._table_name, (value,))
        item = cur.fetchone()
        if item:
            item = item["id"]
        self.stopTimer('find')
        return item

    def getAll( self ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s`" % self._table_name)
        items = cur.fetchall()
        opts = {}
        for item in items:
            opts[ item["id"] ] = item["value"].decode()
        self.stopTimer('getAll')
        return opts

    def getAllRevert( self ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s`" % self._table_name)
        items = cur.fetchall()
        opts = {}
        for item in items:
            opts[ item["value"].decode() ] = item["id"]
        self.stopTimer('getAllRevert')
        return opts

    pass
