# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableCompressionType( Table ):

    _table_name = "compression_type"

    def create( self ):
        cur = self.getCursor()

        # Create table
        cur.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT, "+
                "value VARCHAR(255) NOT NULL"+
            ")"
        )
        try:
            cur.execute(
                "ALTER TABLE %(table_name)s ADD UNIQUE INDEX %(index_name)s (`value`)",
                {
                    "table_name": self.getName(),
                    "index_name": self.getName() + "_value"
                }
            )
        except:
            pass
        return

    def insert( self, value ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "INSERT INTO %(table_name)s (value) VALUES (%(value)s)",
            {
                'table_name': self.getName(),
                'value': value,
            }
        )
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update( self, item_id, value ):
        """
        @return: count updated rows
        @rtype: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE %(table_name)s SET value=%(value)s WHERE id=%(id)s",
            {
                'table_name': self.getName(),
                'value': value,
                'id': item_id
            }
        )
        count = cur.rowcount
        self.stopTimer('update')
        return count

    def get( self, item_id ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT value FROM %(table_name)s WHERE id=%(id)s",
            {
                'table_name': self.getName(),
                'id': item_id,
            }
        )
        item = cur.fetchone()
        if item:
            item = item["value"].decode()
        self.stopTimer('get')
        return item

    def find( self, value ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT id FROM %(table_name)s WHERE value=%(value)s",
            {
                'table_name': self.getName(),
                'value': value,
            }
        )
        item = cur.fetchone()
        if item:
            item = item["id"]
        self.stopTimer('find')
        return item

    def getAll( self ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM %s", self.getName())
        items = cur.fetchall()
        opts = {}
        for item in items:
            opts[ item["id"] ] = item["value"].decode()
        self.stopTimer('getAll')
        return opts

    def getAllRevert( self ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM %s", self.getName())
        items = cur.fetchall()
        opts = {}
        for item in items:
            opts[ item["value"].decode() ] = item["id"]
        self.stopTimer('getAllRevert')
        return opts

    pass
