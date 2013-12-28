# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableHash( Table ):

    _table_name = "hash"

    def create( self ):
        cur = self.getCursor()

        # Create table
        cur.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "id BIGINT UNSIGNED PRIMARY KEY AUTOINCREMENT, "+
                "hash TINYBLOB NOT NULL "+
            ")"
        )
        try:
            cur.execute(
                "ALTER TABLE %(table_name)s ADD UNIQUE INDEX %(index_name)s (`hash`)",
                {
                    "table_name": self.getName(),
                    "index_name": self.getName() + "_hash"
                }
            )
        except:
            pass
        return

    def insert( self, value):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "INSERT INTO %(table_name)s (hash) VALUES (%(value)s)",
            {
                'table_name': self.getName(),
                'value': value
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
            "UPDATE %(table_name)s SET hash=%(value)s WHERE id=%(id)s",
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
            "SELECT hash FROM %(table_name)s WHERE id=%(id)s",
            {
                'table_name': self.getName(),
                'id': item_id
            }
        )
        item = cur.fetchone()
        if item:
            item = item["hash"]
        self.stopTimer('get')
        return item

    def find( self, value ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT id FROM %(table_name)s WHERE hash=%(value)s",
            {
                'table_name': self.getName(),
                'value': value
            }
        )
        item = cur.fetchone()
        if item:
            item = item["id"]
        self.stopTimer('find')
        return item

    pass
