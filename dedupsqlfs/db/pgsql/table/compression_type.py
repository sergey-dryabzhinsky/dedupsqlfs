# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableCompressionType( Table ):

    _table_name = "compression_type"
    _key_block_size = 2

    def create( self ):
        cur = self.getCursor()

        # Create table
        cur.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "`id` SMALLINT UNSIGNED PRIMARY KEY AUTO_INCREMENT, "+
                "`value` CHAR(32) NOT NULL"+
            ")"+
            self._getCreationAppendString()
        )

        self.createIndexIfNotExists("value", ("value",), unique=True)
        return

    def insert( self, value ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            "(`value`) VALUES (%(value)s)",
            {
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
            "UPDATE `%s` " % self.getName()+
            "SET `value`=%(value)s WHERE `id`=%(id)s",
            {
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
            "SELECT `value` FROM `%s` " % self.getName()+
            "WHERE `id`=%(id)s",
            {
                'id': item_id,
            }
        )
        item = cur.fetchone()
        if item:
            item = item["value"]
        self.stopTimer('get')
        return item

    def find( self, value ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT `id` FROM `%s`" % self.getName() +
            " WHERE `value`=%(value)s",
            {
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
        cur.execute("SELECT * FROM `%s`" % self.getName())
        items = cur.fetchall()
        opts = {}
        for item in items:
            opts[ item["id"] ] = item["value"]
        self.stopTimer('getAll')
        return opts

    def getAllRevert( self ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s`" % self.getName())
        items = cur.fetchall()
        opts = {}
        for item in items:
            opts[ item["value"] ] = item["id"]
        self.stopTimer('getAllRevert')
        return opts

    pass
