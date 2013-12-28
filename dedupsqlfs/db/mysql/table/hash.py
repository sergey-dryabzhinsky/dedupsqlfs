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
                "`id` BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT, "+
                "`hash` TINYBLOB NOT NULL "+
            ")"
        )
        try:
            cur.execute(
                "ALTER TABLE `%s` " % self.getName()+
                " ADD UNIQUE INDEX `%s` " % (self.getName()+"_hash")+
                " (`hash`)"
            )
        except:
            pass
        return

    def insert( self, value):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "INSERT INTO `%s` " %self.getName()+
            " (`hash`) VALUES (%(value)s)",
            {
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
            "UPDATE `%s` " %self.getName()+
            " SET `hash`=%(value)s WHERE `id`=%(id)s",
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
            "SELECT `hash` FROM `%s` " % self.getName()+
            " WHERE `id`=%(id)s",
            {
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
            "SELECT `id` FROM `%s` " %self.getName()+
            " WHERE `hash`=%(value)s",
            {
                'value': value
            }
        )
        item = cur.fetchone()
        if item:
            item = item["id"]
        self.stopTimer('find')
        return item

    pass
