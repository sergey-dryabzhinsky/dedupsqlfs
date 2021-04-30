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
                "`value` VARBINARY(64) NOT NULL "+
            ")"+
            self._getCreationAppendString()
        )
        self.createIndexIfNotExists("value", ("value",), unique=True)
        return

    def insert( self, value):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "INSERT INTO `%s` " %self.getName()+
            " (`value`) VALUES (X%(value)s)",
            {
                'value': value.hex()
            }
        )
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def insertRaw( self, rowId, value):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "INSERT INTO `%s` " %self.getName()+
            " (`id`,`value`) VALUES (%(id)s, X%(value)s)",
            {
                'id': rowId,
                'value': value.hex()
            }
        )
        item = cur.lastrowid
        self.stopTimer('insertRaw')
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
            " SET `value`=X%(value)s WHERE `id`=%(id)s",
            {
                'value': value.hex(),
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
            " WHERE `id`=%(id)s",
            {
                'id': item_id
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
            "SELECT `id` FROM `%s` " % self.getName()+
            " WHERE `value`=X%(value)s",
            {
                'value': value.hex()
            }
        )
        item = cur.fetchone()
        if item:
            item = item["id"]
        self.stopTimer('find')
        return item

    def get_count(self):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT COUNT(1) as `cnt` FROM `%s`" % self.getName())
        item = cur.fetchone()
        if item:
            item = item["cnt"]
        else:
            item = 0
        self.stopTimer('get_count')
        return item

    def get_hash_ids(self, start_id, end_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT `id` FROM `%s` " % self.getName()+
                    " WHERE `id`>=%s AND `id`<%s", (start_id, end_id,))
        nameIds = set(item["id"] for item in cur)
        self.stopTimer('get_hash_ids')
        return nameIds

    def remove_by_ids(self, id_str):
        self.startTimer()
        count = 0
        if id_str:
            cur = self.getCursor()
            cur.execute("DELETE FROM `%s` " % self.getName()+
                        " WHERE `id` IN (%s)" % (id_str,))
            count = cur.rowcount
        self.stopTimer('remove_by_ids')
        return count

    pass
