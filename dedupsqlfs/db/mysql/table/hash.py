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
                "`hash` VARBINARY(128) NOT NULL "+
            ")"+
            self._getCreationAppendString()
        )

        self.createIndexIfNotExists("hash", ("hash",), unique=True)
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

    def remove_by_ids(self, hash_ids):
        self.startTimer()
        count = 0
        id_str = ",".join(hash_ids)
        if id_str:
            cur = self.getCursor()
            cur.execute("DELETE FROM `%s` " % self.getName()+
                        " WHERE `id` IN (%s)" % (id_str,))
            count = cur.rowcount
        self.stopTimer('remove_by_ids')
        return count

    pass
