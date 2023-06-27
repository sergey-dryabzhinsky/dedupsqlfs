# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableHashCount( Table ):

    _table_name = "hash_count"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "`hash_id` BIGINT UNSIGNED PRIMARY KEY,"+
                "`cnt` BIGINT UNSIGNED"+
            ")"+
            self._getCreationAppendString()
        )
        return

    def insert( self, hash_id):
        """
        :param hash_id: int
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            " (`hash_id`,`cnt`) VALUES (%(id)s, 1)",
            {
                "id": hash_id,
            }
        )

        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def find( self, hash_id):
        """
        :param some_id: int
        :return: dict|null
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT `hash_id` FROM `%s` " % self.getName()+
            " WHERE `hash_id`=%(id)s",
            {
                "id": some_id
            }
        )
        item = cur.fetchone()
        self.stopTimer('find')
        return item

    def inc( self, hash_id):
        """
        Increment counter

        :param hash_id: int
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE FROM `%s` " % self.getName()+
            " SET `cnt`=`cnt`+1 WHERE `hash_id`=%(id)s",
            {
                "id": hash_id
            }
        )
        item = cur.fetchone()
        self.stopTimer('inc')
        return item

    def dec( self, hash_id):
        """
        Decrement counter

        :param hash_id: int
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE FROM `%s` " % self.getName()+
            " SET `cnt`=`cnt`-1 WHERE `hash_id`=%(id)s",
            {
                "id": hash_id
            }
        )
        item = cur.fetchone()
        self.stopTimer('dec')
        return item

    def get_unused_hashes(self):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT `hash_id` FROM `%s` " % self.getName()+
                    " WHERE `cnt` <= 0")
        hashes = (str(item["hash_id"]) for item in iter(cur.fetchone,None))
        self.stopTimer('get_unused_hashes')
        return hashes

    def count_unused_hashes(self):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT COUNT(1) as `unused` FROM `%s` " % self.getName()+
                    " WHERE `cnt` <= 0")
        item = cur.fetchone()
        cnt = 0
        if item:
            cnt = item["unused"]
        self.stopTimer('count_unused_hashes')
        return cnt

    pass
