# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableHashOwner( Table ):

    _table_name = "hash_owner"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "`hash_id` BIGINT UNSIGNED PRIMARY KEY,"+
                "`uuid` char(32)"+
            ")"+
            self._getCreationAppendString()
        )
        self.createIndexIfNotExists("hash", ('hash_id',))
        self.createIndexIfNotExists("owner", ('uuid',))
        return

    def addHashOwner( self, hash_id, uuid):
        """
        :param hash_id: int
        :param uuid: str
        :return: None
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            " (`hash_id`,`uuid`) VALUES (%(id)s, '%(uuid)s')",
            {
                "id": hash_id,
                "uuid":uuid,
            }
        )

        self.stopTimer('addHashOwner')
        return None

    def hashHasOwner( self, hash_id, uuid):
        """
        :param hash_id: int
        :param uuid: str
        :return: int|null
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT COUNT(1) AS `cnt` FROM `%s` " % self.getName()+
            " WHERE `hash_id`=%(id)s AND `uuid`='%(uuid)s'",
            {
                "id": some_id,
                "uuid":uuid,
            }
        )
        item = cur.fetchone()
        self.stopTimer('hashHasOwner')
        return item['cnt']

    pass
