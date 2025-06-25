# -*- coding: utf8 -*-
"""
Таблица для дефрагментации
Счетчик используемых hash_id в индексах блоков
Увеличивается при сохранении нового
Уменьшается при удалении
"""

__author__ = 'sergey'

from dedupsqlfs.db.sqlite.table import Table

class TableHashOwner( Table ):

    _table_name = "hash_owner"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "hash_id INTEGER PRIMARY KEY,"+
                "uuid TEXT"+
            ");"
        )
        self.createIndexIfNotExists('owner', ("uuid",))
        return

    def hashHasOwner( self, hash_id, uuid):
        """
        :param hash_id: int
        :param uuid: str
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT COUNT(1) AS `cnt` FROM `%s` WHERE `hash_id`=? AND `uuid`=?" % self.getName(), (
            hash_id, uuid,
        ))
        item = cur.fetchone()
        self.stopTimer('hashHasOwner')
        return item['cnt']

    def addHashOwner( self, hash_id, uuid):
        """
        :param hash_id: int
        :return: None
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute("INSERT INTO `%s` (`hash_id`,`uuid`) VALUES (?,?)" % self.getName(), (
            hash_id, uuid,
        ))
        self.stopTimer('addHashOwner')
        return None

    pass
