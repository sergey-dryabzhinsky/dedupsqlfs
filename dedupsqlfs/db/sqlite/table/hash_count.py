# -*- coding: utf8 -*-
"""
Таблица для дефрагментации
Счетчик используемых hash_id в индексах блоков
Увеличивается при сохранении нового
Уменьшается при удалении
"""

__author__ = 'sergey'

from dedupsqlfs.db.sqlite.table import Table

class TableHashCount( Table ):

    _table_name = "hash_count"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "hash_id INTEGER PRIMARY KEY,"+
                "cnt INTEGER"+
            ");"
        )
        return

    def insert( self, hash_id):
        """
        :param hash_id: int
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("INSERT INTO `%s`(hash_id, cnt) VALUES (?,1)" % self.getName(), (
            hash_id,
        ))

        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def find( self, hash_id):
        """
        :param hash_id: int
        :return: dict|null
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT `hash_id` FROM `%s` WHERE `hash_id`=?" % self.getName(), (
            hash_id,
        ))
        item = cur.fetchone()
        self.stopTimer('find')
        return item

    def inc( self, hash_id):
        """
        Increase counter

        :param hash_id: int
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET `cnt`=`cnt`+1 WHERE `hash_id`=?" % self.getName(), (
            hash_id,
        ))
        item = cur.fetchone()
        self.stopTimer('inc')
        return item

    def dec( self, hash_id):
        """
        Decrease counter

        :param hash_id: int
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET `cnt`=`cnt`-1 WHERE `hash_id`=?" % self.getName(), (
            hash_id,
        ))
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
