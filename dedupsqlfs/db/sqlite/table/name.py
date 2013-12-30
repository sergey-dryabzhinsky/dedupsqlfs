# -*- coding: utf8 -*-

__author__ = 'sergey'

import sqlite3
from dedupsqlfs.db.sqlite.table import Table

class TableName( Table ):

    _table_name = "name"

    def create(self):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "+
                "value BLOB NOT NULL, "+
                "UNIQUE(value) "
            ");"
        )
        return

    def getRowSize(self, value):
        """
        :param value: bytes
        :return: int
        """
        bvalue = sqlite3.Binary(value)
        return 8 + len(bvalue)*2+12

    def insert(self, value):
        """
        :param value: bytes
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        bvalue = sqlite3.Binary(value)

        cur.execute("INSERT INTO `%s`(value) VALUES (?)" % self._table_name, (bvalue,))
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def find(self, value):
        """
        :param value: bytes
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        bvalue = sqlite3.Binary(value)

        cur.execute("SELECT id FROM `%s` WHERE value=?" % self._table_name, (bvalue,))
        item = cur.fetchone()
        if item:
            item = item["id"]
        self.stopTimer('find')
        return item

    def get(self, name_id):
        """
        :param name_id: int
        :return: bytes
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("SELECT value FROM `%s` WHERE id=?" % self._table_name, (name_id,))
        item = cur.fetchone()
        if item:
            item = item["value"]
        self.stopTimer('get')
        return item

    def get_count(self):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT COUNT(`id`) as `cnt` FROM `%s`" % self.getName())
        item = cur.fetchone()
        if item:
            item = item["cnt"]
        else:
            item = 0
        self.stopTimer('get_count')
        return item

    def get_name_ids(self, start_id, end_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT `id` FROM `%s` " % self.getName()+
                    " WHERE `id`>=? AND `id`<?", (start_id, end_id,))
        nameIds = tuple(str(item["id"]) for item in cur.fetchall())
        self.stopTimer('get_name_ids')
        return nameIds

    def remove_by_ids(self, name_ids):
        self.startTimer()
        count = 0
        id_str = ",".join(name_ids)
        if id_str:
            cur = self.getCursor()
            cur.execute("DELETE FROM `%s` WHERE `id` IN (%s)" % (self.getName(), id_str,))
            count = cur.rowcount
        self.stopTimer('remove_by_ids')
        return count

    pass
