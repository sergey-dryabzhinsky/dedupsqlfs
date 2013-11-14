# -*- coding: utf8 -*-

__author__ = 'sergey'

import sqlite3
from dedupsqlfs.db.table import Table

class TableName( Table ):

    _table_name = "name"

    def create(self):
        self.startTimer()
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "+
                "value BLOB NOT NULL, "+
                "UNIQUE(value) "
            ");"
        )
        self.stopTimer()
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
        self.commit()
        self.stopTimer()
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
        self.stopTimer()
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
        self.stopTimer()
        return item

    pass
