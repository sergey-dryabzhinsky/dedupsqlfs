# -*- coding: utf8 -*-

__author__ = 'sergey'

import sqlite3
from dedupsqlfs.db.table import Table

class TableName( Table ):

    _table_name = "name"

    def create(self):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "+
                "value BLOB NOT NULL UNIQUE"+
            ");"
        )
        return

    def insert(self, value):
        """
        :param value: bytes
        :return: int
        """
        cur = self.getCursor()

        bvalue = sqlite3.Binary(value)

        cur.execute("INSERT INTO `%s`(value) VALUES (?)" % self._table_name, (bvalue,))
        item = cur.lastrowid
        self.commit()
        return item

    def find(self, value):
        """
        :param value: bytes
        :return: int
        """
        cur = self.getCursor()

        bvalue = sqlite3.Binary(value)

        cur.execute("SELECT id FROM `%s` WHERE value=?" % self._table_name, (bvalue,))
        item = cur.fetchone()
        if item:
            item = item["id"]
        return item

    def get(self, name_id):
        """
        :param name_id: int
        :return: bytes
        """
        cur = self.getCursor()

        cur.execute("SELECT value FROM `%s` WHERE id=?" % self._table_name, (name_id,))
        item = cur.fetchone()
        if item:
            item = item["value"]
        return item

    def remove_by_ids(self, ids_list, _not_=False):
        cur = self.getCursor()
        if not _not_:
            cur.execute("DELETE FROM `%s` WHERE id IN (%s)" % (self._table_name, ",".join(ids_list,)))
        else:
            cur.execute("DELETE FROM `%s` WHERE id NOT IN (%s)" % (self._table_name, ",".join(ids_list,)))
        count = cur.rowcount
        self.commit()
        return count

    pass
