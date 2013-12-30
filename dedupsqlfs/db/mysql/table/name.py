# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableName( Table ):

    _table_name = "name"
    _key_block_size = 2

    def create(self):
        cur = self.getCursor()

        # Create table
        cur.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "`id` BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT, "+
                "`value` BLOB NOT NULL"+
            ")"+
            self._getCreationAppendString()
        )

        self.createIndexIfNotExists("value", ('value',), unique=True, indexSizes={'value':200})
        return

    def getRowSize(self, value):
        """
        :param value: bytes
        :return: int
        """
        return 8 + len(value)+2

    def insert(self, value):
        """
        :param value: bytes
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            " (`value`) VALUES (%s)", (value,))
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

        cur.execute(
            "SELECT `id` FROM `%s` " % self.getName()+
            " WHERE `value`=%s", (value,))
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

        cur.execute("SELECT `value` FROM `%s` " % self.getName()+
                    " WHERE `id`=%s", (name_id,))
        item = cur.fetchone()
        if item:
            item = item["value"]
        self.stopTimer('get')
        return item

    pass
