# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableName( Table ):

    _table_name = "name"

    def create(self):
        cur = self.getCursor()

        # Create table
        cur.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT, "+
                "value BLOB NOT NULL"+
            ");"
        )
        try:
            cur.execute(
                "ALTER TABLE %(table_name)s ADD UNIQUE INDEX %(index_name)s (`value`)",
                {
                    "table_name": self.getName(),
                    "index_name": self.getName() + "_value"
                }
            )
        except:
            pass
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

        cur.execute("INSERT INTO `%s`(value) VALUES (%%s)" % self._table_name, (value,))
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

        cur.execute("SELECT id FROM `%s` WHERE value=%%s" % self._table_name, (value,))
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

        cur.execute("SELECT value FROM `%s` WHERE id=%%d" % self._table_name, (name_id,))
        item = cur.fetchone()
        if item:
            item = item["value"]
        self.stopTimer('get')
        return item

    pass
