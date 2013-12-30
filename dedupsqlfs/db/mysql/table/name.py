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

    def get_names_count(self):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT COUNT(`id`) as `cnt` FROM `%s`" % self.getName())
        item = cur.fetchone()
        if item:
            item = item["cnt"]
        else:
            item = 0
        self.stopTimer('get_names_count')
        return item

    def get_name_ids(self, start_id, end_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT `id` FROM `%s` " % self.getName()+
                    " WHERE `id`>=%s AND `id`<%s", (start_id, end_id,))
        nameIds = tuple(str(item["id"]) for item in cur.fetchall())
        self.stopTimer('get_name_ids')
        return nameIds

    def remove_by_ids(self, name_ids):
        self.startTimer()
        count = 0
        id_str = ",".join(name_ids)
        if id_str:
            cur = self.getCursor()
            cur.execute("DELETE FROM `%s` " % self.getName()+
                        " WHERE `id` IN (%s)" % (id_str,))
            count = cur.rowcount
        self.stopTimer('remove_by_ids')
        return count

    pass
