# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.sqlite.table import Table

class TableHashBlockSize( Table ):

    _table_name = "hash_block_size"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "hash_id INTEGER PRIMARY KEY, "+
                "real_size INTEGER NOT NULL, "+
                "comp_size INTEGER NOT NULL "+
            ");"
        )
        return

    def insert( self, hash_id, real_size, comp_size):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("INSERT INTO `%s`(hash_id, real_size, comp_size) VALUES (?,?,?)" % self._table_name,
                    (hash_id, real_size, comp_size,))
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update( self, hash_id, real_size, comp_size):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("UPDATE `%s` SET real_size=?, comp_size=? WHERE hash_id=?" % self._table_name,
                    (real_size, comp_size, hash_id,))
        count = cur.rowcount
        self.stopTimer('update')
        return count

    def get( self, hash_id):
        """
        :param hash_id: int
        :return: Row
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE hash_id=?" % self._table_name, (hash_id,))
        item = cur.fetchone()
        self.stopTimer('get')
        return item

    def get_real_sizes( self, hash_ids):
        """
        :param hash_ids: list|tuple
        :return: int
        """
        self.startTimer()

        items = ()
        hids = ",".join((str(hid) for hid in hash_ids))
        if hids:
            cur = self.getCursor()
            cur.execute(
                "SELECT `real_size`,`hash_id` FROM `%s` " % self.getName()+
                " WHERE `hash_id` IN (%s)" % hids
            )
            items = cur.fetchall()
        self.stopTimer('get_real_sizes')
        return items

    def sum_real_size( self, hash_ids):
        """
        :param hash_ids: list|tuple
        :return: int
        """
        self.startTimer()

        item = 0
        hids = ",".join((str(hid) for hid in hash_ids))
        if hids:
            cur = self.getCursor()
            cur.execute(
                "SELECT SUM(`real_size`) as `s` FROM `%s` " % self.getName()+
                " WHERE `hash_id` IN (%s)" % hids
            )
            item = cur.fetchone()
            if item:
                item = int(item["s"])
            else:
                item = 0
        self.stopTimer('sum_real_size')
        return item

    def sum_real_size_all( self ):
        """
        :param hash_ids: list|tuple
        :return: int
        """
        self.startTimer()

        cur = self.getCursor()
        cur.execute("SELECT SUM(`real_size`) as `s` FROM `%s` " % self.getName())
        item = cur.fetchone()
        if item:
            item = int(item["s"])
        else:
            item = 0
        self.stopTimer('sum_real_size_all')
        return item

    def sum_comp_size( self, hash_ids):
        """
        :param hash_ids: list|tuple
        :return: int
        """
        self.startTimer()

        item = 0
        hids = ",".join((str(hid) for hid in hash_ids))
        if hids:
            cur = self.getCursor()
            cur.execute(
                "SELECT SUM(`comp_size`) as `s` FROM `%s` " % self.getName()+
                " WHERE `hash_id` IN (%s)" % hids
            )
            item = cur.fetchone()
            if item:
                item = int(item["s"])
            else:
                item = 0
        self.stopTimer('sum_comp_size')
        return item

    def sum_comp_size_all( self ):
        """
        :param hash_ids: list|tuple
        :return: int
        """
        self.startTimer()

        cur = self.getCursor()
        cur.execute("SELECT SUM(`comp_size`) as `s` FROM `%s` " % self.getName())
        item = cur.fetchone()
        if item:
            item = int(item["s"])
        else:
            item = 0
        self.stopTimer('sum_comp_size_all')
        return item

    def remove_by_ids(self, hash_ids):
        self.startTimer()
        count = 0
        id_str = ",".join(hash_ids)
        if id_str:
            cur = self.getCursor()
            cur.execute("DELETE FROM `%s` " % self.getName()+
                        " WHERE `hash_id` IN (%s)" % (id_str,))
            count = cur.rowcount
        self.stopTimer('remove_by_ids')
        return count

    pass
