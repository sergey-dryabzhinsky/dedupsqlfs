# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableHashBlockSize( Table ):

    _table_name = "hash_block_size"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "`hash_id` BIGINT UNSIGNED PRIMARY KEY, "+
                "`real_size` INT UNSIGNED NOT NULL, "+
                "`comp_size` INT UNSIGNED NOT NULL "+
            ")"+
            self._getCreationAppendString()
        )
        return

    def insert( self, hash_id, real_size, comp_size):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute(
            "INSERT INTO `%s` " %self.getName()+
            " (`hash_id`, `real_size`, `comp_size`) "+
            "VALUES (%(hash_id)s, %(real_size)s, %(comp_size)s)",
            {
                "hash_id": hash_id,
                "real_size": real_size,
                "comp_size": comp_size
            }
        )
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update( self, hash_id, real_size, comp_size):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `real_size`=%(real_size)s, `comp_size`=%(comp_size)s WHERE `hash_id`=%(id)s",
            {
                "real_size": real_size,
                "comp_size": comp_size,
                "id": hash_id
            }
        )
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
        cur.execute(
            "SELECT * FROM `%s` " % self.getName()+
            " WHERE `hash_id`=%(id)s",
            {
                "id": hash_id
            }
        )
        item = cur.fetchone()
        self.stopTimer('get')
        return item

    def get_real_sizes( self, hash_ids):
        """
        :param hash_ids: list|tuple
        :return: tuple
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

    def get_hashes_to_comp_sizes( self, hash_ids):
        """
        :param hash_ids: list|tuple
        :return: dict
        """
        self.startTimer()

        items = {}
        hids = ",".join((str(hid) for hid in hash_ids))
        if hids:
            cur = self.getCursor()
            cur.execute(
                "SELECT `comp_size`,`hash_id` FROM `%s` " % self.getName()+
                " WHERE `hash_id` IN (%s)" % hids
            )
            for item in cur.fetchall():
                items[ str(item["hash_id"]) ] = item["comp_size"]
        self.stopTimer('get_hashes_to_comp_sizes')
        return items

    def get_hashes_to_real_sizes( self, hash_ids):
        """
        :param hash_ids: list|tuple
        :return: dict
        """
        self.startTimer()

        items = {}
        hids = ",".join((str(hid) for hid in hash_ids))
        if hids:
            cur = self.getCursor()
            cur.execute(
                "SELECT `real_size`,`hash_id` FROM `%s` " % self.getName()+
                " WHERE `hash_id` IN (%s)" % hids
            )
            for item in cur.fetchall():
                items[ str(item["hash_id"]) ] = item["real_size"]
        self.stopTimer('get_hashes_to_real_sizes')
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
