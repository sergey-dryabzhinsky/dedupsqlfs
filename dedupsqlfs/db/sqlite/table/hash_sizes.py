# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.sqlite.table import Table

class TableHashSizes( Table ):

    _table_name = "hash_sizes"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "`hash_id` INTEGER PRIMARY KEY, "+
                "`writed_size` INTEGER NOT NULL,"+
                "`compressed_size` INTEGER NOT NULL"+
            ");"
        )
        return

    def insert( self, hash_id, writed_size, compressed_size):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("INSERT INTO `%s`(hash_id, `writed_size`, `compressed_size`) VALUES (?,?,?)" % self.getName(),
                    (hash_id, writed_size, compressed_size,))
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update( self, hash_id, writed_size, compressed_size):
        """
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("UPDATE `%s` SET writed_size=?, compressed_size=? WHERE hash_id=?" % self.getName(),
                    (writed_size, compressed_size, hash_id,))
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
        cur.execute("SELECT * FROM `%s` WHERE hash_id=?" % self.getName(), (hash_id,))
        item = cur.fetchone()
        self.stopTimer('get')
        return item

    def remove_by_ids(self, id_str):
        self.startTimer()
        count = 0
        if id_str:
            cur = self.getCursor()
            cur.execute("DELETE FROM `%s` " % self.getName()+
                        " WHERE `hash_id` IN (%s)" % (id_str,))
            count = cur.rowcount
        self.stopTimer('remove_by_ids')
        return count

    def get_sizes_by_hash_ids(self, id_str):
        self.startTimer()
        items = {}
        if id_str:
            cur = self.getCursor()
            cur.execute("SELECT * FROM `%s` " % self.getName()+
                        " WHERE `hash_id` IN (%s)" % (id_str,))
            for _i in iter(cur.fetchone, None):
                items[ _i["hash_id"] ] = (_i["writed_size"], _i["compressed_size"],)

        self.stopTimer('get_sizes_by_hash_ids')
        return items


    def get_median_compressed_size(self):
        self.startTimer()

        cur = self.getCursor()

        csizes = []
        cur.execute("SELECT `compressed_size` AS `s` FROM `%s`" % self.getName())
        for _i in iter(cur.fetchone, None):
            csizes.append(_i["s"])

        if not len(csizes):
            return 0

        from dedupsqlfs.my_math import quickselect_median
        median = quickselect_median(csizes)

        self.stopTimer('get_median_compressed_size')
        return median

    def get_mean_compressed_size(self):
        self.startTimer()

        cur = self.getCursor()

        cur.execute("SELECT SUM(`compressed_size`) AS `s` FROM `%s`" % self.getName())
        item = cur.fetchone()

        isum = item["s"]
        if not isum:
            self.stopTimer('get_mean_compressed_size')
            return 0

        cur.execute("SELECT COUNT(1) AS `c` FROM `%s`" % self.getName())
        item = cur.fetchone()

        icount = item["c"]
        if not icount:
            self.stopTimer('get_mean_compressed_size')
            return 0

        result = isum / icount

        self.stopTimer('get_mean_compressed_size')
        return result

    pass
