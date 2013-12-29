# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableInode( Table ):

    _table_name = "inode"

    def create( self ):
        cur = self.getCursor()

        # Create table
        cur.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "`id` BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT, "+
                "`nlinks` INT UNSIGNED NOT NULL, "+
                "`mode` SMALLINT UNSIGNED NOT NULL, "+
                "`uid` SMALLINT UNSIGNED NOT NULL, "+
                "`gid` SMALLINT UNSIGNED NOT NULL, "+
                "`rdev` INT UNSIGNED NOT NULL, "+
                "`size` BIGINT UNSIGNED NOT NULL, "+
                "`atime` INT UNSIGNED NOT NULL, "+
                "`mtime` INT UNSIGNED NOT NULL, "+
                "`ctime` INT UNSIGNED NOT NULL, "+
                "`atime_ns` INT UNSIGNED NOT NULL DEFAULT 0, "+
                "`mtime_ns` INT UNSIGNED NOT NULL DEFAULT 0, "+
                "`ctime_ns` INT UNSIGNED NOT NULL DEFAULT 0"+
            ");"
        )

        self.createIndexIfNotExists("id_nlinks", ('id', 'nlinks',))
        return

    def getRowSize(self):
        return 8 + 4 + 2 + 2 + 2 + 4 + 8 + 6 * 4

    def insert( self, nlinks, mode,
                uid=-1, gid=-1, rdev=0, size=0, atime=0, mtime=0, ctime=0,
                atime_ns=0, mtime_ns=0, ctime_ns=0):
        """
        :param value: bytes
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("INSERT INTO `%s`" % self.getName() +
                    "(`nlinks`, `mode`, `uid`, `gid`, `rdev`, `size`, `atime`, `mtime`, `ctime`, `atime_ns`, `mtime_ns`, `ctime_ns`) " +
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (
            nlinks, mode, uid, gid, rdev, size, atime, mtime, ctime, atime_ns, mtime_ns, ctime_ns
        ))
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update_data( self, inode_id, row_data=None):
        """
        :param value: bytes
        :return: int
        """
        if not row_data:
            return 0

        self.startTimer()
        cur = self.getCursor()

        query = "UPDATE `%s` SET " % self.getName()
        params = ()
        values = ()
        for key in row_data.keys():
            if key == "id":
                continue
            params += ("`%s`=%%s" % key,)
            values += (row_data[key],)

        if not values:
            return 0

        query += ", ".join(params)
        query += " WHERE id=%s"

        values += (inode_id,)

        cur.execute(query, values)
        item = cur.rowcount
        self.stopTimer('update_data')
        return item

    def set_size(self, inode, size):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `size`=%(size)s WHERE `id`=%(id)s",
            {
                "size": size,
                "id": inode
            }
        )
        count = cur.rowcount
        self.stopTimer('set_size')
        return count

    def get(self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT * FROM `%s` " % self.getName()+
            " WHERE `id`=%(id)s",
            {
                "id": inode
            }
        )
        item = cur.fetchone()
        self.stopTimer('get')
        return item

    def get_mode(self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT `mode` FROM `%s` " % self.getName()+
            " WHERE `id`=%(id)s",
            {
                "id": inode
            }
        )
        item = int(cur.fetchone()["mode"])
        self.stopTimer('get_mode')
        return item

    def get_size(self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT `size` FROM `%s` " % self.getName()+
            " WHERE `id`=%(id)s",
            {
                "id": inode
            }
        )
        item = int(cur.fetchone()["size"])
        self.stopTimer('get_size')
        return item

    def inc_nlinks(self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `nlinks`=`nlinks`+1 WHERE `id`=%(id)s",
            {
                "id": inode
            }
        )
        count = cur.rowcount
        self.stopTimer('inc_nlinks')
        return count

    def dec_nlinks(self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `nlinks`=`nlinks`-1 WHERE `id`=%(id)s",
            {
                "id": inode
            }
        )
        count = cur.rowcount
        self.stopTimer('dec_nlinks')
        return count

    def count_nlinks_by_ids(self, id_list):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT COUNT(1) as `cnt` FROM `%s` WHERE `id` IN (%s) AND nlinks>0" % (
            self._table_name, ",".join(id_list),)
        )
        result = cur.fetchone()["cnt"]
        self.stopTimer('count_nlinks_by_ids')
        return result

    def get_count(self):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT COUNT(1) as `cnt` FROM `%s`" % self.getName())
        item = cur.fetchone()
        self.stopTimer('get_count')
        return item["cnt"]

    pass
