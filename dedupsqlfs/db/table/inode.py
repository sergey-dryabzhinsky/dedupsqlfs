# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.table import Table

class TableInode( Table ):

    _table_name = "inode"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "+
                "nlinks INTEGER NOT NULL, "+
                "mode INTEGER NOT NULL, "+
                "uid INTEGER NOT NULL, "+
                "gid INTEGER NOT NULL, "+
                "rdev INTEGER NOT NULL, "+
                "size INTEGER NOT NULL, "+
                "atime INTEGER NOT NULL, "+
                "mtime INTEGER NOT NULL, "+
                "ctime INTEGER NOT NULL, "+
                "atime_ns INTEGER NOT NULL DEFAULT 0, "+
                "mtime_ns INTEGER NOT NULL DEFAULT 0, "+
                "ctime_ns INTEGER NOT NULL DEFAULT 0"+
            ");"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS inode_nlinks ON `%s` (" % self._table_name+
                "nlinks"+
            ");"
        )
        return

    def getRowSize(self):
        return 8 * 13

    def insert( self, nlinks, mode,
                uid=-1, gid=-1, rdev=0, size=0, atime=0, mtime=0, ctime=0,
                atime_ns=0, mtime_ns=0, ctime_ns=0):
        """
        :param value: bytes
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("INSERT INTO `%s`(nlinks, mode, uid, gid, rdev, size, atime, mtime, ctime, atime_ns, mtime_ns, ctime_ns) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" % self._table_name, (
            nlinks, mode, uid, gid, rdev, size, atime, mtime, ctime, atime_ns, mtime_ns, ctime_ns
        ))
        item = cur.lastrowid
        self.commit()
        self.stopTimer()
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

        query = "UPDATE `%s` SET " % self._table_name
        params = ()
        values = ()
        for key in row_data.keys():
            params += ("%s=?" % key,)
            values += (row_data[key],)
        query += ", ".join(params)
        query += " WHERE id=?"

        values += (inode_id,)

        cur.execute(query, values)
        item = cur.rowcount
        self.commit()
        self.stopTimer()
        return item

    def set_mode(self, inode, mode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET mode=? WHERE id=?" % self._table_name, (mode, inode,))
        count = cur.rowcount
        self.commit()
        self.stopTimer()
        return count

    def set_uid(self, inode, uid):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET uid=? WHERE id=?" % self._table_name, (uid, inode,))
        count = cur.rowcount
        self.commit()
        self.stopTimer()
        return count

    def set_gid(self, inode, gid):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET gid=? WHERE id=?" % self._table_name, (gid, inode,))
        count = cur.rowcount
        self.commit()
        self.stopTimer()
        return count

    def set_size(self, inode, size):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET size=? WHERE id=?" % self._table_name, (size, inode,))
        count = cur.rowcount
        self.commit()
        self.stopTimer()
        return count

    def set_atime(self, inode, atime, atime_ns=0):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET atime=?, atime_ns=? WHERE id=?" % self._table_name, (
            atime, atime_ns, inode,
        ))
        count = cur.rowcount
        self.commit()
        self.stopTimer()
        return count

    def set_mtime(self, inode, mtime, mtime_ns=0):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET mtime=?, mtime_ns=? WHERE id=?" % self._table_name, (
            mtime, mtime_ns, inode,
        ))
        count = cur.rowcount
        self.commit()
        self.stopTimer()
        return count

    def set_ctime(self, inode, ctime, ctime_ns=0):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET ctime=?, ctime_ns=? WHERE id=?" % self._table_name, (
            ctime, ctime_ns, inode,
        ))
        count = cur.rowcount
        self.commit()
        self.stopTimer()
        return count

    def get(self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE id=?" % self._table_name, (inode,))
        item = cur.fetchone()
        self.stopTimer()
        return item

    def get_mode(self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT mode FROM `%s` WHERE id=?" % self._table_name, (inode,))
        item = int(cur.fetchone()["mode"])
        self.stopTimer()
        return item

    def inc_nlinks(self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET nlinks = nlinks + 1 WHERE id = ?" % self._table_name, (inode,))
        count = cur.rowcount
        self.commit()
        self.stopTimer()
        return count

    def dec_nlinks(self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET nlinks = nlinks - 1 WHERE id = ?" % self._table_name, (inode,))
        count = cur.rowcount
        self.commit()
        self.stopTimer()
        return count

    def remove_by_nlinks(self):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("DELETE FROM `%s` WHERE nlinks <= 0" % self._table_name)
        count = cur.rowcount
        self.commit()
        self.stopTimer()
        return count

    def count_nlinks_by_ids(self, id_list):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT COUNT(1) as cnt FROM `%s` WHERE id IN (%s) AND nlinks>0" % (
            self._table_name, ",".join(id_list),)
        )
        result = cur.fetchone()["cnt"]
        self.stopTimer()
        return result

    def get_count(self):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT COUNT(1) as cnt FROM `%s`" % self._table_name)
        item = cur.fetchone()
        self.stopTimer()
        return item["cnt"]

    pass
