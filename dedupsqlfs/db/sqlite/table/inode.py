# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.sqlite.table import Table

class TableInode( Table ):

    _table_name = "inode"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "+
                "nlinks INTEGER NOT NULL, "+
                "mode INTEGER NOT NULL, "+
                "uid INTEGER NOT NULL, "+
                "gid INTEGER NOT NULL, "+
                "rdev INTEGER NOT NULL, "+
                "size INTEGER NOT NULL, "+
                "atime INTEGER NOT NULL, "+
                "mtime INTEGER NOT NULL, "+
                "ctime INTEGER NOT NULL"+
            ");"
        )
        self.createIndexIfNotExists('id_nlinks', ('id', 'nlinks',))
        return

    def getRowSize(self):
        return 8 * 10

    def insert( self, nlinks, mode,
                uid=-1, gid=-1, rdev=0, size=0, atime=0, mtime=0, ctime=0):
        """
        :param value: bytes
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        cur.execute("INSERT INTO `%s`(nlinks, mode, uid, gid, rdev, size, atime, mtime, ctime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)" % self.getName(), (
            nlinks, mode, uid, gid, rdev, size, atime, mtime, ctime,
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
            params += ("%s=?" % key,)
            values += (row_data[key],)

        if not values:
            return 0

        query += ", ".join(params)
        query += " WHERE id=?"

        values += (inode_id,)

        cur.execute(query, values)
        item = cur.rowcount
        self.stopTimer('update_data')
        return item

    def get(self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE id=?" % self.getName(), (inode,))
        item = cur.fetchone()
        self.stopTimer('get')
        return item

    def get_mode(self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT `mode` FROM `%s` WHERE id=?" % self.getName(), (inode,))
        item = int(cur.fetchone()["mode"])
        self.stopTimer('get_mode')
        return item

    def get_size(self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT `size` FROM `%s` WHERE id=?" % self.getName(), (inode,))
        item = int(cur.fetchone()["size"])
        self.stopTimer('get_size')
        return item

    def inc_nlinks(self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET nlinks = nlinks + 1 WHERE id = ?" % self.getName(), (inode,))
        count = cur.rowcount
        self.stopTimer('inc_nlinks')
        return count

    def dec_nlinks(self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET nlinks = nlinks - 1 WHERE id = ?" % self.getName(), (inode,))
        count = cur.rowcount
        self.stopTimer('dec_nlinks')
        return count

    def count_nlinks_by_ids(self, id_list):
        self.startTimer()

        result = 0
        id_str = ",".join(id_list)
        if id_str:
            cur = self.getCursor()
            cur.execute(
                "SELECT COUNT(1) as `cnt` FROM `%s` WHERE `id` IN (%s) AND `nlinks`!=0" % (
                self.getName(), id_str,)
            )
            result = cur.fetchone()["cnt"]

        self.stopTimer('count_nlinks_by_ids')
        return result

    def get_count(self):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT COUNT(1) as cnt FROM `%s`" % self.getName())
        item = cur.fetchone()
        self.stopTimer('get_count')
        return item["cnt"]

    def get_sizes(self):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT SUM(`size`) as `s` FROM `%s` WHERE `nlinks`>0" % self.getName())
        item = cur.fetchone()
        if not item or item["s"] is None:
            item = 0
        else:
            item = item["s"]
        self.stopTimer('get_sizes')
        return item

    def get_sizes_by_id(self, inodes):
        self.startTimer()
        items = {}
        id_str = ",".join(inodes)
        if id_str:
            cur = self.getCursor()
            cur.execute("SELECT `id`,`size` FROM `%s`" % self.getName()+
                        " WHERE id in (%s)" % id_str)
            for item in iter(cur.fetchone, None):
                items[ str(item["id"]) ] = item["size"]

        self.stopTimer('get_sizes_by_id')
        return items

    def get_sizes_by_inodes(self, inodes):
        self.startTimer()

        item = 0
        id_str = ",".join(str(i) for i in inodes)
        if id_str:
            cur = self.getCursor()
            cur.execute("SELECT SUM(`size`) as `s` FROM `%s` " % self.getName()+
                        " WHERE `id` IN (%s) AND `nlinks`>0" % id_str)
            item = cur.fetchone()
            if item and item["s"]:
                item = int(item["s"])
        self.stopTimer('get_sizes_by_inodes')
        return item

    def get_inode_ids(self, start_id, end_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT `id` FROM `%s` " % self.getName()+
                    " WHERE `id`>=? AND `id`<?", (start_id, end_id,))
        nameIds = set(str(item["id"]) for item in iter(cur.fetchone,None))
        self.stopTimer('get_inode_ids')
        return nameIds

    def remove_by_ids(self, inode_ids):
        self.startTimer()
        count = 0
        id_str = ",".join(inode_ids)
        if id_str:
            cur = self.getCursor()
            cur.execute("DELETE FROM `%s` " % self.getName()+
                        " WHERE `id` IN (%s)" % (id_str,))
            count = cur.rowcount
        self.stopTimer('remove_by_ids')
        return count

    def get_inodes_by_inodes(self, inode_ids):
        self.startTimer()

        iids = set()
        id_str = ",".join(inode_ids)
        if id_str:
            cur = self.getCursor()
            cur.execute("SELECT `id` FROM `%s` " % self.getName()+
                            " WHERE `id` IN (%s)" % (id_str,))
            iids = set(str(item["id"]) for item in iter(cur.fetchone,None))

        self.stopTimer('get_inodes_by_inodes')
        return iids

    pass
