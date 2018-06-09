# -*- coding: utf8 -*-

__author__ = 'sergey'

import hashlib
from time import time
from dedupsqlfs.db.mysql.table import Table

class TableSubvolume( Table ):

    _table_name = "subvolume"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "`id` INT UNSIGNED PRIMARY KEY AUTO_INCREMENT, "+
                "`hash` BINARY(16) NOT NULL, "+
                "`name` BLOB NOT NULL, "+
                "`stats` TEXT, "+
                "`root_diff` TEXT, "+
                "`readonly` TINYINT UNSIGNED NOT NULL DEFAULT 0, "+
                "`stats_at` INT UNSIGNED, "+
                "`root_diff_at` INT UNSIGNED, "+
                "`created_at` INT UNSIGNED NOT NULL, "+
                "`mounted_at` INT UNSIGNED, "+
                "`updated_at` INT UNSIGNED"+
            ")"+
            self._getCreationAppendString()
        )
        return

    def insert( self, name, created_at, mounted_at=None, updated_at=None, stats_at=None, stats=None, root_diff_at=None, root_diff=None ):
        """
        :param name: str            - name for subvolume/snapshot
        :param created_at: int      - creation time
        :param mounted_at: int|None - subvolume mounted
        :param updated_at: int|None - subvolume updated
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        digest = hashlib.new('md5', name).digest()

        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            " (`hash`,`name`,`created_at`, `mounted_at`, `updated_at`, `stats_at`, `stats`, `root_diff_at`, `root_diff`) "+
            "VALUES (%(hash)s, %(name)s, %(created)s, %(mounted)s, %(updated)s, %(statsed)s, %(stats)s, %(diffed)s, %(root_diff)s)",
            {
                "hash": digest,
                "name": name,
                "created": int(created_at),
                "mounted": mounted_at,
                "updated": updated_at,
                "statsed": stats_at,
                "stats": stats,
                "diffed": root_diff_at,
                "root_diff": root_diff
            }
        )
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def readonly(self, subvol_id, flag=True):
        self.startTimer()
        if flag:
            flag = 1
        else:
            flag = 0
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `readonly`=%(readonly)s WHERE `id`=%(id)s",
            {
                "readonly": flag,
                "id": subvol_id
            }
        )
        self.stopTimer('readonly')
        return cur.rowcount

    def mount_time(self, subvol_id, mtime=None):
        self.startTimer()
        if mtime is None:
            mtime = time()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `mounted_at`=%(mounted)s WHERE `id`=%(id)s",
            {
                "mounted": int(mtime),
                "id": subvol_id
            }
        )
        self.stopTimer('mount_time')
        return cur.rowcount

    def update_time(self, subvol_id, utime=None):
        self.startTimer()
        if utime is None:
            utime = time()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `updated_at`=%(updated)s WHERE `id`=%(id)s",
            {
                "updated": int(utime),
                "id": subvol_id
            }
        )
        self.stopTimer('update_time')
        return cur.rowcount

    def stats_time(self, subvol_id, stime=None):
        self.startTimer()
        if stime is None:
            stime = time()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `stats_at`=%(stime)s WHERE `id`=%(id)s",
            {
                "stime": int(stime),
                "id": subvol_id
            }
        )
        self.stopTimer('stats_time')
        return cur.rowcount

    def set_stats(self, subvol_id, stats):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `stats`=%(stats)s WHERE `id`=%(id)s",
            {
                "stats": stats,
                "id": subvol_id
            }
        )
        self.stopTimer('set_stats')
        return cur.rowcount

    def root_diff_time(self, subvol_id, rtime=None):
        self.startTimer()
        if rtime is None:
            rtime = time()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `root_diff_at`=%(rtime)s WHERE `id`=%(id)s",
            {
                "rtime": int(rtime),
                "id": subvol_id
            }
        )
        self.stopTimer('stats_time')
        return cur.rowcount

    def set_root_diff(self, subvol_id, root_diff):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `root_diff`=%(rdiff)s WHERE `id`=%(id)s",
            {
                "rdiff": root_diff,
                "id": subvol_id
            }
        )
        self.stopTimer('set_stats')
        return cur.rowcount

    def delete(self, subvol_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "DELETE FROM `%s` " % self.getName()+
            " WHERE `id`=%(id)s",
            {
                "id": subvol_id
            }
        )
        item = cur.rowcount
        self.stopTimer('delete')
        return item

    def get(self, subvol_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT * FROM `%s` " % self.getName()+
            " WHERE `id`=%(id)s",
            {
                "id": subvol_id
            }
        )
        item = cur.fetchone()
        self.stopTimer('get')
        return item

    def find(self, name):
        self.startTimer()
        cur = self.getCursor()

        digest = hashlib.new('md5', name).digest()

        cur.execute(
            "SELECT * FROM `%s` " % self.getName()+
            " WHERE `id`=%(hash)s",
            {
                "hash": digest
            }
        )
        item = cur.fetchone()
        self.stopTimer('find')
        return item

    def get_ids(self, order_by=None, order_dir="ASC"):
        self.startTimer()
        cur = self.getCursor()
        sql = "SELECT id FROM `%s`" % self.getName()
        if order_by:
            sql += " ORDER BY `%s` %s" % (order_by, order_dir,)
        cur.execute(sql)
        items = (item["id"] for item in cur.fetchall())
        self.stopTimer('get_ids')
        return items

    pass
