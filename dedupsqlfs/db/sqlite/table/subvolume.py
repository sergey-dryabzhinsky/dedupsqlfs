# -*- coding: utf8 -*-

__author__ = 'sergey'

import hashlib
import sqlite3
from time import time
from dedupsqlfs.db.sqlite.table import Table

class TableSubvolume( Table ):

    _table_name = "subvolume"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "+
                "`hash` CHAR(32) NOT NULL, "+
                "`name` BLOB NOT NULL, "+
                "`stats` TEXT, "+
                "`readonly` TINYINT UNSIGNED NOT NULL DEFAULT 0, "+
                "stats_at INTEGER, "+
                "created_at INTEGER NOT NULL, "+
                "mounted_at INTEGER, "+
                "updated_at INTEGER"
            ");"
        )
        self.createIndexIfNotExists("hash", ('hash',), unique=True)
        return

    def insert( self, name, created_at, mounted_at=None, updated_at=None ):
        """
        :param name: str            - subvolume/snapshot name
        :param created_at: int      - creation time
        :param mounted_at: int|None - subvolume mounted
        :param updated_at: int|None - subvolume updated
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()

        context = hashlib.new('md5')
        context.update(name)
        digest = context.hexdigest()

        bname = sqlite3.Binary(name)

        cur.execute("INSERT INTO `%s`(hash, name, created_at, mounted_at, updated_at) " % self.getName()+
                    "VALUES (?, ?, ?, ?, ?)", (digest, bname, int(created_at), mounted_at, updated_at))
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
            " SET `readonly`=? WHERE `id`=?",
            (flag, subvol_id)
        )
        self.stopTimer('readonly')
        return cur.rowcount

    def mount_time(self, subvol_id, mtime=None):
        self.startTimer()
        if mtime is None:
            mtime = time()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET mounted_at=? WHERE id=? " % self.getName(),
                    (int(mtime), subvol_id,))
        self.stopTimer('mount_time')
        return cur.rowcount

    def update_time(self, subvol_id, utime=None):
        self.startTimer()
        if utime is None:
            utime = time()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET updated_at=? WHERE id=? " % self.getName(),
                    (int(utime), subvol_id,))
        self.stopTimer('update_time')
        return cur.rowcount

    def delete(self, subvol_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("DELETE FROM `%s` WHERE id=?" % self.getName(), (subvol_id,))
        item = cur.rowcount
        self.stopTimer('delete')
        return item

    def stats_time(self, subvol_id, stime=None):
        self.startTimer()
        if stime is None:
            stime = time()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET stats_at=? WHERE id=? " % self.getName(),
                    (int(stime), subvol_id,))
        self.stopTimer('stats_time')
        return cur.rowcount

    def set_stats(self, subvol_id, stats):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET stats=? WHERE id=? " % self.getName(),
                    (stats, subvol_id,))
        self.stopTimer('set_stats')
        return cur.rowcount

    def get(self, subvol_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE id=?" % self.getName(), (subvol_id,))
        item = cur.fetchone()
        if item:
            item['hash'] = item['hash'].decode()
        self.stopTimer('get')
        return item

    def find(self, name):
        self.startTimer()
        cur = self.getCursor()

        context = hashlib.new('md5')
        context.update(name)
        digest = context.hexdigest()

        cur.execute(
            "SELECT * FROM `%s` " % self.getName()+
            " WHERE `hash`=?", (digest,)
        )
        item = cur.fetchone()
        if item:
            item['hash'] = item['hash'].decode()
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
