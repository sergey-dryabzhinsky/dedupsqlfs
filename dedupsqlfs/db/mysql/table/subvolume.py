# -*- coding: utf8 -*-

__author__ = 'sergey'

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
                "`readonly` TINYINT UNSIGNED NOT NULL DEFAULT 0, "+
                "`created_at` INT UNSIGNED NOT NULL, "+
                "`mounted_at` INT UNSIGNED, "+
                "`updated_at` INT UNSIGNED"+
            ")"+
            self._getCreationAppendString()
        )
        return

    def insert( self, created_at, mounted_at=None, updated_at=None ):
        """
        :param created_at: int      - creation time
        :param mounted_at: int|None - subvolume mounted
        :param updated_at: int|None - subvolume updated
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            " (`created_at`, `mounted_at`, `updated_at`) "+
            "VALUES (%(created)s, %(mounted)s, %(updated)s)",
            {
                "created": created_at,
                "mounted": mounted_at,
                "updated": updated_at
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
            mtime = int(time())
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `mounted_at`=%(mounted)s WHERE `id`=%(id)s",
            {
                "mounted": mtime,
                "id": subvol_id
            }
        )
        self.stopTimer('mount_time')
        return cur.rowcount

    def update_time(self, subvol_id, utime=None):
        self.startTimer()
        if utime is None:
            utime = int(time())
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `updated_at`=%(updated)s WHERE `id`=%(id)s",
            {
                "updated": utime,
                "id": subvol_id
            }
        )
        self.stopTimer('update_time')
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

    pass
