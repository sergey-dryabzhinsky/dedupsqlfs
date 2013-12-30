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
                "`node_id` BIGINT UNSIGNED PRIMARY KEY, "+
                "`created_at` INT UNSIGNED NOT NULL, "+
                "`mounted_at` INT UNSIGNED, "+
                "`updated_at` INT UNSIGNED"+
            ")"+
            self._getCreationAppendString()
        )
        return

    def insert( self, node_id, created_at, mounted_at=None, updated_at=None ):
        """
        :param node_id: int         - tree node
        :param name_id: int         - node name
        :param created_at: int      - creation time
        :param mounted_at: int|None - subvolume mounted
        :param updated_at: int|None - subvolume updated
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            " (`node_id`, `created_at`, `mounted_at`, `updated_at`) "+
            "VALUES (%(node)s, %(created)s, %(mounted)s, %(updated)s)",
            {
                "node": node_id,
                "created": created_at,
                "mounted": mounted_at,
                "updated": updated_at
            }
        )
        self.stopTimer('insert')
        return node_id

    def mount_time(self, node_id, mtime=None):
        self.startTimer()
        if mtime is None:
            mtime = int(time())
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `mounted_at`=%(mounted)s WHERE `node_id`=%(node)s",
            {
                "mounted": mtime,
                "node": node_id
            }
        )
        self.stopTimer('mount_time')
        return cur.rowcount

    def update_time(self, node_id, utime=None):
        self.startTimer()
        if utime is None:
            utime = int(time())
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `updated_at`=%(updated)s WHERE `node_id`=%(node)s",
            {
                "updated": utime,
                "node": node_id
            }
        )
        self.stopTimer('update_time')
        return cur.rowcount

    def delete(self, node_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "DELETE FROM `%s` " % self.getName()+
            " WHERE `node_id`=%(node)s",
            {
                "node": node_id
            }
        )
        item = cur.rowcount
        self.stopTimer('delete')
        return item

    def get(self, node_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT * FROM `%s` " % self.getName()+
            " WHERE `node_id`=%(node)s",
            {
                "node": node_id
            }
        )
        item = cur.fetchone()
        self.stopTimer('get')
        return item

    pass
