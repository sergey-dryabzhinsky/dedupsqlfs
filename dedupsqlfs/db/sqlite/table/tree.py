# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.sqlite.table import Table

class TableTree( Table ):

    _table_name = "tree"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "+
                "parent_id INTEGER, "+
                "name_id INTEGER NOT NULL, "+
                "inode_id INTEGER NOT NULL, "+
                "UNIQUE (parent_id, name_id)"+
            ");"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS tree_inode ON `%s` (" % self.getName()+
                "inode_id"+
            ");"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS tree_parent_id ON `%s` (" % self.getName()+
                "parent_id,id"+
            ");"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS tree_name ON `%s` (" % self.getName()+
                "name_id"+
            ");"
        )
        return

    def getRowSize(self):
        return 4 * 8

    def insert( self, parent_id, name_id, inode_id ):
        """
        :param parent_id: int|None
        :param name_id: int
        :param inode_id: int
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute("INSERT INTO `%s`(parent_id, name_id, inode_id) " % self.getName()+
                    "VALUES (?, ?, ?)", (parent_id, name_id, inode_id))
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def rename_inode( self, node_id, new_parent_id, new_name_id):
        """
        :param parent_id: int|None
        :param name_id: int
        :param inode_id: int
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` " % self.getName()+
                    "SET parent_id=?, name_id=? WHERE id=?", (new_parent_id, new_name_id, node_id))
        item = cur.lastrowid
        self.stopTimer('rename_inode')
        return item

    def delete(self, node_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("DELETE FROM `%s` WHERE id=?" % self.getName(), (node_id,))
        item = cur.rowcount
        self.stopTimer('delete')
        return item

    def find_by_parent_name(self, parent_id, name_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT id, inode_id FROM `%s` WHERE parent_id=? AND name_id=?" % self.getName(), (parent_id, name_id,))
        item = cur.fetchone()
        self.stopTimer('find_by_parent_name')
        return item

    def find_by_inode(self, inode_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE inode_id=?" % self.getName(), (inode_id, ))
        item = cur.fetchone()
        self.stopTimer('find_by_inode')
        return item

    def get(self, node_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE id=?" % self.getName(), (node_id,))
        item = cur.fetchone()
        self.stopTimer('get')
        return item

    def get_children_inodes(self, parent_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT inode_id FROM `%s` WHERE parent_id=? ORDER BY `id` ASC" % self.getName(), (parent_id, ))
        items = (str(_i["inode_id"]) for _i in iter(cur.fetchone, None))
        self.stopTimer('get_children_inodes')
        return items

    def get_children(self, parent_id, offset=0):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE `parent_id`=? AND `id`>? ORDER BY `id` ASC" % self.getName(),
                    (parent_id, offset, ))
        items = cur.fetchall()
        self.stopTimer('get_children')
        return items

    def get_inodes_by_inodes(self, inode_ids):
        self.startTimer()

        iids = set()
        id_str = ",".join(inode_ids)
        if id_str:
            cur = self.getCursor()
            cur.execute("SELECT `inode_id` FROM `%s` " % self.getName()+
                            " WHERE `inode_id` IN (%s)" % (id_str,))
            iids = set(str(item["inode_id"]) for item in iter(cur.fetchone,None))

        self.stopTimer('get_inodes_by_inodes')
        return iids

    def get_inodes_by_inodes_intgen(self, id_str):
        self.startTimer()

        iids = ()
        if id_str:
            cur = self.getCursor()
            cur.execute("SELECT `inode_id` FROM `%s` " % self.getName()+
                            " WHERE `inode_id` IN (%s)" % (id_str,))
            iids = (item["inode_id"] for item in iter(cur.fetchone,None))

        self.stopTimer('get_inodes_by_inodes_intgen')
        return iids

    def get_all_inodes_set(self):
        self.startTimer()

        cur = self.getCursor()
        cur.execute("SELECT `inode_id` FROM `%s` " % self.getName())
        iids = set(item["inode_id"] for item in iter(cur.fetchone,None))

        self.stopTimer('get_all_inodes_set')
        return iids

    pass
