# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.sqlite.table import Table

class TableTree( Table ):

    _table_name = "tree"

    _selected_subvol = None

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "+
                "subvol_id INTEGER, "+
                "parent_id INTEGER, "+
                "name_id INTEGER NOT NULL, "+
                "inode_id INTEGER NOT NULL, "+
                "UNIQUE (subvol_id, parent_id, name_id)"+
            ");"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS tree_inode ON `%s` (" % self.getName()+
                "inode_id"+
            ");"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS tree_parent ON `%s` (" % self.getName()+
                "parent_id"+
            ");"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS tree_parent_name ON `%s` (" % self.getName()+
                "parent_id,name_id"+
            ");"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS tree_subvol ON `%s` (" % self.getName()+
                "subvol_id"+
            ");"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS tree_name ON `%s` (" % self.getName()+
                "name_id"+
            ");"
        )
        return

    def getRowSize(self):
        return 5 * 13

    def selectSubvolume(self, node_id):
        self._selected_subvol = node_id
        return self

    def getSelectedSubvolume(self):
        return self._selected_subvol

    def insert( self, parent_id, name_id, inode_id ):
        """
        :param parent_id: int|None
        :param name_id: int
        :param inode_id: int
        :return: int
        """
        self.startTimer()
        cur = self.getCursor()
        cur.execute("INSERT INTO `%s`(subvol_id, parent_id, name_id, inode_id) " % self.getName()+
                    "VALUES (?, ?, ?, ?)", (self._selected_subvol, parent_id, name_id, inode_id))
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def delete(self, node_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("DELETE FROM `%s` WHERE id=?" % self.getName(), (node_id,))
        item = cur.rowcount
        self.stopTimer('delete')
        return item

    def delete_subvolume(self, subvol_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("DELETE FROM `%s` WHERE subvol_id=?" % self.getName(), (subvol_id,))
        item = cur.rowcount + self.delete(subvol_id)
        self.stopTimer('delete_subvolume')
        return item

    def count_subvolume_inodes(self, subvol_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT DISTINCT COUNT(inode_id) AS cnt FROM `%s` WHERE subvol_id=?" % self.getName(), (subvol_id,))
        item = cur.fetchone()
        if item:
            item = item["cnt"]
        else:
            item = 0
        self.stopTimer('count_subvolume_inodes')
        return item

    def count_subvolume_names(self, subvol_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT DISTINCT COUNT(name_id) AS cnt FROM `%s` WHERE subvol_id=?" % self.getName(), (subvol_id,))
        item = cur.fetchone()
        if item:
            item = item["cnt"]
        else:
            item = 0
        self.stopTimer('count_subvolume_names')
        return item

    def count_subvolume_nodes(self, subvol_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT COUNT(1) AS cnt FROM `%s` WHERE subvol_id=?" % self.getName(), (subvol_id,))
        item = cur.fetchone()
        if item:
            item = item["cnt"]
        else:
            item = 0
        self.stopTimer('count_subvolume_nodes')
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
        cur.execute("SELECT inode_id FROM `%s` WHERE parent_id=?" % self.getName(), (parent_id, ))
        _items = cur.fetchall()
        items = ("%i" % _i["inode_id"] for _i in _items)
        self.stopTimer('get_children_inodes')
        return items

    def get_children(self, parent_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE parent_id=?" % self.getName(), (parent_id, ))
        items = cur.fetchall()
        self.stopTimer('get_children')
        return items


    def getCursorForSelectInodes(self):
        cursor = self.getCursor()
        cursor.execute("SELECT `inode_id` FROM `%s` " % self.getName())
        return cursor

    def getCursorForSelectNodeInodes(self, node_id):
        cursor = self.getCursor()
        cursor.execute("SELECT `inode_id` FROM `%s` WHERE `subvol_id`=?" % self.getName(), (node_id,))
        return cursor

    def getCursorForSelectCurrentSubvolInodes(self):
        cursor = self.getCursor()
        cursor.execute(
            "SELECT `inode_id` FROM `%s` " % self.getName()+
            " WHERE `subvol_id`=? OR `id`=?",
            (self.getSelectedSubvolume(), self.getSelectedSubvolume(),)
        )
        return cursor

    def get_names_by_names(self, name_ids):
        self.startTimer()

        nids = ()
        id_str = ",".join(name_ids)
        if id_str:
            cur = self.getCursor()
            cur.execute("SELECT `name_id` FROM `%s` " % self.getName()+
                            " WHERE `name_id` IN (%s)" % (id_str,))
            nids = tuple(str(item["name_id"]) for item in iter(cur.fetchone(),None))

        self.stopTimer('get_names_by_names')
        return nids

    pass
