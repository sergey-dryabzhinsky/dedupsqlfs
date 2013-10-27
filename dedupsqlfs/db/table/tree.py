# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.table import Table

class TableTree( Table ):

    _table_name = "tree"

    _selected_subvol = None

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "+
                "subvol_id INTEGER, "+
                "parent_id INTEGER, "+
                "name_id INTEGER NOT NULL, "+
                "inode_id INTEGER NOT NULL, "+
                "UNIQUE (subvol_id, parent_id, name_id)"+
            ");"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS tree_inode ON `%s` (" % self._table_name+
                "inode_id"+
            ");"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS tree_parent ON `%s` (" % self._table_name+
                "parent_id"+
            ");"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS tree_subvol ON `%s` (" % self._table_name+
                "subvol_id"+
            ");"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS tree_name ON `%s` (" % self._table_name+
                "name_id"+
            ");"
        )
        return

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
        cur = self.getCursor()
        cur.execute("INSERT INTO `%s`(subvol_id, parent_id, name_id, inode_id) " % self._table_name+
                    "VALUES (?, ?, ?, ?)", (self._selected_subvol, parent_id, name_id, inode_id))
        item = cur.lastrowid
        self.commit()
        return item

    def delete(self, node_id):
        cur = self.getCursor()
        cur.execute("DELETE FROM `%s` WHERE id=?" % self._table_name, (node_id,))
        item = cur.rowcount
        return item

    def delete_subvolume(self, subvol_id):
        cur = self.getCursor()
        cur.execute("DELETE FROM `%s` WHERE subvol_id=?" % self._table_name, (subvol_id,))
        item = cur.rowcount + self.delete(subvol_id)
        return item

    def find_by_parent_name(self, parent_id, name_id):
        cur = self.getCursor()
        cur.execute("SELECT id, inode_id FROM `%s` WHERE parent_id=? AND name_id=?" % self._table_name, (parent_id, name_id,))
        item = cur.fetchone()
        return item

    def find_by_inode(self, inode_id):
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE inode_id=?" % self._table_name, (inode_id, ))
        item = cur.fetchone()
        return item

    def get(self, node_id):
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE id=?" % self._table_name, (node_id,))
        item = cur.fetchone()
        return item

    def get_children_inodes(self, parent_id):
        cur = self.getCursor()
        cur.execute("SELECT inode_id FROM `%s` WHERE parent_id=?" % self._table_name, (parent_id, ))
        _items = cur.fetchall()
        items = ("%i" % _i["inode_id"] for _i in _items)
        return items

    def get_children(self, parent_id):
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE parent_id=?" % self._table_name, (parent_id, ))
        items = cur.fetchall()
        return items

    def fetch(self, limit=None, offset=None):
        cur = self.getCursor()

        query = "SELECT * FROM `%s` WHERE subvol_id=?" % self._table_name
        if limit is not None:
            query += " LIMIT %d" % limit
            if offset is not None:
                query += " OFFSET %d" % offset

        cur.execute(query, (self._selected_subvol, ))
        items = cur.fetchall()
        return items

    pass
