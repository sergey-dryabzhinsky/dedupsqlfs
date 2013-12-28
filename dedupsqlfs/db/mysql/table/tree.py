# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableTree( Table ):

    _table_name = "tree"

    _selected_subvol = None

    def create( self ):
        cur = self.getCursor()

        # Create table
        cur.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "id BIGINT UNSIGNED PRIMARY KEY AUTOINCREMENT, "+
                "subvol_id BIGINT UNSIGNED, "+
                "parent_id BIGINT UNSIGNED, "+
                "name_id BIGINT UNSIGNED NOT NULL, "+
                "inode_id BIGINT UNSIGNED NOT NULL"+
            ");"
        )

        try:
            cur.execute(
                "ALTER TABLE %(table_name)s ADD UNIQUE INDEX %(index_name)s (subvol_id, parent_id, name_id)",
                {
                    "table_name": self.getName(),
                    "index_name": self.getName() + "_spn"
                }
            )
        except:
            pass

        try:
            cur.execute(
                "ALTER TABLE %(table_name)s ADD INDEX %(index_name)s (inode_id)",
                {
                    "table_name": self.getName(),
                    "index_name": self.getName() + "_inode"
                }
            )
        except:
            pass

        try:
            cur.execute(
                "ALTER TABLE %(table_name)s ADD INDEX %(index_name)s (parent_id)",
                {
                    "table_name": self.getName(),
                    "index_name": self.getName() + "_parent"
                }
            )
        except:
            pass

        try:
            cur.execute(
                "ALTER TABLE %(table_name)s ADD INDEX %(index_name)s (parent_id, name_id)",
                {
                    "table_name": self.getName(),
                    "index_name": self.getName() + "_parent_name"
                }
            )
        except:
            pass

        try:
            cur.execute(
                "ALTER TABLE %(table_name)s ADD INDEX %(index_name)s (subvol_id)",
                {
                    "table_name": self.getName(),
                    "index_name": self.getName() + "_subvol"
                }
            )
        except:
            pass

        try:
            cur.execute(
                "ALTER TABLE %(table_name)s ADD INDEX %(index_name)s (name_id)",
                {
                    "table_name": self.getName(),
                    "index_name": self.getName() + "_name"
                }
            )
        except:
            pass

        return

    def getRowSize(self):
        return 5 * 8

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
        cur.execute(
            "INSERT INTO %(table_name)s (subvol_id, parent_id, name_id, inode_id) "+
            "VALUES (%(subvol)s, %(parent)s, %(name)s, %(inode)s)",
            {
                "table_name": self.getName(),
                "subvol": self._selected_subvol,
                "parent": parent_id,
                "name": name_id,
                "inode": inode_id
            }
        )
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def delete(self, node_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "DELETE FROM %(table_name)s WHERE id=%(id)s",
            {
                "table_name": self.getName(),
                "id": node_id
            }
        )
        item = cur.rowcount
        self.stopTimer('delete')
        return item

    def delete_subvolume(self, subvol_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "DELETE FROM %(table_name)s WHERE subvol_id=%(subvol)s",
            {
                "table_name": self.getName(),
                "subvol": subvol_id
            }
        )
        item = cur.rowcount + self.delete(subvol_id)
        self.stopTimer('delete_subvolume')
        return item

    def count_subvolume_inodes(self, subvol_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT DISTINCT COUNT(inode_id) AS cnt FROM %(table_name)s WHERE subvol_id=%(subvol)s",
            {
                "table_name": self.getName(),
                "subvol": subvol_id
            }
        )
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
        cur.execute(
            "SELECT DISTINCT COUNT(name_id) AS cnt FROM %(table_name)s WHERE subvol_id=%(subvol)s",
            {
                "table_name": self.getName(),
                "subvol": subvol_id
            }
        )
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
        cur.execute(
            "SELECT COUNT(1) AS cnt FROM %(table_name)s WHERE subvol_id=%(subvol)s",
            {
                "table_name": self.getName(),
                "subvol": subvol_id
            }
        )
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
        cur.execute(
            "SELECT id, inode_id FROM %(table_name)s WHERE parent_id=%(parent)s AND name_id=%(name)s",
            {
                "table_name": self.getName(),
                "parent": parent_id,
                "name": name_id
            }
        )
        item = cur.fetchone()
        self.stopTimer('find_by_parent_name')
        return item

    def find_by_inode(self, inode_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT * FROM %(table_name)s WHERE inode_id=%(inode)s",
            {
                "table_name": self.getName(),
                "inode": inode_id
            }
        )
        item = cur.fetchone()
        self.stopTimer('find_by_inode')
        return item

    def get(self, node_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT * FROM %(table_name)s WHERE id=%(node)s",
            {
                "table_name": self.getName(),
                "node": node_id
            }
        )
        item = cur.fetchone()
        self.stopTimer('get')
        return item

    def get_children_inodes(self, parent_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT inode_id FROM %(table_name)s WHERE parent_id=%(parent)s",
            {
                "table_name": self.getName(),
                "parent": parent_id
            }
        )
        _items = cur.fetchall()
        items = ("%i" % _i["inode_id"] for _i in _items)
        self.stopTimer('get_children_inodes')
        return items

    def get_children(self, parent_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT * FROM %(table_name)s WHERE parent_id=%(parent)s",
            {
                "table_name": self.getName(),
                "parent": parent_id
            }
        )
        items = cur.fetchall()
        self.stopTimer('get_children')
        return items

    pass
