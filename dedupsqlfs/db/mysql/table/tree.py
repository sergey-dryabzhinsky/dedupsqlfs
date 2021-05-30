# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableTree( Table ):

    _table_name = "tree"

    def create( self ):
        cur = self.getCursor()

        # Create table
        cur.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "`id` BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT, "+
                "`parent_id` BIGINT UNSIGNED, "+
                "`name_id` BIGINT UNSIGNED NOT NULL, "+
                "`inode_id` BIGINT UNSIGNED NOT NULL"+
            ")"+
            self._getCreationAppendString()
        )

        self.createIndexIfNotExists("pn", ('parent_id', 'name_id',), unique=True)
        self.createIndexIfNotExists("inode", ('inode_id',))
        self.createIndexIfNotExists("parent_id", ('parent_id', 'id'))

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
        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            " (`parent_id`, `name_id`, `inode_id`) "+
            "VALUES (%(parent)s, %(name)s, %(inode)s)",
            {
                "parent": parent_id,
                "name": name_id,
                "inode": inode_id
            }
        )
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
                    "SET parent_id=%(parent)s, name_id=%(name)s WHERE id=%(id)s",
                    {
                        "parent": new_parent_id,
                        "name": new_name_id,
                        "id": node_id
                     })
        item = cur.lastrowid
        self.stopTimer('rename_inode')
        return item


    def delete(self, node_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "DELETE FROM `%s` " % self.getName()+
            " WHERE `id`=%(id)s",
            {
                "id": node_id
            }
        )
        item = cur.rowcount
        self.stopTimer('delete')
        return item

    def find_by_parent_name(self, parent_id, name_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT `id`, `inode_id`, `name_id` FROM `%s` " % self.getName()+
            " WHERE `parent_id`=%(parent)s AND `name_id`=%(name)s",
            {
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
            "SELECT * FROM `%s` " % self.getName()+
            " WHERE `inode_id`=%(inode)s",
            {
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
            "SELECT * FROM `%s` " % self.getName()+
            " WHERE `id`=%(node)s",
            {
                "node": node_id
            }
        )
        item = cur.fetchone()
        cur.close()
        self.stopTimer('get')
        return item

    def get_children_inodes(self, parent_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT `inode_id` FROM `%s` " % self.getName()+
            " WHERE `parent_id`=%(parent)s ORDER BY `id` ASC",
            {
                "parent": parent_id
            }
        )
        _items = cur.fetchall()
        items = ("%i" % _i["inode_id"] for _i in _items)
        self.stopTimer('get_children_inodes')
        return items

    def get_children(self, parent_id, offset=0):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT `id`,`name_id`,`inode_id` FROM `%s` " % self.getName()+
            " WHERE `parent_id`=%(parent)s AND `id`>%(offset)s ORDER BY `id` ASC",
            {
                "parent": parent_id,
                "offset": offset
            }
        )
        items = (item for item in cur)
        self.stopTimer('get_children')
        return items

    def get_inodes_by_inodes(self, inode_ids):
        self.startTimer()

        iids = ()
        id_str = ",".join(inode_ids)
        if id_str:
            cur = self.getCursor()
            cur.execute("SELECT `inode_id` FROM `%s` " % self.getName()+
                            " WHERE `inode_id` IN (%s)" % (id_str,))
            iids = set(str(item["inode_id"]) for item in cur)

        self.stopTimer('get_inodes_by_inodes')
        return iids

    def get_inodes_by_inodes_intgen(self, id_str):
        self.startTimer()

        iids = ()
        if id_str:
            cur = self.getCursor()
            cur.execute("SELECT `inode_id` FROM `%s` " % self.getName()+
                            " WHERE `inode_id` IN (%s)" % (id_str,))
            iids = (item["inode_id"] for item in cur)

        self.stopTimer('get_inodes_by_inodes_intgen')
        return iids

    def get_all_inodes_set(self):
        self.startTimer()

        cur = self.getCursor()
        cur.execute("SELECT `inode_id` FROM `%s` " % self.getName())
        iids = set(item["inode_id"] for item in cur)

        self.stopTimer('get_all_inodes_set')
        return iids

    pass
