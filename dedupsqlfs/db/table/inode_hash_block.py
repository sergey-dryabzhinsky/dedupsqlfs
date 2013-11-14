# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.table import Table

class TableInodeHashBlock( Table ):

    _table_name = "inode_hash_block"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self._table_name+
                "inode_id INTEGER NOT NULL, "+
                "block_number INTEGER NOT NULL, "+
                "hash_id INTEGER NOT NULL, "+
                "PRIMARY KEY (inode_id, block_number)"+
            ")"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS ihb_hash ON `%s` (" % self._table_name+
                "hash_id"+
            ");"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS ihb_inode ON `%s` (" % self._table_name+
                "inode_id"+
            ");"
        )
        return

    def insert( self, inode, block_number, hash_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("INSERT INTO `%s`(inode_id, block_number, hash_id) VALUES (?,?,?)" % self._table_name,
                    (inode, block_number, hash_id))
        item = cur.lastrowid
        self.commit()
        self.stopTimer()
        return item

    def update( self, inode, block_number, new_hash_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET hash_id=? WHERE inode_id=? AND block_number=?" % self._table_name,
                    (new_hash_id, inode, block_number,))
        item = cur.rowcount
        self.commit()
        self.stopTimer()
        return item

    def delete( self, inode ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("DELETE FROM `%s` WHERE inode_id=?" % self._table_name, (inode,))
        count = cur.rowcount
        self.commit()
        self.stopTimer()
        return count

    def get_by_inode_number( self, inode, block_number ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE inode_id=? AND block_number=?" % self._table_name, (inode, block_number,))
        item = cur.fetchone()
        self.stopTimer()
        return item

    def delete_by_inode_number( self, inode, block_number ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("DELETE FROM `%s` WHERE inode_id=? AND block_number=?" % self._table_name, (inode, block_number,))
        item = cur.rowcount
        self.stopTimer()
        return item

    def get_hashes_by_inode( self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT hash_id FROM `%s` WHERE inode_id=? GROUP BY hash_id" % self._table_name, (inode,))
        items = cur.fetchall()
        self.stopTimer()
        return items

    def get_by_inode( self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE inode_id=?" % self._table_name, (inode,))
        items = cur.fetchall()
        self.stopTimer()
        return items

    def get_count_hash( self, hash_id ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT COUNT(1) as cnt FROM `%s` WHERE hash_id=?" % self._table_name, (hash_id,))
        item = cur.fetchone()
        if item:
            item = item["cnt"]
        else:
            item = 0
        self.stopTimer()
        return item

    pass
