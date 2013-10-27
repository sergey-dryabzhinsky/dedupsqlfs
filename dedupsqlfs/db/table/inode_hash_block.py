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
                "compression_type_id INTEGER NOT NULL, "+
                "block_size INTEGER NOT NULL, "+
                "PRIMARY KEY (inode_id, block_number)"+
            ")"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS ib_hash ON `%s` (" % self._table_name+
                "hash_id"+
            ");"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS b_compression_type ON `%s` (" % self._table_name+
                "compression_type_id"+
            ");"
        )
        return

    def insert( self, inode, block_number, hash_id, compression_type_id, block_size ):
        cur = self.getCursor()
        cur.execute("INSERT INTO `%s`(inode_id, block_number, hash_id, compression_type_id, block_size) VALUES (?,?,?,?,?)" % self._table_name,
                    (inode, block_number, hash_id, compression_type_id, block_size))
        item = cur.lastrowid
        self.commit()
        return item

    def update( self, inode, block_number, new_hash_id, compression_type_id, new_block_size ):
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET hash_id=?, compression_type_id=?, block_size=? WHERE inode_id=? AND block_number=?" % self._table_name,
                    (new_hash_id, compression_type_id, new_block_size, inode, block_number,))
        item = cur.rowcount
        self.commit()
        return item

    def delete( self, inode ):
        cur = self.getCursor()
        cur.execute("DELETE FROM `%s` WHERE inode_id=?" % self._table_name, (inode,))
        count = cur.rowcount
        self.commit()
        return count

    def count_compression_type( self ):
        cur = self.getCursor()
        cur.execute("SELECT COUNT(compression_type_id) AS cnt,compression_type_id FROM `%s` GROUP BY compression_type_id" % self._table_name)
        items = cur.fetchall()
        return items

    def get_by_inode_number( self, inode, block_number ):
        cur = self.getCursor()
        cur.execute("SELECT * FROM `%s` WHERE inode_id=? AND block_number=?" % self._table_name, (inode, block_number,))
        item = cur.fetchone()
        return item

    def get_hashes_by_inode( self, inode):
        cur = self.getCursor()
        cur.execute("SELECT hash_id,block_size FROM `%s` WHERE inode_id=? GROUP BY hash_id" % self._table_name, (inode,))
        item = cur.fetchall()
        return item

    def get_count_hash( self, hash_id ):
        cur = self.getCursor()
        cur.execute("SELECT COUNT(1) as cnt FROM `%s` WHERE hash_id=?" % self._table_name, (hash_id,))
        item = cur.fetchone()
        if item:
            item = item["cnt"]
        else:
            item = 0
        return item

    def get_hashes_count( self ):
        cur = self.getCursor()
        cur.execute("SELECT COUNT(hash_id) as cnt FROM `%s`" % self._table_name)
        item = cur.fetchone()
        if item:
            item = item["cnt"]
        else:
            item = 0
        return item

    pass
