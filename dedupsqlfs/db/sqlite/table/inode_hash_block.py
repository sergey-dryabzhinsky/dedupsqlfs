# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.sqlite.table import Table

class TableInodeHashBlock( Table ):

    _table_name = "inode_hash_block"

    def create( self ):
        c = self.getCursor()

        # Create table
        c.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "inode_id INTEGER NOT NULL, "+
                "block_number INTEGER NOT NULL, "+
                "hash_id INTEGER NOT NULL, "+
                "real_size INTEGER NOT NULL DEFAULT 0, "+
                "PRIMARY KEY (inode_id, block_number)"+
            ")"
        )

        self.createIndexIfNotExists('hash', ("hash_id",))
        return

    def insert( self, inode, block_number, hash_id, real_size=0):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("INSERT INTO `%s`(inode_id, block_number, hash_id, real_size) VALUES (?,?,?,?)" % self.getName(),
                    (inode, block_number, hash_id, real_size,))
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update( self, inode, block_number, new_hash_id, new_size):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET hash_id=?, real_size=? WHERE inode_id=? AND block_number=?" % self.getName(),
                    (new_hash_id, new_size, inode, block_number,))
        item = cur.rowcount
        self.stopTimer('update')
        return item

    def update_hash( self, inode, block_number, new_hash_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET hash_id=? WHERE inode_id=? AND block_number=?" % self.getName(),
                    (new_hash_id, inode, block_number,))
        item = cur.rowcount
        self.stopTimer('update_hash')
        return item

    def update_size( self, inode, block_number, new_size):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("UPDATE `%s` SET real_size=? WHERE inode_id=? AND block_number=?" % self.getName(),
                    (new_size, inode, block_number,))
        item = cur.rowcount
        self.stopTimer('update_size')
        return item

    def delete( self, inode ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("DELETE FROM `%s` WHERE inode_id=?" % self.getName(), (inode,))
        count = cur.rowcount
        self.stopTimer('delete')
        return count

    def get( self, inode, block_number ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT `hash_id`,`real_size` FROM `%s` " % self.getName()+
            " WHERE `inode_id`=? AND `block_number`=?",
            (inode, block_number,)
        )
        item = cur.fetchone()
        self.stopTimer('get')
        return item

    def hash_by_inode_number( self, inode, block_number ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT `hash_id` FROM `%s` " % self.getName()+
            " WHERE `inode_id`=? AND `block_number`=?",
            (inode, block_number,)
        )
        item = cur.fetchone()
        if item:
            item = item["hash_id"]
        self.stopTimer('hash_by_inode_number')
        return item

    def delete_by_inode_number( self, inode, block_number ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("DELETE FROM `%s` WHERE inode_id=? AND block_number=?" % self.getName(), (inode, block_number,))
        item = cur.rowcount
        self.stopTimer('delete_by_inode_number')
        return item

    def delete_by_inode_number_more( self, inode, block_number ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT block_number FROM `%s` WHERE inode_id=? AND block_number>?" % self.getName(), (inode, block_number,))
        items = cur.fetchall()
        if items:
            cur.execute("DELETE FROM `%s` WHERE inode_id=? AND block_number>?" % self.getName(), (inode, block_number,))
        self.stopTimer('delete_by_inode_number_more')
        return items

    def get_count_uniq_inodes(self):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT COUNT(DISTINCT `inode_id`) as `cnt` FROM `%s`" % self.getName())
        item = cur.fetchone()
        if item:
            item = item["cnt"]
        else:
            item = 0
        self.stopTimer('get_count_uniq_inodes')
        return item

    def get_uniq_hashes(self):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT `hash_id` FROM `%s`" % self.getName())
        hashes = set(int(item['hash_id']) for item in iter(cur.fetchone,None))
        self.stopTimer('get_uniq_hashes')
        return hashes

    def count_hashes_by_hashes(self, hash_ids):
        self.startTimer()
        count = 0
        id_str = ",".join(str(hid) for hid in hash_ids)
        if id_str:
            cur = self.getCursor()
            cur.execute("SELECT COUNT(1) as `cnt` FROM `%s` " % self.getName()+
                        " WHERE `hash_id` IN (%s)" % (id_str,))
            item = cur.fetchone()
            if item:
                count = item["cnt"]
        self.stopTimer('count_hashes_by_hashes')
        return count

    def count_realsize_by_hashes(self, hash_ids):
        self.startTimer()
        count = 0
        id_str = ",".join(str(hid) for hid in hash_ids)
        if id_str:
            cur = self.getCursor()
            cur.execute("SELECT SUM(`real_size`) as `srs` FROM `%s` " % self.getName()+
                        " WHERE `hash_id` IN (%s)" % (id_str,))
            item = cur.fetchone()
            if item and item["srs"]:
                count = int(item["srs"])
        self.stopTimer('count_realsize_by_hashes')
        return count

    def get_inode_ids(self, start_id, end_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT DISTINCT `inode_id` FROM `%s` " % self.getName()+
                    " WHERE `inode_id`>=? AND `inode_id`<?", (start_id, end_id,))
        nameIds = set(str(item["inode_id"]) for item in iter(cur.fetchone,None))
        self.stopTimer('get_inode_ids')
        return nameIds

    def remove_by_inodes(self, inode_ids):
        self.startTimer()
        count = 0
        id_str = ",".join(inode_ids)
        if id_str:
            cur = self.getCursor()
            cur.execute("DELETE FROM `%s` " % self.getName()+
                        " WHERE `inode_id` IN (%s)" % (id_str,))
            count = cur.rowcount
        self.stopTimer('remove_by_inodes')
        return count

    def get_hashid_by_inodes(self, inode_ids):
        self.startTimer()
        hashes = ()
        id_str = ",".join(inode_ids)
        if id_str:
            cur = self.getCursor()
            cur.execute("SELECT `hash_id` FROM `%s` " % self.getName()+
                        " WHERE `inode_id` IN (%s)" % (id_str,))
            hashes = (item["hash_id"] for item in iter(cur.fetchone,None))
        self.stopTimer('get_hashid_by_inodes')
        return hashes

    def get_hash_inode_ids(self):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT `hash_id`,`inode_id` FROM `%s` " % self.getName())
        iids = (item for item in iter(cur.fetchone,None))
        self.stopTimer('get_hash_inode_ids')
        return iids

    pass
