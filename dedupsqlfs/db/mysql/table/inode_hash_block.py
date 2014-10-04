# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table

class TableInodeHashBlock( Table ):

    _table_name = "inode_hash_block"

    _selected_subvol = None

    def create( self ):
        cur = self.getCursor()

        # Create table
        cur.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName()+
                "`subvol_id` INT UNSIGNED, "+
                "`inode_id` BIGINT UNSIGNED NOT NULL, "+
                "`block_number` BIGINT UNSIGNED NOT NULL, "+
                "`hash_id` BIGINT UNSIGNED NOT NULL, "+
                "`real_size` INT UNSIGNED NOT NULL, "+
                "`real_comp_size` INT UNSIGNED NOT NULL, "+
                "`writed_size` INT UNSIGNED NOT NULL, "+
                "`writed_comp_size` INT UNSIGNED NOT NULL "+
            ")"+
            self._getCreationAppendString()
        )

        self.createIndexIfNotExists("inode_block", ('inode_id', 'block_number',), unique=True)
        self.createIndexIfNotExists("subvol", ('subvol_id',))
        self.createIndexIfNotExists("hash", ('hash_id',))
        self.createIndexIfNotExists("inode", ('inode_id',))
        self.createIndexIfNotExists("hash_inode", ('hash_id', 'inode_id',))
        return

    def selectSubvolume(self, subvol_id):
        self._selected_subvol = subvol_id
        return self

    def getSelectedSubvolume(self):
        return self._selected_subvol

    def insert( self, inode, block_number, hash_id,
                real_size, real_comp_size, writed_size, writed_comp_size):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "INSERT INTO `%s` " % self.getName()+
            " (`inode_id`, `block_number`, `hash_id`, `real_size`, `comp_size`) "+
            " VALUES (%(inode)s, %(block)s, %(hash)s, %(real)s, %(real_comp)s, %(writed)s, %(writed_comp)s)",
            {
                "inode": inode,
                "block": block_number,
                "hash": hash_id,
                "real": real_size,
                "real_comp": real_comp_size,
                "writed": writed_size,
                "writed_comp": writed_comp_size
            }
        )
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update( self, inode, block_number, new_hash_id,
                new_real_size, new_real_comp_size, new_writed_size, new_writed_comp_size):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName()+
            " SET `hash_id`=%(hash)s, "+
            " `real_size`=%(real)s, `real_comp_size`=%(real_comp)s, "+
            " `writed_size`=%(writed)s, `writed_comp_size`=%(writed_comp)s "+
            " WHERE `inode_id`=%(inode)s AND `block_number`=%(block)s",
            {
                "hash": new_hash_id,
                "inode": inode,
                "block": block_number,
                "real": new_real_size,
                "real_comp": new_real_comp_size,
                "writed": new_writed_size,
                "writed_comp": new_writed_comp_size
            }
        )
        item = cur.rowcount
        self.stopTimer('update')
        return item

    def delete( self, inode ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "DELETE FROM `%s` " % self.getName()+
            " WHERE `inode_id`=%(inode)s",
            {
                "inode": inode
            }
        )
        count = cur.rowcount
        self.stopTimer('delete')
        return count

    def hash_by_inode_number( self, inode, block_number ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT `hash_id` FROM `%s` " % self.getName()+
            " WHERE `inode_id`=%(inode)s AND `block_number`=%(block)s",
            {
                "inode": inode,
                "block": block_number
            }
        )
        item = cur.fetchone()
        if item:
            item = item["hash_id"]
        self.stopTimer('hash_by_inode_number')
        return item

    def delete_by_inode_number( self, inode, block_number ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "DELETE FROM `%s` " % self.getName()+
            " WHERE `inode_id`=%(inode)s AND `block_number`=%(block)s",
            {
                "inode": inode,
                "block": block_number
            }
        )
        item = cur.rowcount
        self.stopTimer('delete_by_inode_number')
        return item

    def get_hashes_by_inode( self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT `hash_id` FROM `%s` " % self.getName()+
            " WHERE `inode_id`=%(inode)s",
            {
                "inode": inode
            }
        )
        items = tuple(str(item["hash_id"]) for item in cur.fetchall())
        self.stopTimer('get_hashes_by_inode')
        return items

    def get_hashes_by_subvol( self, subvol_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT DISTINCT `hash_id` FROM `%s` " % self.getName()+
            " WHERE `subvol_id`=%(subvol)s",
            {
                "subvol": subvol_id
            }
        )
        items = tuple(str(item["hash_id"]) for item in cur.fetchall())
        self.stopTimer('get_hashes_by_subvol')
        return items

    def get_by_inode( self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT * FROM `%s` " % self.getName()+
            " WHERE `inode_id`=%(inode)s",
            {
                "inode": inode
            }
        )
        self.stopTimer('get_by_inode')
        def fetchone():
            self.startTimer()
            item = cur.fetchone()
            self.stopTimer('get_by_inode')
            return item
        for row in iter(fetchone, None):
            yield row
        return

    def get_sum_sizes_by_subvol( self, subvol_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT SUM(`real_size`) as `real`, SUM(`real_comp_size`) as `real_comp`,"+
                    " SUM(`writed_size`) as `writed`, SUM(`writed_comp_size`) as `writed_comp` FROM `%s` " % self.getName()+
                    " WHERE subvol_id=%s", (subvol_id,))
        item = cur.fetchone()
        self.stopTimer('get_sum_sizes_by_subvol')
        return item

    def get_count_by_inode( self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT COUNT(1) as `cnt` FROM `%s` " % self.getName()+
                    " WHERE inode_id=%s", (inode,))
        item = cur.fetchone()
        self.stopTimer('get_count_by_inode')
        return item['cnt']

    def get_count_hash( self, hash_id ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT COUNT(1) as `cnt` FROM `%s` " % self.getName()+
            " WHERE `hash_id`=%(hash)s",
            {
                "hash": hash_id
            }
        )
        item = cur.fetchone()
        if item:
            item = item["cnt"]
        else:
            item = 0
        self.stopTimer('get_count_hash')
        return item

    def get_count_hash_by_inode( self, hash_id, inode_id ):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT COUNT(1) as `cnt` FROM `%s` " % self.getName()+
            " WHERE `hash_id`=%(hash)s AND `inode_id`=%(inode)s",
            {
                "hash": hash_id,
                "inode": inode_id
            }
        )
        item = cur.fetchone()
        if item:
            item = item["cnt"]
        else:
            item = 0
        self.stopTimer('get_count_hash_by_inode')
        return item

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

    def get_inode_ids(self, start_id, end_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute("SELECT DISTINCT `inode_id` FROM `%s` " % self.getName()+
                    " WHERE `inode_id`>=%s AND `inode_id`<%s", (start_id, end_id,))
        nameIds = tuple(str(item["inode_id"]) for item in cur)
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

    def get_hashes_by_hashes(self, hash_ids):
        self.startTimer()

        iids = ()
        id_str = ",".join(hash_ids)
        if id_str:
            cur = self.getCursor()
            cur.execute("SELECT DISTINCT `hash_id` FROM `%s` " % self.getName()+
                            " WHERE `hash_id` IN (%s)" % (id_str,))
            iids = tuple(str(item["hash_id"]) for item in cur)

        self.stopTimer('get_hashes_by_hashes')
        return iids

    pass
