# -*- coding: utf8 -*-

__author__ = 'sergey'

from dedupsqlfs.db.mysql.table import Table


class TableInodeHashBlock(Table):

    _table_name = "inode_hash_block"

    _selected_subvol = None

    def create(self):
        cur = self.getCursor()

        # Create table
        cur.execute(
            "CREATE TABLE IF NOT EXISTS `%s` (" % self.getName() +
            "`inode_id` BIGINT UNSIGNED NOT NULL, " +
            "`block_number` BIGINT UNSIGNED NOT NULL, " +
            "`hash_id` BIGINT UNSIGNED NOT NULL, " +
            "`real_size` INT UNSIGNED NOT NULL DEFAULT 0" +
            ")" +
            self._getCreationAppendString()
        )

        self.createIndexIfNotExists("inode_block", ('inode_id', 'block_number',), unique=True)
        self.createIndexIfNotExists("hash", ('hash_id',))
        return

    def insert(self, inode, block_number, hash_id, real_size=0):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "INSERT INTO `%s` " % self.getName() +
            " (`inode_id`,`block_number`,`hash_id`,`real_size`) " +
            " VALUES (%(inode)s, %(block)s, %(hash)s, %(size)s)",
            {
                "inode": inode,
                "block": block_number,
                "hash": hash_id,
                "size": real_size
            }
        )
        item = cur.lastrowid
        self.stopTimer('insert')
        return item

    def update(self, inode, block_number, new_hash_id, new_size):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName() +
            " SET `hash_id`=%(hash)s, `real_size`=%(size)s " +
            " WHERE `inode_id`=%(inode)s AND `block_number`=%(block)s",
            {
                "hash": new_hash_id,
                "size": new_size,
                "inode": inode,
                "block": block_number
            }
        )
        item = cur.rowcount
        self.stopTimer('update')
        return item

    def update_hash(self, inode, block_number, new_hash_id):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName() +
            " SET `hash_id`=%(hash)s " +
            " WHERE `inode_id`=%(inode)s AND `block_number`=%(block)s",
            {
                "hash": new_hash_id,
                "inode": inode,
                "block": block_number
            }
        )
        item = cur.rowcount
        self.stopTimer('update')
        return item

    def update_size(self, inode, block_number, new_size):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "UPDATE `%s` " % self.getName() +
            " SET `real_size`=%(size)s " +
            " WHERE `inode_id`=%(inode)s AND `block_number`=%(block)s",
            {
                "size": new_size,
                "inode": inode,
                "block": block_number
            }
        )
        item = cur.rowcount
        self.stopTimer('update_size')
        return item

    def delete(self, inode):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "DELETE FROM `%s` " % self.getName() +
            " WHERE `inode_id`=%(inode)s",
            {
                "inode": inode
            }
        )
        count = cur.rowcount
        self.stopTimer('delete')
        return count

    def get(self, inode, block_number):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT `hash_id`,`real_size` FROM `%s` " % self.getName() +
            " WHERE `inode_id`=%(inode)s AND `block_number`=%(block)s",
            {
                "inode": inode,
                "block": block_number
            }
        )
        item = cur.fetchone()
        self.stopTimer('get')
        return item

    def hash_by_inode_number(self, inode, block_number):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT `hash_id` FROM `%s` " % self.getName() +
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

    def delete_by_inode_number(self, inode, block_number):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "DELETE FROM `%s` " % self.getName() +
            " WHERE `inode_id`=%(inode)s AND `block_number`=%(block)s",
            {
                "inode": inode,
                "block": block_number
            }
        )
        item = cur.rowcount
        self.stopTimer('delete_by_inode_number')
        return item

    def delete_by_inode_number_more(self, inode, block_number):
        self.startTimer()
        cur = self.getCursor()
        cur.execute(
            "SELECT block_number FROM `%s` " % self.getName() +
            " WHERE `inode_id`=%(inode)s AND `block_number`>%(block)s",
            {
                "inode": inode,
                "block": block_number
            }
        )
        items = cur.fetchall()
        if items:
            cur.execute(
                "DELETE FROM `%s` " % self.getName() +
                " WHERE `inode_id`=%(inode)s AND `block_number`>%(block)s",
                {
                    "inode": inode,
                    "block": block_number
                }
            )
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
        hashes = set(int(item['hash_id']) for item in cur)
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
        cur.execute("SELECT DISTINCT `inode_id` FROM `%s` " % self.getName() +
                    " WHERE `inode_id`>=%s AND `inode_id`<%s", (start_id, end_id,))
        nameIds = set(str(item["inode_id"]) for item in cur)
        self.stopTimer('get_inode_ids')
        return nameIds

    def remove_by_inodes(self, inode_ids):
        self.startTimer()
        count = 0
        id_str = ",".join(inode_ids)
        if id_str:
            cur = self.getCursor()
            cur.execute("DELETE FROM `%s` " % self.getName() +
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
        iids = (item for item in cur)
        self.stopTimer('get_hash_inode_ids')
        return iids

    pass
