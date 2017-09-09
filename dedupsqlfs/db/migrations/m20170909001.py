# -*- coding: utf8 -*-
#
# DB migration 001 by 2016-04-22
#
# Store real block size in index table
#
__author__ = 'sergey'

__NUMBER__ = 20170909001

def run(manager):
    """
    :param manager: Database manager
    :type  manager: dedupsqlfs.db.sqlite.manager.DbManager|dedupsqlfs.db.mysql.manager.DbManager
    :return: bool
    """

    try:
        table_sv = manager.getTable("subvolume")
        """
        :type table_sv: dedupsqlfs.db.sqlite.table.subvolume.TableSubvolume |
                        dedupsqlfs.db.mysql.table.subvolume.TableSubvolume
        """

        from dedupsqlfs.lib.constants import ROOT_SUBVOLUME_NAME

        cur = table_sv.getCursor()

        cur.execute("SELECT `hash` FROM `%s`" % table_sv.getName())
        svHashes = cur.fetchall()

        manager.getLogger().info("Migration #%s: subvolumes to process = %r" % (__NUMBER__, svHashes,))

        for item in svHashes:

            h = item["hash"].decode()

            tn = "%s_%s" % ("inode_hash_block", h,)

            table = manager.getTable(tn, True)
            """
            :type table: dedupsqlfs.db.sqlite.table.inode_hash_block.TableInodeHashBlock |
                            dedupsqlfs.db.mysql.table.inode_hash_block.TableInodeHashBlock
            """
            if not table.hasField('real_size'):
                if manager.TYPE == "sqlite":
                    cur.execute("ALTER TABLE inode_hash_block ADD COLUMN real_size INTEGER NOT NULL DEFAULT 0;")
                if manager.TYPE == "mysql":
                    cur.execute("ALTER TABLE inode_hash_block ADD COLUMN real_size INT UNSIGNED NOT NULL DEFAULT 0;")

            table.commit()
            table.close()


    except Exception as e:
        import traceback
        manager.getLogger().error("Migration #%s error: %s" % (__NUMBER__, e,))
        manager.getLogger().error("Migration #%s trace:\n%s" % (__NUMBER__, traceback.format_exc(),))
        return False

    table_opts = manager.getTable("option")

    table_opts.getCursor()

    mignumber = table_opts.get("migration")
    if not mignumber:
        table_opts.insert("migration", __NUMBER__)
    else:
        table_opts.update("migration", __NUMBER__)

    table_opts.commit()

    return True
