# -*- coding: utf8 -*-
#
# DB migration 001 by 2021-05-30
#
# Table `tree` : recreate indexes `inode` + `parent_id`
#
__author__ = 'sergey'

__NUMBER__ = 20210530003

def run(manager):
    """
    :param manager: Database manager
    :type  manager: dedupsqlfs.db.sqlite.manager.DbManager|dedupsqlfs.db.mysql.manager.DbManager
    :return: bool
    """

    try:
        table_sv = manager.getTable("subvolume")
        """
        :type table_sv: dedupsqlfs.db.sqlite.table.TableSubvolume |
                        dedupsqlfs.db.mysql.table.TableSubvolume
        """

        manager.getLogger().info("Migration #%s" % (__NUMBER__,))

        manager.getLogger().info("Migrate `tree` table on all subvolumes")

        cur = table_sv.getCursor(True)

        cur.execute("SELECT id FROM `%s`" % table_sv.getName())

        for subvol in iter(cur.fetchone, None):

            tname = "tree_%d" % (subvol['id'],)
            table_t = manager.getTable(tname, True)
            """
            :type table_t: dedupsqlfs.db.sqlite.table.TableTree |
                            dedupsqlfs.db.mysql.table.TableTree
            """

            manager.getLogger().info("Re-create indexes `inode` and `parent_id` on `%s`" % tname)
            if manager.TYPE == "sqlite":
                c = table_t.getCursor()
                c.execute("DROP INDEX IF EXISTS `tree_inode`")
                c.execute("DROP INDEX IF EXISTS `tree_parent_id`")
                table_t.createIndexIfNotExists('inode', ('inode_id',))
                table_t.createIndexIfNotExists('parent_id', ('parent_id', 'id',))

            table_t.commit()
            table_t.close()

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
