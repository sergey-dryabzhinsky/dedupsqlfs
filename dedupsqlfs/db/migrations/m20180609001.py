# -*- coding: utf8 -*-
#
# DB migration 001 by 2017-11-03
#
# Table `hash` uses binary(64) hash now
#
__author__ = 'sergey'

__NUMBER__ = 20180609001

def run(manager):
    """
    :param manager: Database manager
    :type  manager: dedupsqlfs.db.sqlite.manager.DbManager|dedupsqlfs.db.mysql.manager.DbManager
    :return: bool
    """

    try:
        table_h = manager.getTable("hash")
        """
        :type table_name: dedupsqlfs.db.sqlite.table.subvolume.TableHash |
                        dedupsqlfs.db.mysql.table.subvolume.TableHash
        """

        from dedupsqlfs.lib.constants import ROOT_SUBVOLUME_NAME

        manager.getLogger().info("Migration #%s" % (__NUMBER__,))

        manager.getLogger().info("Migrate hash table")

        cur = table_h.getCursor(True)

        # Rename table to _old
        manager.getLogger().info("Rename old table")
        if manager.TYPE == "mysql":
            cur.execute("RENAME TABLE `%s` TO `%s_old`;" % (table_h.getName(),table_h.getName(),))
        if manager.TYPE == "sqlite":
            cur.execute("ALTER TABLE `%s` RENAME TO `%s_old`;" % (table_h.getName(), table_h.getName(),))
            # Sqlite indexes not connected to tables
            table_h.createIndexOnTableIfNotExists("%s_old" % table_h.getName(), "hash", ("hash",), True)
            table_h.dropIndex("hash")

        # Create new table
        manager.getLogger().info("Create new table")
        table_h.create()

        cur.execute("SELECT * FROM `%s_old`" % table_h.getName())

        for h in iter(cur.fetchone, None):

            table_h.insertRaw(h['id'], h['hash'])

        table_h.commit()

        manager.getLogger().info("Drop old table")
        cur.execute("DROP TABLE `%s_old`;" % (table_h.getName(),))
        if manager.TYPE == "sqlite":
            # Sqlite indexes not connected to tables
            table_h.dropIndexOnTable("%s_old" % table_h.getName(), "hash")

        table_h.commit()
        table_h.close()


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
