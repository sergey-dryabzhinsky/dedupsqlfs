# -*- coding: utf8 -*-
#
# DB migration 001 by 2017-11-03
#
# New statistics for subvolume - root diff in blocks / bytes
#
__author__ = 'sergey'

__NUMBER__ = 20171103001

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

        manager.getLogger().info("Migration #%s" % (__NUMBER__,))

        if not table_sv.hasField('root_diff'):
            if manager.TYPE == "sqlite":
                cur.execute("ALTER TABLE `subvolume` ADD COLUMN `root_diff` TEXT;")
            if manager.TYPE == "mysql":
                cur.execute("ALTER TABLE `subvolume` ADD COLUMN `root_diff` TEXT;")

        if not table_sv.hasField('root_diff_at'):
            if manager.TYPE == "sqlite":
                cur.execute("ALTER TABLE `subvolume` ADD COLUMN `root_diff_at` INTEGER;")
            if manager.TYPE == "mysql":
                cur.execute("ALTER TABLE `subvolume` ADD COLUMN `root_diff_at` INT UNSIGNED;")

        table_sv.commit()
        table_sv.close()


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
