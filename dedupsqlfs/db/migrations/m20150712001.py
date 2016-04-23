# -*- coding: utf8 -*-
#
# DB migration 001 by 2015-07-12
#
# Add two fields to subvol table
#
__author__ = 'sergey'

__NUMBER__ = 20150712001


def run(manager):
    """
    :param manager: Database manager
    :type  manager: dedupsqlfs.db.sqlite.manager.DbManager|dedupsqlfs.db.mysql.manager.DbManager
    :return: bool
    """

    table_subvol = manager.getTable("subvolume")

    try:
        cur = table_subvol.getCursor()

        cur.execute("ALTER TABLE subvolume ADD COLUMN stats TEXT;")
        cur.execute("ALTER TABLE subvolume ADD COLUMN stats_at INTEGER;")

        table_subvol.commit()
    except Exception as e:
        manager.getLogger().error("Migration #%s error: %s" % (__NUMBER__, e,))
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
