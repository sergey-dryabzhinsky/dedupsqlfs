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

    try:
        table_sv = manager.getTable("subvolume")
        """
        :type table_sv: dedupsqlfs.db.sqlite.table.subvolume.TableSubvolume |
                        dedupsqlfs.db.mysql.table.subvolume.TableSubvolume
        """

        cur = table_sv.getCursor()

        if not table_sv.hasField('stats'):
            cur.execute("ALTER TABLE subvolume ADD COLUMN stats TEXT;")
        if not table_sv.hasField('stats_at'):
            cur.execute("ALTER TABLE subvolume ADD COLUMN stats_at INTEGER;")

        table_sv.commit()
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
