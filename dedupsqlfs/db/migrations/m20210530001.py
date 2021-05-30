# -*- coding: utf8 -*-
#
# DB migration 001 by 2021-05-30
#
# Table `hash_compression_type` don't uses index `hct_type`
#
__author__ = 'sergey'

__NUMBER__ = 20210530001

def run(manager):
    """
    :param manager: Database manager
    :type  manager: dedupsqlfs.db.sqlite.manager.DbManager|dedupsqlfs.db.mysql.manager.DbManager
    :return: bool
    """

    try:
        table_hct = manager.getTable("hash_compression_type")
        """
        :type table_hct: dedupsqlfs.db.sqlite.table.TableHashCompressionType |
                        dedupsqlfs.db.mysql.table.TableHashCompressionType
        """

        manager.getLogger().info("Migration #%s" % (__NUMBER__,))

        manager.getLogger().info("Migrate `hash_compression_type` table")

        manager.getLogger().info("Drop unused index on `type_id`")
        if manager.TYPE == "mysql":
            table_hct.dropIndex("type")
        if manager.TYPE == "sqlite":
            c = table_hct.getCursor()
            c.execute("DROP INDEX IF EXISTS `hct_type`")

        table_hct.commit()
        table_hct.close()


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
