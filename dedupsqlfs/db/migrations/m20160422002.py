# -*- coding: utf8 -*-
#
# DB migration 001 by 2016-04-22
#
# Alter tables names for all snapshots
# for compatibility with 1.3 version
#
__author__ = 'sergey'

__NUMBER__ = 20160422002


def run(manager):
    """
    :param manager: Database manager
    :type  manager: dedupsqlfs.db.sqlite.manager.DbManager|dedupsqlfs.db.mysql.manager.DbManager
    :return: bool
    """

    if manager.TYPE in ("sqlite3",):

        try:
            table_sv = manager.getTable("subvolume")
            """
            :type table_sv: dedupsqlfs.db.sqlite.table.subvolume.TableSubvolume |
                            dedupsqlfs.db.mysql.table.subvolume.TableSubvolume
            """

            from dedupsqlfs.lib.constants import ROOT_SUBVOLUME_NAME

            cur = table_sv.getCursor()

            cur.execute("SELECT hash, name FROM %s WHERE name != '%s'" % (table_sv.getName(), ROOT_SUBVOLUME_NAME,))
            svHashes = cur.fetchall()

            manager.getLogger().info("Migration #%s: subvolumes to process = %r" % (__NUMBER__, svHashes,))

            for item in svHashes:

                h = item["hash"].decode()

                for tn in ["inode", "xattr", "tree", "link", "inode_option", "inode_hash_block",]:

                    old_tn = "%s_%s" % (tn, h,)

                    table = manager.getTable(old_tn, True)
                    """
                    :type table: dedupsqlfs.db.sqlite.table._base.Table |
                                    dedupsqlfs.db.mysql.table_base.Table
                    """
                    table.setName(old_tn)
                    if table.hasTable():
                        manager.getLogger().info("Migration #%s: alter table name %r => %r" % (__NUMBER__, old_tn, tn,))
                        cur = table.getCursor()
                        cur.execute("ALTER TABLE `%s` RENAME TO `%s`;" % (old_tn, tn,))
                    table.setName(tn)

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
