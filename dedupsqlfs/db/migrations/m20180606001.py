# -*- coding: utf8 -*-
#
# DB migration 001 by 2017-11-03
#
# Tables `name` and `subvolume` uses xxh32 hash as ID value
#
__author__ = 'sergey'

__NUMBER__ = 20180606001

import os
from ddsf_xxhash import xxh32

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

        table_nm = manager.getTable("name")
        """
        :type table_name: dedupsqlfs.db.sqlite.table.subvolume.TableName |
                        dedupsqlfs.db.mysql.table.subvolume.TableName
        """

        from dedupsqlfs.lib.constants import ROOT_SUBVOLUME_NAME

        manager.getLogger().info("Migration #%s" % (__NUMBER__,))

        manager.getLogger().info("1. Migrate subvolume table")

        cur = table_sv.getCursor(True)

        # Rename table to _old
        manager.getLogger().info("Rename old table")
        if manager.TYPE == "mysql":
            cur.execute("RENAME TABLE `%s` TO `%s_old`;" % (table_sv.getName(),table_sv.getName(),))
        if manager.TYPE == "sqlite":
            cur.execute("ALTER TABLE `%s` RENAME TO `%s_old`;" % (table_sv.getName(), table_sv.getName(),))

        # Create new table
        manager.getLogger().info("Create new table")
        table_sv.create()

        cur.execute("SELECT * FROM `%s_old`" % table_sv.getName())

        for subvol in iter(cur.fetchone, None):

            newId = xxh32(subvol['name']).intdigest()

            checkId = table_sv.insert(subvol['name'], subvol['created_at'], subvol['mounted_at'], subvol['updated_at'],
                            subvol['stats_at'], subvol['stats'], subvol['root_diff_at'], subvol['root_diff'])

            if checkId != newId:
                manager.getLogger().error("EEE: genrated subvolume IDs not equal: %r != %r!" % (checkId, newId,))
                raise RuntimeError('Error while migrating subvolume table!')

            oldHash = subvol['hash']

            manager.getLogger().info("Rename all subvolume %r dependant table files" % newId)

            for tableName in ("inode", "inode_option", "inode_hash_block", "link", "xattr", "tree"):

                oldName = "%s_%s" % (tableName, oldHash,)
                newName = "%s_%s" % (tableName, newId,)
                oldPath = manager.getTable(oldName, True).getDbFilePath()
                newPath = manager.getTable(newName, True).getDbFilePath()

                manager.getLogger().info("-- %r -> %r" % (oldName, newName,))
                os.rename(oldPath, newPath)

        table_sv.commit()

        manager.getLogger().info("Drop old table")
        cur.execute("DROP TABLE `%s_old`;" % (table_sv.getName(),))

        table_sv.commit()
        table_sv.close()


        manager.getLogger().info("2. Migrate name table")

        cur = table_nm.getCursor(True)

        # Rename table to _old
        manager.getLogger().info("Rename old table")
        if manager.TYPE == "mysql":
            cur.execute("RENAME TABLE `%s` TO `%s_old`;" % (table_nm.getName(),table_nm.getName(),))
        if manager.TYPE == "sqlite":
            cur.execute("ALTER TABLE `%s` RENAME TO `%s_old`;" % (table_nm.getName(), table_nm.getName(),))

        # Create new table
        manager.getLogger().info("Create new table")
        table_nm.create()

        cur.execute("SELECT * FROM `%s_old`" % table_nm.getName())

        for nm in iter(cur.fetchone, None):

            newId = xxh32(nm['value']).intdigest()

            checkId = table_nm.insert(nm['value'])

            if checkId != newId:
                manager.getLogger().error("EEE: genrated subvolume IDs not equal: %r != %r!" % (checkId, newId,))
                raise RuntimeError('Error while migrating name table!')

        table_nm.commit()

        manager.getLogger().info("Drop old table")
        cur.execute("DROP TABLE `%s_old`;" % (table_nm.getName(),))

        table_nm.commit()
        table_nm.close()


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
