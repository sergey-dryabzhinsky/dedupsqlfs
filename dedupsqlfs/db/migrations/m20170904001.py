# -*- coding: utf8 -*-
#
# DB migration 001 by 2016-04-22
#
# Converts all inode tables from all snapshots to 3.2 FS format
# Always store inode times in nanoseconds
# for compatibility with 1.3 version
#
__author__ = 'sergey'

__NUMBER__ = 20170904001

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

            tn = "%s_%s" % ("inode", h,)

            table = manager.getTable(tn, True)
            """
            :type table: dedupsqlfs.db.sqlite.table.inode.TableInode |
                            dedupsqlfs.db.mysql.table.inode.TableInode
            """
            inodeCur = table.getCursor(True)
            inodeCur.execute("SELECT * FROM `%s`" % table.getName())
            for inodeItem in inodeCur:
                new_data = {
                    "atime": inodeItem["atime"] * 10**9 + inodeItem["atime_ns"],
                    "ctime": inodeItem["ctime"] * 10**9 + inodeItem["ctime_ns"],
                    "mtime": inodeItem["mtime"] * 10**9 + inodeItem["mtime_ns"],
                }
                table.update_data(inodeItem["id"], new_data)

            tn = table.getName()
            tmp_name = tn + "_tmp"
            inodeCur.execute("ALTER TABLE `%s` RENAME TO `%s`;" % (tn, tmp_name,))
            table.create()
            inodeCur.execute("INSERT INTO `%s` SELECT `id`,`nlinks`,`mode`,`uid`,`gid`,`rdev`,`size`,`atime`,`mtime`,`ctime` FROM `%s`;" % (tn, tmp_name,))
            inodeCur.execute("DROP TABLE `%s`;" % (tmp_name,))

    except Exception as e:
        import traceback
        manager.getLogger().error("Migration #%s error: %s" % (__NUMBER__, e,))
        manager.getLogger().error("Migration #%s trace:\n%s" % (__NUMBER__, traceback.format_exc(),))
        return False

    table_opts = manager.getTable("option")

    table_opts.getCursor()

    fsv = table_opts.get("fs_version")
    if not fsv:
        table_opts.insert("fs_version", "3.2")
    else:
        table_opts.update("fs_version", "3.2")

    mignumber = table_opts.get("migration")
    if not mignumber:
        table_opts.insert("migration", __NUMBER__)
    else:
        table_opts.update("migration", __NUMBER__)

    table_opts.commit()

    return True
