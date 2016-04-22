# -*- coding: utf8 -*-
#
# DB migration 001 by 2016-04-22
#
# Alter tables names for all snapshots
# for compatibility with 1.3 version
#
__author__ = 'sergey'

__NUMBER__ = 20160422001

def run(manager):

    if manager.TYPE in ("sqlite3",):

        tableSV = manager.getTable("subvolume")

        try:
            from dedupsqlfs.lib.constants import ROOT_SUBVOLUME_NAME

            cur = tableSV.getCursor()

            cur.execute("SELECT hash FROM %s WHERE name != '%s'" % (tableSV.getName(), ROOT_SUBVOLUME_NAME,))
            svHashes = cur.fetchall()
            for item in svHashes:

                h = item["hash"].decode()

                for tn in ("inode", "xattr", "tree", "link", "inode_option", "inode_hash_block",):

                    old_tn = "%s_%s" % (tn, h,)

                    table = manager.getTable(old_tn)
                    cur.execute("ALTER TABLE %s RENAME TO %s;" % (old_tn, tn,))
                    table.commit()

        except Exception:
            pass

    tableOpts = manager.getTable("option")

    tableOpts.getCursor()
    mignumber = tableOpts.get("migration")
    if not mignumber:
        tableOpts.insert("migration", __NUMBER__)
    else:
        tableOpts.update("migration", __NUMBER__)

    tableOpts.commit()

    return
