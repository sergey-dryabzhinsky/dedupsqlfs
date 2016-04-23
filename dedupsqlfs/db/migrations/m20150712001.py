# -*- coding: utf8 -*-
#
# DB migration 001 by 2015-07-12
#
# Add two fields to subvol table
#
__author__ = 'sergey'

__NUMBER__ = 20150712001

def run(manager):

    tableSubvol = manager.getTable("subvolume")

    try:
        cur = tableSubvol.getCursor()

        cur.execute("ALTER TABLE subvolume ADD COLUMN stats TEXT;")
        cur.execute("ALTER TABLE subvolume ADD COLUMN stats_at INTEGER;")

        tableSubvol.commit()
    except Exception:
        return False

    tableOpts = manager.getTable("option")

    tableOpts.getCursor()
    mignumber = tableOpts.get("migration")
    if not mignumber:
        tableOpts.insert("migration", __NUMBER__)
    else:
        tableOpts.update("migration", __NUMBER__)

    tableOpts.commit()

    return True
