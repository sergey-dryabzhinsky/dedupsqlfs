# -*- coding: utf8 -*-
#
# DB migration 001 by 2015-07-12
#
# - Rename current ZSTD compression into 'zstd001'
# - Add new 'zstd' compression for 0.3+
#
__author__ = 'sergey'

__NUMBER__ = 20151113001

def run(manager):

    tableCT = manager.getTable("compression_type")

    cur = tableCT.getCursor()
    cur.execute("UPDATE compression_type SET value='zstd001' WHERE value='zstd';")
    cur.execute("INSERT INTO compression_type (value) VALUES ('zstd');")

    tableCT.commit()

    tableOpts = manager.getTable("option")

    tableOpts.getCursor()
    mignumber = tableOpts.get("migration")
    if not mignumber:
        tableOpts.insert("migration", __NUMBER__)
    else:
        tableOpts.update("migration", __NUMBER__)

    tableOpts.commit()

    return
