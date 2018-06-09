# -*- coding: utf8 -*-
#
# DB migration 002 by 2018-06-09
#
# - Add new 'xz' compression
# - Remove old 'lzma' compression
# - change all old lzma to new xz compression
#
__author__ = 'sergey'

__NUMBER__ = 20180609002


def run(manager):
    """
    :param manager: Database manager
    :type  manager: dedupsqlfs.db.sqlite.manager.DbManager|dedupsqlfs.db.mysql.manager.DbManager
    :return: bool
    """

    try:
        table_ct = manager.getTable("compression_type")
        """
        :type table_ct: dedupsqlfs.db.sqlite.table.compression_type.TableCompressionType |
                        dedupsqlfs.db.mysql.table.compression_type.TableCompressionType
        """
        table_hct = manager.getTable("hash_compression_type")
        """
        :type table_ct: dedupsqlfs.db.sqlite.table.compression_type.TableHashCompressionType |
                        dedupsqlfs.db.mysql.table.compression_type.TableHashCompressionType
        """

        newHashTypeId = table_ct.find('xz')
        if not newHashTypeId:
            cur = table_ct.getCursor()
            cur.execute("INSERT INTO compression_type (value) VALUES ('xz');")
            newHashTypeId = cur.lastrowid
            table_ct.commit()

        hashTypeId = table_ct.find('lzma')
        if hashTypeId:
            cur = table_ct.getCursor()
            cur.execute("DELETE FROM compression_type WHERE id=?;", (hashTypeId,))
            table_ct.commit()

        cur2 = table_hct.getCursor()
        cur2.execute("UPDATE hash_compression_type SET type_id=? WHERE type_id=?", (hashTypeId, newHashTypeId,))
        table_hct.commit()

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
