# -*- coding: utf8 -*-
#
# DB migration 001 by 2017-05-10
#
# - Rename current Lz4 compression into 'lz4r07'
# - Add new 'lz4' compression for 1.7+
#
__author__ = 'sergey'

__NUMBER__ = 20170510001


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

        if not table_ct.find('lz4r07'):
            cur = table_ct.getCursor()

            cur.execute("UPDATE compression_type SET value='lz4r07' WHERE value='lz4';")
            cur.execute("INSERT INTO compression_type (value) VALUES ('lz4');")

            table_ct.commit()
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
