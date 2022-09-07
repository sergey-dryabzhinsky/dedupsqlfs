# -*- coding: utf8 -*-
#
# DB migration 001 by 2022-09-07
#
# - Remove old compression methods
#
__author__ = 'sergey'

__NUMBER__ = 20220907001


def run(manager):
    """
    :param manager: Database manager
    :type  manager: dedupsqlfs.db.sqlite.manager.DbManager |
                    dedupsqlfs.db.mysql.manager.DbManager
    :return: bool
    """

    try:
        table_ct = manager.getTable("compression_type")
        """
        :type table_ct: dedupsqlfs.db.sqlite.table.compression_type.TableCompressionType |
                        dedupsqlfs.db.mysql.table.compression_type.TableCompressionType
        """

        cur = table_ct.getCursor()

        cur.execute("DELETE compression_type WHERE value='zstd001';")
        cur.execute("DELETE compression_type WHERE value='zstd036';")
        cur.execute("DELETE compression_type WHERE value='zstd047';")
        cur.execute("DELETE compression_type WHERE value='zstd061';")
        cur.execute("DELETE compression_type WHERE value='quicklz';")
        cur.execute("DELETE compression_type WHERE value='quicklzf';")
        cur.execute("DELETE compression_type WHERE value='quicklzm';")
        cur.execute("DELETE compression_type WHERE value='quicklzb';")
        cur.execute("DELETE compression_type WHERE value='lz4r07';")

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
