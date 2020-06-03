# -*- coding: utf8 -*-
#
# Misc DB functions
#
__author__ = 'sergey'

def check_engines():
    engines = ()
    try:
        import sqlite3
        engines += ('sqlite',)
    except:
        pass

    try:
        import pymysql

        from dedupsqlfs.db.mysql import get_table_engines

        if len(get_table_engines()):
            engines += ('mysql',)
    except:
        pass

    try:
        import psycopg2
        engines += ('pgsql',)
    except:
        pass

    msg = ""
    if engines:
        msg = "Use selected storage engine. One of "+", ".join(engines)+". Default is "+engines[0]+"."
        if 'sqlite' in engines:
            msg += " Note: 'sqlite' use less disk space, and may work slowly on large data."

    return engines, msg
