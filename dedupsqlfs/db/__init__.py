# -*- coding: utf8 -*-
#
# Misc DB functions
#
__author__ = 'sergey'

def check_engines():
    engines = ()
    try:
        import sqlite3
        engines += ('sqlite')
    except:
        pass
    try:
        import pymysql
        engines += ('mysql')
    except:
        pass

    msg = "Use selected storage engine. One of "+",".join(engines)+". Default is "+engines[0]+"."
    if engines and 'sqlite' in engines:
        msg += " Note: 'sqlite' use less disk space, but work slowly on large data."

    return engines, msg
