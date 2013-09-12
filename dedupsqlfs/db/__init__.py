# -*- coding: utf8 -*-
#
# Misc DB functions
#

__author__ = 'sergey'

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def tuple_factory(cursor, row):
    t = ()
    for idx, col in enumerate(cursor.description):
        t += (row[idx],)
    return t
