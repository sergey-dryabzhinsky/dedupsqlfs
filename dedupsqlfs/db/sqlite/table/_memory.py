# -*- coding: utf8 -*-

__author__ = 'sergey'

import platform

from dedupsqlfs.db.sqlite.table._base import Table
from dedupsqlfs.db.sqlite.row import dict_factory

class MemoryTable( Table ):

    def _decompress(self):
        return True

    def _compress(self):
        return True

    def connect( self ):
        import sqlite3

        pageSize = self.calcFilePageSize()

        cacheSize = 64*1024*1024 / pageSize

        conn = sqlite3.connect(":memory:")

        conn.row_factory = dict_factory
        conn.text_factory = bytes

        # Set journal mode early as possible
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA journal_size_limit=0")

        # Dirty hack again for pypy
        isPyPy = platform.python_implementation() == 'PyPy'
        if isPyPy:
            # Something wrong with sqlite3 module in pypy3
            # Works correctly only with autocommit
            self.getManager().setAutocommit(True)

        conn.execute("PRAGMA read_uncommitted=ON")
        conn.isolation_level = "DEFERRED"

        conn.execute("PRAGMA synchronous=OFF")

        conn.execute("PRAGMA page_size=%i" % pageSize)
        conn.execute("PRAGMA cache_size=%i" % cacheSize)

        self._conn = conn
        return

    def getFileSize(self):
        return 0

    def vacuum(self):
        self.startTimer()
        self.getConnection().execute("VACUUM")
        self.stopTimer("vacuum")
        return 0

    def drop(self):
        self.startTimer()
        self.close()
        self.stopTimer("drop")
        return self

    pass
