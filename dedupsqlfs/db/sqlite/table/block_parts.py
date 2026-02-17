# -*- coding: utf8 -*-

__author__ = 'sergey'

from sqlite3 import Binary
from dedupsqlfs.db.sqlite.table import Table
from dedupsqlfs.db.sqlite.table.block import TableBlock
from dedupsqlfs.db.sqlite.table.block_fs import TableBlockFs

class TableBlockPartitions( Table ):

    n_parts = 1

    parts = None

    _table_name = 'block_parts'



    def getPart(self, n):
        if self.parts is None:
            self.parts = {}
        
        if n not in self.parts:
            if self._manager.getAppOption("block_data_storage_on_fs"):
                self.parts[n] = TableBlockFs(self._manager)
            else:
                self.parts[n] = TableBlock(self._manager)
            self.parts[n].setClustered( self._clustered )
            self.parts[n].setName( "block_%03d" % n )
            self.parts[n].setFileName( "block_%03d" % n )

        return self.parts[n]


    def getDbPageSize( self ):
        for i in range(0, self.n_parts):
            t = self.getPart(i)
            return t.getDbPageSize()
        return 0


    def getPageSize( self ):
        for i in range(0, self.n_parts):
            t = self.getPart(i)
            return t.getPageSize()
        return 0


    def setPageSize( self, page_size ):
        for i in range(0, self.n_parts):
            t = self.getPart(i)
            t.setPageSize(page_size)
        return


    def shrinkMemory( self ):
        for i in range(0, self.n_parts):
            t = self.getPart(i)
            t.shrinkMemory()
        return


    def connect( self ):
        return


    def hasTable( self ):
        has = True
        for i in range(0, self.n_parts):
            t = self.getPart(i)
            if not t.hasTable():
                has = False
        return has


    def create( self ):
        for i in range(0, self.n_parts):
            t = self.getPart(i)
            t.create()
        return

    def begin( self ):
        for i in range(0, self.n_parts):
            t = self.getPart(i)
            t.begin()
        return self

    def rollback( self ):
        for i in range(0, self.n_parts):
            t = self.getPart(i)
            t.rollback()
        return self

    def commit( self ):
        for i in range(0, self.n_parts):
            t = self.getPart(i)
            t.commit()
        return self

    def vacuum( self ):
        retsz = 0
        for i in range(0, self.n_parts):
            t = self.getPart(i)
            retsz += t.vacuum()
        return retsz

    def close( self ):
        for i in range(0, self.n_parts):
            t = self.getPart(i)
            t.close()
        return


    def insert( self, hash_id, data):
        """
        :param hash_id: int
        :param data: bytes
        :return: int
        """
        self.startTimer()

        p = hash_id % self.n_parts
        t = self.getPart(p)
        item = t.insert(hash_id, data)

        self.stopTimer('insert')
        return item

    def update( self, hash_id, data):
        """
        :param hash_id: int
        :param data: bytes
        :return: int
        """
        self.startTimer()

        p = hash_id % self.n_parts
        t = self.getPart(p)
        count = t.update(hash_id, data)

        self.stopTimer('update')
        return count

    def get( self, hash_id):
        """
        :param hash_id: int
        :return: Row
        """
        self.startTimer()

        p = hash_id % self.n_parts
        t = self.getPart(p)
        item = t.get(hash_id)

        self.stopTimer('get')
        return item

    def remove_by_ids(self, id_str):
        self.startTimer()
        count = 0
        if id_str:

            for i in range(0, self.n_parts):
                t = self.getPart(i)
                count += t.remove_by_ids(id_str)

        self.stopTimer('remove_by_ids')
        return count

    pass
