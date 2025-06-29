# -*- coding: utf8 -*-

__author__ = 'sergey'

import os
from textwrap import wrap
import shutil
from dedupsqlfs.db.sqlite.table import Table
from dedupsqlfs.lib.timers_ops import TimersOps

class TableBlock( Table, TimersOps ):

    def insert( self, hash_id, data):
        """
        :param hash_id: int
        :param data: bytes
        :return: int
        """
        self.startTimer()
        datap = self.hashToPath(hash_id)
        dn = os.path.dirname(datap)
        if not os.path.isdir(dn):
            os.makedirs(dn, mode=0o777, exist_ok=True)
        self.writeData(datap, data)
        self.stopTimer('insert')
        return hash_id

    def writeData(path, data):
        self.startTimer()
        f = open(path,"wb")
        written=f.write(data)
        f.flush()
        f.close()
        self.stopTimer('writeData')
        return written
 
    def readData(path):
        self.startTimer()
        f = open(path,"rb")
        data=f.read()
        f.close()
        self.stopTimer('readData')
        return data
 
    def hashToPath(hash_id):
        self.startTimer()
        db_path = self.getDbFilePath()
        p = os.path.join(db_path, self._table_name)
        hashdigest = self.manager.getTable("hash").get(hash_id).hex()
        hexp = wrap(hashdigest,4)[:4]
        fp = os.path.join(p, hexp, hashdigest)
        self.stopTimer('hashToPath')
        return fp

    def update( self, hash_id, data):
        """
        :param hash_id: int
        :param data: bytes
        :return: int
        """
        self.startTimer()

        datap = self.hashToPath(hash_id)
        if not os.path.isfile(datap):
            # nothing to update, insert first
            return 0
        self.writeData(datap, data)
        self.stopTimer('update')
        return 1

    def get( self, hash_id):
        """
        :param hash_id: int
        :return: Row
        """
        self.startTimer()
        datap = self.hashToPath(hash_id)
        if not os.path.isfile(datap):
            # nothing to update, insert first
            return None
        value = self.readData(datap)
        self.stopTimer('get')
        return {"hash_id" hash_id:, "value":value}

    def remove_by_ids(self, id_str):
        self.startTimer()
        count = 0
        if id_str:
            for hash_id in id_str.split(","):
                datap = self.hashToPath(hash_id)
                if os.path.isfile(datap):
                  os.unlink(datap)
                  dn = os.path.dirname(dataap)
                  shutil.rmtree(dn, ignore_errors=True)
                  count +=1
        self.stopTimer('remove_by_ids')
        return count

    pass
