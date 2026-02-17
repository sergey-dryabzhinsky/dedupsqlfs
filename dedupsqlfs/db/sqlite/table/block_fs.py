# -*- coding: utf8 -*-

__author__ = 'sergey'

import os
from textwrap import wrap
import shutil
from dedupsqlfs.db.sqlite.table import Table
from dedupsqlfs.lib.timers_ops import TimersOps
from dedupsqlfs.fs import mymakedirs

class TableBlockFs( Table, TimersOps ):

    _table_name="blocks"

    def insert( self, hash_id, data):
        """
        :param hash_id: int
        :param data: bytes
        :return: int
        """
        self.startTimer()
        datap = self.hashToPath(hash_id)
        print(datap)
        dn = os.path.dirname(datap)
        print(dn)
        if not os.path.isdir(dn):
          mymakedirs(dn, mode=0o777, exist_ok=True)
        self.writeData(datap, data)
        self.stopTimer('insert')
        return hash_id

    def writeData(self, path, data):
        self.startTimer()
        f = open(path,"wb")
#        print("try to write %d bytes" % len(data))
        written=f.write(data)
#        print("wrote %d bytes" % written )
        f.flush()
        f.close()
        self.stopTimer('writeData')
        return written
 
    def readData(self, path):
        self.startTimer()
        f = open(path,"rb")
        data=f.read()
        f.close()
        self.stopTimer('readData')
        return data
 
    def hashToPath(self, hash_id):
        self.startTimer()
        db_path = self.getDbFilePath()
        p = os.path.join(db_path, self._table_name)
        hashdigest = self._manager.getTable("hash").get(hash_id).hex()
        hexp = wrap(hashdigest,4)[:4]
        fp = os.path.join(p, hexp[0], hexp[1], hexp[2], hexp[3], hashdigest)
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
        return {"hash_id": hash_id, "value":value}

    def remove_by_ids(self, id_str):
        self.startTimer()
        count = 0
        if id_str:
            for hash_id in id_str.split(","):
                datap = self.hashToPath(hash_id)
                if os.path.isfile(datap):
                  os.unlink(datap)
                  dn = os.path.dirname(datap)
                  shutil.rmtree(dn, ignore_errors=True)
                  count +=1
        self.stopTimer('remove_by_ids')
        return count

    def getDbFilePath(self):
        if self._table_file_name==":memory:":
            self._db_file_path = ":memory:"
        if not self._db_file_path:
            bp = self.getManager().getBasePath()
            if self.getClustered():
                bp = self.getManager().getClusterPath()
            self._db_file_path = os.path.join(
                bp,
                self.getManager().getDbName(),
                self.getFileName()
            )
            self._db_file_path = os.path.abspath(self._db_file_path)
        return self._db_file_path

    def create(self):
      self.startTimer()
      p = db_path = self.getDbFilePath()
#      p = os.path.join(db_path, self._table_name)
#      if not os.path.isdir(p):
      mymakedirs(p, mode=0o777, exist_ok=True)
      self.stopTimer('create')

    def shrinkMemory(self):
      pass

    pass
