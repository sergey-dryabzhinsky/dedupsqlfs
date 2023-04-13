# -*- coding: utf8 -*-

"""
Store blocks on FS by hex-hash value path
"""

__author__ = 'sergey'

import re
import os
from time import time
import subprocess

from dedupsqlfs.db.sqlite.table import Table

class TableBlockFS( Table ):

    _table_name = "block"

    _base_path = None

    def connect( self ):
        return

    def hasTable( self ):
        return True

    def create( self ):
        return

    def shrinkMemory( self ):
        return


    def getBasePath( self ):

        if self._base_path is not None:
            return self._base_path

        bp = self.getManager().getBasePath()
        if self.getClustered():
            bp = self.getManager().getClusterPath()

        self._base_path = bp

        return bp


    def getDbPageSize( self ):

        bp = self.getManager().getBasePath()
        if self.getClustered():
            bp = self.getManager().getClusterPath()

        st = os.statvfs(bp)
        return st.f_frsize

    def hex_hash_to_path(self, hex_hash, chunk_length=4, chunk_depth=4):
        """
        :param hex_hash: bytes
        :return: string
        """
        start_time = time()

        l = len(hex_hash)
        s = [hex_hash[i:i+chunk_length] for i in range(0, l, chunk_length)]
        # regex slow?
        #s = re.findall('.{1,%d}' % chunk_length, hex_hash)
        res = "/".join(s[:chunk_depth])

        self.incOperationsTimeSpent("hex_hash_to_path", start_time)
        return res

    def insert( self, hex_hash, data):
        """
        :param hex_hash: bytes
        :param data: bytes
        :return: int
        """
        self.startTimer()

        bp = self.getBasePath()

        hex_path = self.hex_hash_to_path(hex_hash)

        full_path = os.path.join(bp, "blocks", hex_path)

        if not os.path.isdir(full_path):
            start_time = time()

            os.makedirs(full_path, 0o750, True)

            self.incOperationsTimeSpent("os.makedirs", start_time)


        file_path = os.path.join(full_path, hex_hash)

        start_time = time()

        f = open(file_path, "wb")
        f.write(data)

        if self.getManager().getSynchronous():
            f.fsync()

        f.close()

        self.incOperationsTimeSpent("write_block_data_to_file", start_time)


        self.stopTimer('insert')
        return 1

    def update( self, hex_hash, data):
        """
        :param hex_hash: bytes
        :param data: bytes
        :return: int
        """
        self.startTimer()

        bp = self.getBasePath()

        hex_path = self.hex_hash_to_path(hex_hash)

        full_path = os.path.join(bp, "blocks", hex_path)

        if not os.path.isdir(full_path):
            return 0

        file_path = os.path.join(full_path, hex_hash)

        start_time = time()

        f = open(file_path, "wb")
        f.write(data)

        if self.getManager().getSynchronous():
            f.fasync()

        f.close()

        self.incOperationsTimeSpent("write_block_data_to_file", start_time)


        self.stopTimer('update')
        return 1

    def get( self, hex_hash):
        """
        :param hex_hash: bytes
        :return: bytes|None
        """
        self.startTimer()

        bp = self.getBasePath()

        hex_path = self.hex_hash_to_path(hex_hash)

        full_path = os.path.join(bp, "blocks", hex_path)

        if not os.path.isdir(full_path):
            return None

        file_path = os.path.join(full_path, hex_hash)

        if not os.path.isfile(file_path):
            return None

        start_time = time()

        f = open(file_path, "rb")
        data = f.read()
        f.close()

        self.incOperationsTimeSpent("read_block_data_from_file", start_time)

        self.stopTimer('get')
        return data

    def remove_by_hashes(self, hashes):
        self.startTimer()
        count = 0
        if hashes:

            bp = self.getBasePath()

            for hex_hash in hases:
                hex_path = self.hex_hash_to_path(hex_hash)

                full_path = os.path.join(bp, "blocks", hex_path)

                if not os.path.isdir(full_path):
                    continue

                file_path = os.path.join(full_path, hex_hash)

                if os.path.isfile(file_path):
                    os.remove(file_path)
                    count += 1

        self.stopTimer('remove_by_hashes')
        return count

    pass
