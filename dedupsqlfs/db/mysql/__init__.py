__author__ = 'sergey'

from dedupsqlfs.fs import which
import subprocess

def get_table_engines():

    table_engines = ()

    mysqld_bin = which("mysqld")

    if not mysqld_bin:
        return table_engines

    try:
        output = subprocess.check_output([mysqld_bin, "--verbose", "--help"], stderr=subprocess.DEVNULL)
        table_engines = ['MyISAM', 'InnoDB',]
        if output.find(b'--aria[='):
            table_engines.append('Aria')
        if output.find(b'--tokudb[='):
            table_engines.append('TokuDB')
        if output.find(b'--rocksdb[='):
            table_engines.append('RocksDB')
    except:
        # No MySQL?
        table_engines = ()

    return table_engines
