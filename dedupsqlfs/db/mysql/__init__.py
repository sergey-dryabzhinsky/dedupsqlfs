__author__ = 'sergey'

import subprocess

def get_table_engines():

    try:
        output = subprocess.check_output(["mysqld", "--version"], stderr=subprocess.DEVNULL)
        table_engines = ('MyISAM', 'InnoDB',)
        if output.find(b'MariaDB'):
            table_engines += ('Aria',)
            output = subprocess.check_output(["mysqld", "--verbose", "--help"], stderr=subprocess.DEVNULL)
            if output.find(b'tokudb'):
                table_engines += ('TokuDB',)
    except:
        # No MySQL?
        table_engines = ()

    return table_engines
