__author__ = 'sergey'

import subprocess

def get_table_engines():

    table_engines = ('MyISAM', 'InnoDB',)

    output = subprocess.check_output(["mysqld", "--version"])
    if output.find(b'MariaDB'):
        table_engines += ('Aria', 'TokuDB',)
    return table_engines
