__author__ = 'sergey'

import subprocess

def get_table_engines():

    try:
        output = subprocess.check_output(["mysqld", "--version"])
        table_engines = ('MyISAM', 'InnoDB',)
        if output.find(b'MariaDB'):
            table_engines += ('Aria', 'TokuDB',)
    except:
        # No MySQL?
        table_engines = ()

    return table_engines
