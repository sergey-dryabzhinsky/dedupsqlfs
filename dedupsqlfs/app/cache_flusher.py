# -*- coding: utf8 -*-
"""
Separate process for cache flushing

@author: Sergey Dryabzhinsky
"""

import sys
import os
from time import time, sleep
import argparse
import dedupsqlfs
import signal

class SimpleCacheFlusher(object):

    sleep_interval = 0.01
    flush_internal = 1

    flush_filepath = None

    mount_point = None

    _stop_flag = False

    def start(self):
        if not self.mount_point:
            raise ValueError("Setup mouht_point value before thread start!")

        if not os.path.isdir(self.mount_point):
            raise ValueError("Value of mouht_point must be existing directory path!")

        self.flush_filepath = os.path.join(self.mount_point, '.dedupsqlfs.io')
        self._stop_flag = False

    def _do_flush(self):

        if not os.path.exists(self.mount_point):
            return

        if not os.path.isdir(self.mount_point):
            return

        if not os.path.ismount(self.mount_point):
            return

        file_io = open(self.flush_filepath, 'w+')
        file_io.seek(0, 0)
        file_io.write("Do not remove!\n")
        file_io.flush()
        file_io.close()


    def do_stop(self):
        self._stop_flag = True

    def run(self):

        last_flush = time()
        while not self._stop_flag:

            now = time()

            if now - last_flush < self.flush_internal:
                sleep(self.sleep_interval)
                continue

            self._do_flush()

            last_flush = now

            pass

        self._stop_flag = False

        return 0


flusher = SimpleCacheFlusher()


def main():
    global flusher

    parser = argparse.ArgumentParser(
        prog="%s/%s cache_flusher python/%s" % (dedupsqlfs.__name__, dedupsqlfs.__version__, sys.version.split()[0]),
        conflict_handler="resolve")

    # Register some custom command line options with the option parser.
    parser.add_argument('-h', '--help', action='help', help="show this help message and exit")

    parser.add_argument('mountpoint', help="specify mount point")

    args = parser.parse_args()

    flusher.mount_point = args.mountpoint
    flusher.start()

    # Catch some common and system depends signals
    signal.signal(signal.SIGINT, flusher.do_stop)
    signal.signal(signal.SIGTERM, flusher.do_stop)
    try:
        signal.signal(signal.SIGABRT, flusher.do_stop)
    except:
        pass
    try:
        signal.signal(signal.SIGQUIT, flusher.do_stop)
    except:
        pass
    try:
        signal.signal(signal.SIGPWR, flusher.do_stop)
    except:
        pass

    return flusher.run()

# vim: ts=4 sw=4 et
