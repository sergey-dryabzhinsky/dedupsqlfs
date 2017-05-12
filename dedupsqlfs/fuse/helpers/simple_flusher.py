# -*- coding: utf8 -*-
"""
@author: Sergey Dryabzhinsky
"""

import os
from time import time, sleep
from threading import Thread, Event


class SimpleThreadingCacheFlusher(Thread):

    stop_event = None
    flush_event = None

    sleep_interval = 0.01
    flush_internal = 1

    flush_filepath = None

    data_root_path = None


    def start(self):
        if not self.data_root_path:
            raise ValueError("Setup data_root_path value before thread start!")

        if not os.path.isdir(self.data_root_path):
            raise ValueError("Value of data_root_path must be existing directory path!")

        self.flush_filepath = os.path.join(self.data_root_path, '.dedupsqlfs.io')
        super().start()


    def _do_flush(self):
        file_io = open(self.flush_filepath, 'w+')
        file_io.seek(0, 0)
        file_io.write("Do not remove!\n")
        file_io.flush()
        file_io.close()


    def do_stop(self):
        if self.stop_event:
            self.stop_event.set()

    def run(self):

        self.stop_event = Event()
        self.flush_event = Event()

        last_flush = time()
        while not self.stop_event.is_set():

            now = time()

            if self.flush_event.is_set():
                self.flush_event.clear()
                last_flush -= self.flush_internal

            if now - last_flush < self.flush_internal:
                sleep(self.sleep_interval)
                continue

            self._do_flush()

            last_flush = now

            pass

        self.stop_event.clear()

        return
