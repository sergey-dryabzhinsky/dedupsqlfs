# -*- coding: utf8 -*-
"""
Base class for compression tool
Some base methods, properties...
Used in single or multi process compression classes
"""

__author__ = 'sergey'

from time import sleep
from .base import BaseCompressTool, Task, Result, constants
from multiprocessing import Queue, Process, cpu_count

class MultiProcCompressTool(BaseCompressTool):

    _procs = None
    _np = 0
    _task_queue = None
    _result_queue = None

    def init(self):

        self._procs = []

        self._np = cpu_count()
        self._task_queue = Queue()
        self._result_queue = Queue()

        for n in range(self._np):
            p = Process(None, None, "Compressor-%s" % n, (self._task_queue, self._result_queue,))
            self._procs.append(p)
            p.start()

        return self

    def stop(self):

        count = 50
        alive = True
        while alive:
            for n in range(self._np):
                self._task_queue.put_nowait("stop")

            sleep(0.1)

            alive = False
            for n in range(self._np):
                if self._procs[n].is_alive():
                    alive = True

            count -= 1
            if count <= 0:
                break

        for n in range(self._np):
            if self._procs[n].is_alive():
                self._procs[n].terminate()

        return self

    def _worker(self, in_queue, out_queue):
        """

        @param in_queue: multiprocessing.Queue
        @param out_queue: multiprocessing.Queue

        @var task: Task

        @return:
        """

        while True:

            try:
                task = in_queue.get_nowait()
            except:
                task = None

            if task is None:
                sleep(0.01)
                continue

            if type(task) is str and task == "stop":
                break

            if type(task) is Task:
                result = Result()
                result.cdata, result.method = self._compressData(task.data)
                result.key = task.key
                out_queue.put_nowait(result)

        return

    def _compressData(self, data):
        """
        Compress data and returns back

        @param key: int|str - task key
        @param data: bytes  - task data

        @return
        """
        method = self.getOption("compression_method")
        forced = self.getOption("compression_forced")
        level = self.getOption("compression_level")

        cdata = data
        data_length = len(data)
        cmethod = constants.COMPRESSION_TYPE_NONE

        if data_length <= self.getOption("compression_minimal_size") and not forced:
            return cdata, cmethod

        if method != constants.COMPRESSION_TYPE_NONE:
            if method not in (constants.COMPRESSION_TYPE_BEST, constants.COMPRESSION_TYPE_CUSTOM,):
                comp = self._compressors[ method ]
                if comp.isDataMayBeCompressed(data):
                    cdata = comp.compressData(data, level)
                    cmethod = method
                    if data_length <= len(cdata) and not forced:
                        cdata = data
                        cmethod = constants.COMPRESSION_TYPE_NONE
            else:
                min_len = data_length * 2
                # BEST
                methods = self._compressors.keys()
                if method == constants.COMPRESSION_TYPE_CUSTOM:
                    methods = self.getOption("compression_custom")
                for m in methods:
                    comp = self._compressors[ m ]
                    if comp.isDataMayBeCompressed(data):
                        _cdata = comp.compressData(data, level)
                        cdata_length = len(_cdata)
                        if min_len > cdata_length:
                            min_len = cdata_length
                            cdata = _cdata
                            cmethod = m

                if data_length <= min_len and not forced:
                    cdata = data
                    cmethod = constants.COMPRESSION_TYPE_NONE

        return cdata, cmethod

    def compressData(self, dataToCompress):
        """
        Compress data and returns back

        @param dataToCompress: dict { hash id: bytes data }

        @return dict { hash id: (compressed data (bytes), compresion method (string) ) }
        """

        result = {}

        nkeys = len(dataToCompress.keys())

        for key, data in dataToCompress.items():
            task = Task()
            task.key = key
            task.data = data
            self._task_queue.put_nowait(task)

        gotKeys = 0
        while gotKeys < nkeys:
            sleep(0.01)
            try:
                res = self._result_queue.get_nowait()
            except:
                res = None

            if res is None:
                continue

            if type(res) is Result:
                result[ res.key ] = (res.cdata, res.method,)
                gotKeys += 1

        return result

    def decompressData(self, method, data):
        """
        deCompress data and returns back

        @return bytes
        """
        comp = self._compressors[ method ]
        return comp.decompressData(data)

    pass
