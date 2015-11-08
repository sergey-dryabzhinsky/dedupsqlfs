# -*- coding: utf8 -*-
"""
Base class for compression tool
Some base methods, properties...
Used in single or multi process compression classes
"""

__author__ = 'sergey'

from time import sleep, time
from .base import BaseCompressTool, Task, Result, constants
from multiprocessing import JoinableQueue, Process, cpu_count

class MultiProcCompressTool(BaseCompressTool):

    _procs = None
    _np = 0
    _np_limit = 0
    _task_queues = None
    _result_queue = None


    def checkCpuLimit(self):
        if self.getOption("cpu_limit"):
            self._np_limit = int(self.getOption("cpu_limit"))
        self._np = cpu_count()
        if self._np_limit > 0:
            if self._np > self._np_limit:
                self._np = self._np_limit
        return self._np

    def init(self):

        self._procs = []
        self._task_queues = []

        self._np = self.checkCpuLimit()

        self._task_queue = JoinableQueue()
        self._result_queue = JoinableQueue()

        for n in range(self._np):
            tq = JoinableQueue()
            self._task_queues.append(tq)
            p = Process(target=self._worker, name="Compressor-%s" % n, args=(tq, self._result_queue,))
            p.start()
            self._procs.append(p)

        return self

    def stop(self):

        count = 50
        alive = True
        while alive:
            for n in range(self._np):
                tq = self._task_queues[ n ]
                tq.put_nowait("stop")

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

        @param in_queue: {multiprocessing.JoinableQueue}
        @param out_queue: {multiprocessing.JoinableQueue}

        @var task: Task

        @return:
        """

        sleep_wait = 0.1

        while True:

            try:
                task = in_queue.get_nowait()
            except:
                task = None

            if task is None:
                sleep(sleep_wait)
                continue

            if type(task) is float:
                sleep_wait = task
                in_queue.task_done()
                sleep(sleep_wait)
                continue

            if type(task) is str and task == "stop":
                in_queue.task_done()
                break

            if type(task) is Task:
                result = Result()
                result.cdata, result.method = self._compressData(task.data)
                result.key = task.key
                out_queue.put_nowait(result)
                in_queue.task_done()

        return

    def _compressData(self, data):
        """
        Compress data and returns back

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
        start_time = time()

        nkeys = len(dataToCompress.keys())

        for n in range(self._np):
            tq = self._task_queues[n]
            tq.put_nowait(0.001)

        i = 0
        for key, data in dataToCompress.items():
            task = Task()
            task.key = key
            task.data = data
            nq = i % self._np
            tq = self._task_queues[ nq ]
            tq.put_nowait(task)
            i += 1

        gotKeys = 0
        while gotKeys < nkeys:
            try:
                res = self._result_queue.get_nowait()
            except:
                res = None

            if res is None:
                sleep(0.001)
                continue

            if type(res) is Result:
                self._result_queue.task_done()
                yield res.key, (res.cdata, res.method,)
                gotKeys += 1

        for n in range(self._np):
            tq = self._task_queues[n]
            tq.put_nowait(0.1)

        self.time_spent_compressing = time() - start_time

        return

    pass
