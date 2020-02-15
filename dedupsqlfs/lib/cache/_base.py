# -*- coding: utf8 -*-
"""
@author Sergey Dryabzhinsky
"""

from time import time

class TimedCache(object):
    """
    Cache storage with timers
    """

    _enable_timers = True

    def __init__(self):
        self._time_spent = {}
        self._op_count = {}
        pass

    def setEnableTimers(self, flag=True):
        self._enable_timers = flag is True
        return self

    def getOperationsCount(self):
        return self._op_count

    def getAllOperationsCount(self):
        s = 0
        if not self._enable_timers:
            return s
        for op, c in self._op_count.items():
            s += c
        return s

    def incOperationsCount(self, op):
        if not self._enable_timers:
            return self
        if not (op in self._op_count):
            self._op_count[ op ] = 0
        self._op_count[ op ] += 1
        return self

    def getTimeSpent(self):
        return self._time_spent

    def getAllTimeSpent(self):
        s = 0
        if not self._enable_timers:
            return s
        for op, t in self._time_spent.items():
            s += t
        return s

    def incOperationsTimeSpent(self, op, start_time):
        if not self._enable_timers:
            return self
        if not (op in self._time_spent):
            self._time_spent[ op ] = 0
        self._time_spent[ op ] += time() - start_time
        return self

    def startTimer(self):
        if not self._enable_timers:
            return self
        self._last_time = time()
        return self

    def stopTimer(self, op):
        if not self._enable_timers:
            return self

        self.incOperationsCount(op)
        self.incOperationsTimeSpent(op, self._last_time)

        self._last_time = None
        return self
