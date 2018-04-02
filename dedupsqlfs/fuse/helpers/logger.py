# -*- coding: utf8 -*-

__author__ = 'sergey'

import sys
from time import time
from dedupsqlfs.log import logging, DEBUG_VERBOSE, IMPORTANT

class DDSFlogger(object):
    """
    Loggin class with ability to skip any logging work
    """


    _app = None


    _time_in = 0


    _filter_calls = None


    _logger = None


    def __init__(self, application, filter_calls=None):
        """
        @param application:
        @type application: dedupsqlfs.fuse.dedupfs.DedupFS

        @param filter_calls:
        @type filter_calls: list|tuple|set|None
        """
        self._app = application
        self._filter_calls = set()
        if type(filter_calls) in (list, tuple, set,):
            self._filter_calls += set(filter_calls)
        self._init()
        pass


    def getTimeIn(self):
        return self._time_in


    def addCallToFilter(self, call):
        """
        Add function name to log filter
        @param call:
        @return:
        """
        self._filter_calls.add(call)


    def _init(self):
        self._logger = logging.getLogger(self._app.__class__.__name__)
        self._logger.setLevel(logging.ERROR)

        # By default - almos all disabled
        self.critical = self._log_critical
        self.error = self._log_error
        self.important = self._log_important
        self.warning = self._empty_log
        self.warn = self._empty_log
        self.note = self._empty_log
        self.info = self._empty_log
        self.debug = self._empty_log
        self.debugv = self._empty_log

        self.logCall = self._empty_log_call

        if self._app.getOption('memory_usage'):
            self._logger.setLevel(IMPORTANT)

        # Configure logging of messages to a file.
        if self._app.getOption("log_file"):
            handler = logging.StreamHandler(open(self._app.getOption("log_file"), 'a'))
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)
        if not self._app.getOption("log_file_only"):
            self._logger.addHandler(logging.StreamHandler(sys.stderr))
        # Convert verbosity argument to logging level?
        if self._app.getOption("verbosity") > 0:
            if self._app.getOption("verbosity") >= 1:
                self._logger.setLevel(logging.WARNING)
                self.warning = self._log_warning
                self.warn = self._log_warning
                self.note = self._log_note

            if self._app.getOption("verbosity") >= 2:
                self._logger.setLevel(logging.INFO)
                self.info = self._log_info

            if self._app.getOption("verbosity") >= 3:
                self._logger.setLevel(logging.DEBUG)
                self.debug = self._log_debug

            if self._app.getOption("verbosity") >= 4:
                self._logger.setLevel(DEBUG_VERBOSE)
                self.debugv = self._log_debugv
                self.logCall = self._log_call
        return


    def _empty_log_call(self, func, msg, *args):
        return


    def _empty_log(self, msg, *args):
        return


    def _log_call(self, func, msg, *args):
        begin_t = time()
        if not self._filter_calls or func in self._filter_calls:
            self._logger.debugv("%s %s" % (func, msg,), *args)
        self._time_in += time() - begin_t


    def _log_critical(self, msg, *args):
        begin_t = time()
        self._logger.critical("%s" % msg, *args)
        self._time_in += time() - begin_t

    def _log_error(self, msg, *args):
        begin_t = time()
        self._logger.error("%s" % msg, *args)
        self._time_in += time() - begin_t

    def _log_important(self, msg, *args):
        begin_t = time()
        self._logger.important("%s" % msg, *args)
        self._time_in += time() - begin_t

    def _log_warning(self, msg, *args):
        begin_t = time()
        self._logger.warning("%s" % msg, *args)
        self._time_in += time() - begin_t

    def _log_note(self, msg, *args):
        begin_t = time()
        self._logger.note("%s" % msg, *args)
        self._time_in += time() - begin_t

    def _log_info(self, msg, *args):
        begin_t = time()
        self._logger.info("%s" % msg, *args)
        self._time_in += time() - begin_t

    def _log_debug(self, msg, *args):
        begin_t = time()
        self._logger.debug("%s" % msg, *args)
        self._time_in += time() - begin_t

    def _log_debugv(self, msg, *args):
        begin_t = time()
        self._logger.debugv("%s" % msg, *args)
        self._time_in += time() - begin_t


    pass
