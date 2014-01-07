# -*- coding: utf8 -*-

__author__ = 'sergey'

import logging

# This is more than 'info' but less than warning
NOTE = logging.INFO + 1

# This is for output more than debug, an extra debug
DEBUG_VERBOSE = logging.DEBUG - 1

logging.addLevelName(DEBUG_VERBOSE, "DEBUGV")
logging.addLevelName(NOTE, "NOTE")

def debugv(self, message, *args, **kws):
    # Yes, logger takes its '*args' as 'args'.
    if self.isEnabledFor(DEBUG_VERBOSE):
        self._log(DEBUG_VERBOSE, message, args, **kws)
    return

def note(self, message, *args, **kws):
    # Yes, logger takes its '*args' as 'args'.
    if self.isEnabledFor(NOTE):
        self._log(NOTE, message, args, **kws)
    return

logging.Logger.debugv = debugv
logging.Logger.note = note
