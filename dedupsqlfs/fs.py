# -*- coding: utf8 -*-
"""
Module to work with file system
"""

__author__ = 'sergey'

import os


def which(bin_exe):
    """
    Поиск исполняемого файла

    @return: False или полный путь до исполняемого файла
    @rtype: str|bool
    """
    # support Linux / POSIX here?
    paths = ["/usr/local/bin", "/usr/local/sbin", "/bin", "/sbin", "/usr/bin", "/usr/sbin"]
    if "PATH" in os.environ:
        paths = os.environ["PATH"].split(":")
    for p in paths:
        full_path = os.path.join(p, bin_exe)
        if os.path.isfile(full_path) and os.access(full_path, os.X_OK):
            return full_path
    return False
