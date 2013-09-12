# -*- coding: utf8 -*-

__author__ = 'sergey'

import os
import stat
import time
import math
import llfuse
from dedupsqlfs.lib import constants

class Snapshot(object):

    _manager = None
    _last_error = None

    def __init__(self, manager):

        self._manager = manager

        self.root_mode = stat.S_IFDIR | 0o755

        pass

    def getManager(self):
        return self._manager

    def getTable(self, name):
        return self.getManager().getTable(name)

    def getLastError(self):
        return self._last_error

    def create(self, name):

        snap_name = b'@' + name

        node = self.__get_tree_node_by_parent_inode_and_name(llfuse.ROOT_INODE, snap_name)
        if node:
            self._last_error = "Already has snapshot / subvolume"
            return False

        root_subvol_node = self.__get_tree_node_by_parent_inode_and_name(llfuse.ROOT_INODE, constants.ROOT_SUBVOLUME_NAME)
        if not root_subvol_node:
            self._last_error = "Don't have root subvolume?!"
            return False

        uid, gid = os.getuid(), os.getgid()

        t_i, t_ns = self.__newctime_tuple()

        root_node = self.getTable("tree").find_by_inode(llfuse.ROOT_INODE)

        name_id = self.getTable("name").insert(snap_name)
        sz = len(snap_name) + 4 + 13*4 + 4*4
        inode_id = self.getTable("inode").insert(2, self.root_mode, uid, gid, 0, sz, t_i, t_i, t_i, t_ns, t_ns, t_ns)
        self.getTable("tree").insert(root_node["id"], name_id, inode_id)

        subvol_node = self.getTable("tree").find_by_inode(inode_id)

        self.__copy_root_tree_to_new_subvolume(root_subvol_node, subvol_node)

        return True


    # -----------------------------------------------

    def __get_tree_node_by_parent_inode_and_name(self, parent_inode, name):

        name_id = self.getTable("name").find(name)
        if not name_id:
            return None

        par_node = self.getTable("tree").find_by_inode(parent_inode)
        if not par_node:
            return None

        node = self.getTable("tree").find_by_parent_name(par_node["id"], name_id)

        return node

    def __copy_root_tree_to_new_subvolume(self, root_node, subvol_node):
        return

    def __newctime(self): # {{{3
        return time.time()

    def __newctime_tuple(self): # {{{3
        return self.__get_time_tuple( self.__newctime() )

    def __get_time_tuple(self, t): # {{{3
        t_ns, t_i = math.modf(t)
        t_ns = int(t_ns * 10**9)
        return int(t_i), t_ns

