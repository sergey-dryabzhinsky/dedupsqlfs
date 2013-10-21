# -*- coding: utf8 -*-

__author__ = 'sergey'

import llfuse
from datetime import datetime
from dedupsqlfs.fuse.subvolume import Subvolume

class Snapshot(Subvolume):

    def make(self, from_subvol, with_name):

        if not self.create(with_name):
            self.getManager().getLogger().error("Subvolume with name %r already exists! Can't snapshot into it!", with_name)
            return

        return

    def remove_older_than(self, dateStr):

        oldDate = datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S")

        fh = self.getManager().opendir(llfuse.ROOT_INODE)

        for name, attr, node in self.getManager().readdir(fh, 0):
            subvolDate = datetime.fromtimestamp(attr.st_ctime)
            if subvolDate < oldDate:
                self.getManager().getLogger().info("Remove %r subvolume", name)
                self.remove(name)
            else:
                self.getManager().getLogger().info("Skip %r subvolume", name)

        self.getManager().releasedir(fh)

        return