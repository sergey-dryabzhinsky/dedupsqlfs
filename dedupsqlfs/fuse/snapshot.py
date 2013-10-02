# -*- coding: utf8 -*-

__author__ = 'sergey'

import llfuse
from datetime import datetime
from dedupsqlfs.fuse.subvolume import Subvolume

class Snapshot(Subvolume):

    def make(self, from_subvol, with_name):
        return

    def remove_older_than(self, dateStr):

        oldDate = datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S")

        fh = self.getManager().opendir(llfuse.ROOT_INODE)

        print("Subvolumes:")
        print("-"*79)
        print("%-50s| %-29s" % ("Name", "Created"))
        print("-"*79)

        for name, attr, node in self.getManager().readdir(fh, 0):
            if name == '@root':
                continue

            subvolDate = datetime.fromtimestamp(attr.st_ctime)
            if subvolDate < oldDate:
                self.remove(name)

        self.getManager().releasedir(fh)

        return