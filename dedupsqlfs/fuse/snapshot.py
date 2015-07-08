# -*- coding: utf8 -*-

__author__ = 'sergey'

from time import time
from datetime import datetime
from dedupsqlfs.fuse.subvolume import Subvolume
from dedupsqlfs.lib import constants

class Snapshot(Subvolume):

    def make(self, from_subvol, with_name):
        """
        Copy all tree,inode,index,link data from one subvolume to new
        """

        if not from_subvol:
            self.getLogger().error("Select subvolume from which you need to create snapshot!")
            return

        if not with_name:
            self.getLogger().error("Define name for snapshot to which you need to copy %r data!" % from_subvol)
            return

        subvol_from = from_subvol
        subvol_to = with_name

        tableSubvol = self.getTable('subvolume')
        subvolItemTo = tableSubvol.find(subvol_to)
        if subvolItemTo:
            self.getLogger().error("Snapshot or subvolume with name %r already exists! Can't create snapshot into it!", with_name)
            return
        else:
            # New subvol
            subvol_id = tableSubvol.insert(subvol_to, int(time()))
            tableSubvol.readonly(subvol_id)
            subvolItemTo = tableSubvol.get(subvol_id)

        subvolItemFrom = tableSubvol.find(subvol_from)

        tableSubvol.update_time(subvolItemTo["id"], subvolItemFrom["updated_at"])

        self.getManager().getManager().commit()

        self.getLogger().debug("Use subvolume: %r" % subvol_from)
        self.getLogger().debug("Into subvolume: %r" % subvol_to)

        for tName in ("tree", "inode", "link", "xattr", "inode_hash_block", "inode_option",):

            self.print_msg("Copy table: %r\n" % tName)
            self.getManager().getManager().copy(
                tName + "_%s" % subvolItemFrom["hash"],
                tName + "_%s" % subvolItemTo["hash"]
            )

        self.print_msg("Done\n")

        self.getManager().getManager().commit()

        return

    def remove_older_than(self, dateStr, use_last_update_time=False):

        oldDate = datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S")

        tableSubvol = self.getTable('subvolume')

        for subvol_id in tableSubvol.get_ids():

            subvol = tableSubvol.get(subvol_id)

            if subvol["name"] == constants.ROOT_SUBVOLUME_NAME:
                continue

            if not use_last_update_time:
                subvolDate = datetime.fromtimestamp(subvol["created_at"])
            else:
                subvolDate = datetime.fromtimestamp(subvol["updated_at"])

            if subvolDate < oldDate:
                self.print_msg("Remove %r snapshot\n" % subvol["name"])
                self.remove(subvol["name"])

        return

    def count_older_than(self, dateStr, use_last_update_time=False):

        oldDate = datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S")

        tableSubvol = self.getTable('subvolume')

        cnt = 0

        for subvol_id in tableSubvol.get_ids():

            subvol = tableSubvol.get(subvol_id)

            if subvol["name"] == constants.ROOT_SUBVOLUME_NAME:
                continue

            if not use_last_update_time:
                subvolDate = datetime.fromtimestamp(subvol["created_at"])
            else:
                subvolDate = datetime.fromtimestamp(subvol["updated_at"])

            if subvolDate < oldDate:
                cnt += 1

        self.print_msg("Count old snapshots: ")
        self.print_out("%s\n" % cnt)
        return

    pass
