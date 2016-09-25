# -*- coding: utf8 -*-

__author__ = 'sergey'

import re
from time import time
from datetime import datetime
from dedupsqlfs.fuse.subvolume import Subvolume
from dedupsqlfs.lib import constants
from dedupsqlfs.dt import CleanUpPlan

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
        if subvolItemFrom["stats_at"] and subvolItemFrom["stats"]:
            tableSubvol.stats_time(subvolItemTo["id"], subvolItemFrom["stats_at"])
            tableSubvol.set_stats(subvolItemTo["id"], subvolItemFrom["stats"])

        self.getManager().getManager().commit()

        self.getLogger().debug("Use subvolume: %r" % subvol_from)
        self.getLogger().debug("Into subvolume: %r" % subvol_to)

        for tName in ("tree", "inode", "link", "xattr", "inode_hash_block", "inode_option",):

            self.print_msg("Copy table: %r\n" % tName)

            self.getManager().getManager().setCompressionProg(
                self.getManager().getOption("sqlite_compression_prog")
            )

            self.getManager().getManager().copy(
                tName + "_%s" % subvolItemFrom["hash"],
                tName + "_%s" % subvolItemTo["hash"],
                True
            )

        self.print_msg("Done\n")

        self.getManager().getManager().commit()
        self.getManager().getManager().close()

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

    def _parseCleanUpPlan(self, cleanUpPlanStr):
        plan = {
            "daily": None,
            "weekly": None,
            "monthly": None,
            "yearly": None
        }

        _ = cleanUpPlanStr.split(",")
        rx = re.compile("\d+")

        for part in _:

            dig = rx.findall(part)
            if not len(dig):
                continue

            if part.find("d") != -1 or part.find("day") != -1:
                plan["daily"] = int(dig[0])
            if part.find("w") != -1 or part.find("week") != -1:
                plan["weekly"] = int(dig[0])
            if part.find("m") != -1 or part.find("month") != -1:
                plan["monthly"] = int(dig[0])
            if part.find("y") != -1 or part.find("year") != -1:
                plan["yearly"] = int(dig[0])

        return plan

    def _getCleanUpPlanObject(self, cleanUpPlanStr):
        cleanUp = CleanUpPlan()

        plan = self._parseCleanUpPlan(cleanUpPlanStr)
        if plan["daily"] is not None:
            cleanUp.setCleanUpPlanDaily(plan["daily"])
        if plan["weekly"] is not None:
            cleanUp.setCleanUpPlanWeekly(plan["weekly"])
        if plan["monthly"] is not None:
            cleanUp.setCleanUpPlanMonthly(plan["monthly"])
        if plan["yearly"] is not None:
            cleanUp.setCleanUpPlanYearly(plan["yearly"])

        return cleanUp

    def remove_plan(self, cleanUpPlanStr, use_last_update_time=False):

        datesCleanUp = self._getCleanUpPlanObject(cleanUpPlanStr)

        tableSubvol = self.getTable('subvolume')

        dates = []

        for subvol_id in tableSubvol.get_ids():

            subvol = tableSubvol.get(subvol_id)

            if subvol["name"] == constants.ROOT_SUBVOLUME_NAME:
                continue

            if not use_last_update_time:
                subvolDate = datetime.fromtimestamp(subvol["created_at"])
            else:
                subvolDate = datetime.fromtimestamp(subvol["updated_at"])

            dates.append(subvolDate)

        dates.sort()
        datesCleanUp.setDates(dates)
        removeDates = datesCleanUp.getRemovedList()

        for subvol_id in tableSubvol.get_ids():

            subvol = tableSubvol.get(subvol_id)

            if subvol["name"] == constants.ROOT_SUBVOLUME_NAME:
                continue

            if not use_last_update_time:
                subvolDate = datetime.fromtimestamp(subvol["created_at"])
            else:
                subvolDate = datetime.fromtimestamp(subvol["updated_at"])

            if subvolDate in removeDates:
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

    def count_plan(self, cleanUpPlanStr, use_last_update_time=False):

        datesCleanUp = self._getCleanUpPlanObject(cleanUpPlanStr)

        tableSubvol = self.getTable('subvolume')

        dates = []

        for subvol_id in tableSubvol.get_ids():

            subvol = tableSubvol.get(subvol_id)

            if subvol["name"] == constants.ROOT_SUBVOLUME_NAME:
                continue

            if not use_last_update_time:
                subvolDate = datetime.fromtimestamp(subvol["created_at"])
            else:
                subvolDate = datetime.fromtimestamp(subvol["updated_at"])

            dates.append(subvolDate)

        dates.sort()
        datesCleanUp.setDates(dates)
        removeDates = datesCleanUp.getRemovedList()

        cnt = len(removeDates)

        self.print_msg("Count old snapshots: ")
        self.print_out("%s\n" % cnt)
        return

    pass
