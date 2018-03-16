#/usr/bin/env python3

import sys
import os

dirname = "dedupsqlfs"

# Figure out the directy which is the prefix
# path-of-current-file/..
curpath = os.path.abspath( sys.argv[0] )
if os.path.islink(curpath):
    curpath = os.readlink(curpath)
currentdir = os.path.dirname( curpath )
basedir = os.path.abspath( os.path.join( currentdir, "..", ".." ) )

sys.path.insert( 0, basedir )
os.chdir(basedir)

from pprint import pprint
from datetime import datetime, timedelta
from dedupsqlfs.dt import CleanUpPlan

daysToTest = 800

today = datetime.now() - timedelta(days=daysToTest)
dates = []

# Days to test
for x in range(1, daysToTest):

    dt = timedelta(days=1)

    today += dt

    print("\nToday: %s" % today)

    dates.append(today)

    cleanUp = CleanUpPlan(today)
    cleanUp.setCleanUpPlanDaily(14)
    cleanUp.setCleanUpPlanWeekly(8)
    cleanUp.setCleanUpPlanMonthly(6)
    cleanUp.setCleanUpPlanYearly(2)

    dates.sort()

    print("Dates to clean:")
    pprint(dates)

    cleanUp.setDates(dates)

    cleanedDates = cleanUp.getCleanedUpList()

    print("Cleaned dates:")
    pprint(cleanedDates)

    removedDates = cleanUp.getRemovedList()

    print("Removed dates:")
    pprint(removedDates)

    dates = cleanedDates

    print("")

