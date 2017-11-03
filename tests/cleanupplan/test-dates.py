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

cleanUp = CleanUpPlan()
cleanUp.setCleanUpPlanDaily(14)
cleanUp.setCleanUpPlanWeekly(8)
cleanUp.setCleanUpPlanMonthly(6)
cleanUp.setCleanUpPlanYearly(2)

now = datetime.now()
dates = [now]

for x in range(1, 750):

    dt = timedelta(days=x)

    dates.append(now - dt)

dates.sort()
cleanUp.setDates(dates)

cleanedDates = cleanUp.getCleanedUpList()

pprint(cleanedDates)

# Add one more day

dt = timedelta(days=1)

dates = cleanedDates

dates.append(now + dt)

dates.sort()
cleanUp.setDates(dates)

cleanedDates = cleanUp.getCleanedUpList()

pprint(cleanedDates)

