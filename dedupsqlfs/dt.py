# -*- coding: utf8 -*-
"""
Module to work with dates
"""

__author__ = 'sergey'

from datetime import datetime, timedelta, time as dtime

class CleanUpPlan:
    """
    This is class for backup cleanup plan

    If there is many snapshots done by some dates
    and no much space - they need to be removed.
    But some of theme need to stay. By some plan.
    Like:
        - keep 7 daily snapshots
        - keep 4 weekly
        - keep 2 monthly
        - keep 1 yearly
        - remove all other

    So, gather all backup snapshot dates (datetime)
    Send them to instance of this class.
    Set desired cleanup plan.
    Get list of dates which must be keeped or destroyed.
    """

    _dates = None
    _max_daily = 7
    _max_weekly = 4
    _max_monthly = 2
    _max_yearly = 1

    _intervals = None

    _now = None

    def __init__(self, now=None):
        self._dates = []
        self._intervals = {}
        self._now = now
        pass

    def getNow(self):
        if self._now is None:
            self._now = datetime.now()
        return self._now

    def setCleanUpPlan(self, max_daily, max_weekly, max_monthly, max_yearly):
        if type(max_daily) is not int:
            raise ValueError("Value of max_daily must be int")
        if type(max_weekly) is not int:
            raise ValueError("Value of max_weekly must be int")
        if type(max_monthly) is not int:
            raise ValueError("Value of max_monthly must be int")
        if type(max_yearly) is not int:
            raise ValueError("Value of max_yearly must be int")
        self._max_daily = max_daily
        self._max_weekly = max_weekly
        self._max_monthly = max_monthly
        self._max_yearly = max_yearly
        return self

    def setCleanUpPlanDaily(self, max_daily):
        if type(max_daily) is not int:
            raise ValueError("Value of max_daily must be int")
        self._max_daily = max_daily
        return self

    def setCleanUpPlanWeekly(self, max_weekly):
        if type(max_weekly) is not int:
            raise ValueError("Value of max_weekly must be int")
        self._max_weekly = max_weekly
        return self

    def setCleanUpPlanMonthly(self, max_monthly):
        if type(max_monthly) is not int:
            raise ValueError("Value of max_monthly must be int")
        self._max_monthly = max_monthly
        return self

    def setCleanUpPlanYearly(self, max_yearly):
        if type(max_yearly) is not int:
            raise ValueError("Value of max_yearly must be int")
        self._max_yearly = max_yearly
        return self

    def setDates(self, dates):
        if type(dates) not in (tuple, list, set):
            raise ValueError("Value of dates must be iteratable: tuple, list, set")
        self._dates[:] = dates[:]
        return self


    def _setupIntervals(self):
        if self._intervals:
            return self

        now = self.getNow()

        deltaMicSec = timedelta(microseconds=1)
        deltaDay = timedelta(days=1)
        deltaWeek = timedelta(weeks=1)

        # Astro month
        deltaMonth = timedelta(seconds=2630016)
        # Astro year
        deltaYear = timedelta(seconds=31557600)

        self._intervals["days"] = []
        self._intervals["weeks"] = []
        self._intervals["months"] = []
        self._intervals["years"] = []

        todayMax = datetime.combine(now.date(), dtime()) + deltaDay - deltaMicSec
        todayMin = datetime.combine(now.date(), dtime())

        for d in range(self._max_daily):
            self._intervals["days"].append((
                todayMin,
                todayMax,
            ))
            todayMin -= deltaDay
            todayMax -= deltaDay

        weekMax = datetime.combine(now.date(), dtime()) + deltaDay - deltaMicSec
        weekMin = datetime.combine(now.date(), dtime()) - deltaWeek + deltaDay
        for d in range(self._max_weekly):
            self._intervals["weeks"].append((
                weekMin,
                weekMax,
            ))
            weekMin -= deltaWeek
            weekMax -= deltaWeek


        nextM = now.date()
        nextM = nextM.replace(day=1)
        try:
            nextM = nextM.replace(month=nextM.month+1)
        except:
            if nextM.month == 12:
                nextM = nextM.replace(year=nextM.year + 1, month=1)
        currM = now.date()
        currM = currM.replace(day=1)

        monthMax = datetime.combine(nextM, dtime()) - deltaMicSec
        monthMin = datetime.combine(currM, dtime())
        for d in range(self._max_monthly):
            self._intervals["months"].append((
                monthMin,
                monthMax,
            ))
            try:
                monthMin = monthMin.replace(month=monthMin.month-1)
            except:
                if monthMin.month == 1:
                    monthMin = monthMin.replace(year=nextM.year - 1, month=12)

            monthMax = monthMax.replace(day=1)
            monthMax = datetime.combine(monthMax.date(), dtime()) - deltaMicSec


        yearMax = datetime.combine(now.replace(now.year+1, month=1, day=1).date(), dtime()) - deltaMicSec
        yearMin = datetime.combine(now.replace(month=1, day=1).date(), dtime())
        for d in range(self._max_yearly):
            self._intervals["years"].append((
                yearMin,
                yearMax,
            ))
            yearMax = yearMin - deltaMicSec
            yearMin = datetime.combine(yearMin.replace(year=yearMin.year-1).date(), dtime())


        return self


    def _check_daily_numbers(self, cur_date):

        isInDays = 0
        foundRange = None
        for dayRange in self._intervals["days"]:
            dateBegin, dateEnd = dayRange
            if cur_date >= dateBegin and cur_date <= dateEnd:
                isInDays = 1
                foundRange = dayRange
                break

        return isInDays, foundRange

    def _check_weekly_numbers(self, cur_date):

        isInDays = 0
        foundRange = None
        for dayRange in self._intervals["weeks"]:
            dateBegin, dateEnd = dayRange
            if cur_date >= dateBegin and cur_date <= dateEnd:
                isInDays = 1
                foundRange = dayRange
                break

        return isInDays, foundRange

    def _check_monthly_numbers(self, cur_date):

        isInDays = 0
        foundRange = None
        for dayRange in self._intervals["months"]:
            dateBegin, dateEnd = dayRange
            if cur_date >= dateBegin and cur_date <= dateEnd:
                isInDays = 1
                foundRange = dayRange
                break

        return isInDays, foundRange

    def _check_yearly_numbers(self, cur_date):

        isInDays = 0
        foundRange = None
        for dayRange in self._intervals["years"]:
            dateBegin, dateEnd = dayRange
            if cur_date >= dateBegin and cur_date <= dateEnd:
                isInDays = 1
                foundRange = dayRange
                break

        return isInDays, foundRange

    def getCleanedUpList(self):
        """
        @return: list of saved dates
        @rtype: list
        """
        self._setupIntervals()

        cleaned = []

        dayRanges = {}

        for d in self._dates:

            chkD, rangeD = self._check_daily_numbers(d)
            if chkD:
                if dayRanges.get(rangeD, None) is None:
                    dayRanges[rangeD] = []
                dayRanges[ rangeD ].append(d)

            chkD, rangeD = self._check_weekly_numbers(d)
            if chkD:
                if dayRanges.get(rangeD, None) is None:
                    dayRanges[rangeD] = []
                dayRanges[ rangeD ].append(d)

            chkD, rangeD = self._check_monthly_numbers(d)
            if chkD:
                if dayRanges.get(rangeD, None) is None:
                    dayRanges[rangeD] = []
                dayRanges[ rangeD ].append(d)

            chkD, rangeD = self._check_yearly_numbers(d)
            if chkD:
                if dayRanges.get(rangeD, None) is None:
                    dayRanges[rangeD] = []
                dayRanges[ rangeD ].append(d)

        for dateRange, dateList in dayRanges.items():

            dateList.sort()

            dt = dateRange[1] - dateRange[0]
            # Get most recent on day, but most early on other ranges
            if dt <= timedelta(days=1):
                cleaned.append(dateList[-1])
            else:
                cleaned.append(dateList[0])

        cleaned = list(set(cleaned))
        cleaned.sort()

        return cleaned

    def getRemovedList(self):
        """
        @return: list of cleaned out dates
        @rtype: list
        """

        self._setupIntervals()

        removed = []

        cleaned = set(self.getCleanedUpList())

        for d in self._dates:
            if d in cleaned:
                continue
            removed.append(d)

        return removed

    pass

