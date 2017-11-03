# -*- coding: utf8 -*-
"""
Module to work with dates
"""

__author__ = 'sergey'

import math

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

    def __init__(self):
        self._dates = []
        pass

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


    def _check_daily_numbers(self, cur_date):
        last = self._dates[-1]

        dt = last - cur_date
        dc = dt.days

        score = 1
        if dc >= self._max_daily:
            score -= 1

        return score > 0

    def _check_weekly_numbers(self, cur_date):
        last = self._dates[-1]

        dt = last - cur_date
        dc = dt.days
        wc = int( math.floor(dc / 7.0) )

        score = 1
        if wc >= self._max_weekly + 1:
            score -= 1
        elif wc == 1 and dc < self._max_daily:
            score -= 1

        weeks = {}
        for d in self._dates:
            dt = last - d
            wn = int( math.floor(dt.days / 7.0) )
            if wn not in weeks:
                weeks[ wn ] = set()
            weeks[ wn ].add( d )

        for wn in weeks:
            s = weeks[ wn ]
            if cur_date in s:
                if len(s) > 1:
                    # keep most old
                    l = list(s)
                    l.sort(reverse=True)
                    if l[-1] != cur_date:
                        score -= 1

        return score > 0

    def _check_monthly_numbers(self, cur_date):
        last = self._dates[-1]

        mc = (last.year - cur_date.year)*12 + last.month - cur_date.month
        dt = last - cur_date
        dc = dt.days
        wc = int( math.floor(dc / 7.0) )

        score = 1
        if mc >= self._max_monthly + 1:
            score -= 1
        elif mc == 1 and wc < self._max_weekly:
            score -= 1

        months = {}
        for d in self._dates:
            mn = (last.year - d.year)*12 + last.month - d.month
            if mn not in months:
                months[ mn ] = set()
            months[ mn ].add( d )

        for mn in months:
            s = months[ mn ]
            if cur_date in s:
                if len(s) > 1:
                    # keep most old
                    l = list(s)
                    l.sort(reverse=True)
                    if l[-1] != cur_date:
                        score -= 1

        return score > 0

    def _check_yearly_numbers(self, cur_date):
        last = self._dates[-1]

        mc = (last.year - cur_date.year)*12 + last.month - cur_date.month
        yc = last.year - cur_date.year

        score = 1
        # wee need more distance
        if yc >= self._max_yearly + 1:
            score -= 1
        # or at least one year diff
        elif yc == 1 and mc < self._max_monthly:
            score -= 1

        years = {}
        for d in self._dates:
            yn = last.year - d.year
            if yn not in years:
                years[ yn ] = set()
            years[ yn ].add( d )

        for yn in years:
            s = years[ yn ]
            if cur_date in s:
                if len(s) > 1:
                    # keep most old
                    l = list(s)
                    l.sort(reverse=True)
                    if l[0] != cur_date:
                        score -= 1

        return score > 0

    def getCleanedUpList(self):
        """
        @return: list of saved dates
        @rtype: list
        """

        cleaned = []

        for d in self._dates:
            chk = 0
            chk += self._check_daily_numbers(d)
            chk += self._check_weekly_numbers(d)
            chk += self._check_monthly_numbers(d)
            chk += self._check_yearly_numbers(d)

            if not chk:
                continue
            cleaned.append(d)

        return cleaned

    def getRemovedList(self):
        """
        @return: list of cleaned out dates
        @rtype: list
        """

        removed = []

        for d in self._dates:
            chk = 0
            chk += self._check_daily_numbers(d)
            chk += self._check_weekly_numbers(d)
            chk += self._check_monthly_numbers(d)
            chk += self._check_yearly_numbers(d)

            if chk:
                continue
            removed.append(d)

        return removed

    pass

