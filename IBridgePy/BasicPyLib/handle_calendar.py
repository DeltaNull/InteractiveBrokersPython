# -*- coding: utf-8 -*-
"""
Created on Fri Jun  2 17:45:12 2017

@author: IBridgePy@gmail.com
"""

'''
from pandas.tseries.holiday import get_calendar, HolidayCalendarFactory, GoodFriday
from datetime import datetime

cal = get_calendar('USFederalHolidayCalendar')  # Create calendar instance
print (cal.rules)
cal.rules.pop(7)                                # Remove Veteran's Day rule
cal.rules.pop(6)                                # Remove Columbus Day rule
tradingCal = HolidayCalendarFactory('TradingCalendar', cal, GoodFriday)
print (tradingCal.rules)

#new instance of class
cal1 = tradingCal()

print (cal1.holidays(datetime(2014, 12, 31), datetime(2016, 12, 31)))
'''

import datetime as dt
import pandas as pd

from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, nearest_workday, \
    USMartinLutherKingJr, USPresidentsDay, GoodFriday, USMemorialDay, \
    USLaborDay, USThanksgivingDay
from pandas.tseries.offsets import MonthEnd
#import pandas_market_calendars as mcal
import numpy as np
import pytz


class USTradingCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('NewYearsDay', month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday('USIndependenceDay', month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25, observance=nearest_workday)
    ]

class MarketCalendar(object):
    def get_trading_close_holidays(self, startDay, endDay):
        # startDay and endDay are inclusive
        # if startDay or endDay is a holiday, it will show up in the result
        inst = USTradingCalendar()
        return inst.holidays(startDay, endDay)
    
    def trading_day(self, day):
        '''
        return True if day is a trading day
        '''
        # Monday weekday=0
        # Sunday weekday=6
        #print (day)
        if day.weekday()>=5: # weekends are not trading day
            return False
        # check if day->day+1 has holidays and if day == holiday
        return not pd.Timestamp(day) in self.get_trading_close_holidays(day, day+dt.timedelta(days=1)) 
    
    def nth_trading_day_of_week(self, aDay):
        if type(aDay)==dt.datetime:
            aDay=aDay.date()
        if not self.trading_day(aDay):
            return 'marketClose' # day is not a trading day
        #if day is a trading day, return Nth trading day of the week
        # 0 is 1st trading day of week
        tmp=aDay.weekday()
        start=aDay-dt.timedelta(days=tmp)
        end=start+dt.timedelta(days=4)
        for ct in self.get_trading_close_holidays(start, end):
            if ct<pd.Timestamp(aDay):
                tmp-=1
        sm=self.count_trading_days_in_a_week(aDay)
        return tmp,-(sm-tmp)
     
    def nth_trading_day_of_month(self, aDay):
        if type(aDay) == dt.datetime:
            aDay = aDay.date()
        if not self.trading_day(aDay):
            return 'marketClose' # day is not a trading day
        #if day is a trading day, return Nth trading day of the month
        # 0 is 1st trading day of month
        tmp = aDay.day
        ans = tmp - 1
        start = aDay.replace(day = 1)
        i = 0
        while i < tmp:
            if not self.trading_day(start + dt.timedelta(days = i)):
                ans -= 1
            i += 1
        sm = self.count_trading_days_in_a_month(aDay)
        return ans,-(sm - ans)
    
    def count_trading_days(self, startDay, endDay):
        '''
        include startDay and endDay
        '''
        ans=0
        i=0
        tmp=startDay+dt.timedelta(days=i)
        while tmp<=endDay:
            if self.trading_day(startDay+dt.timedelta(days=i)):
                ans+=1
            i+=1
            tmp=startDay+dt.timedelta(days=i)
        return ans
    
    def count_trading_days_in_a_month(self, aDay):
        tmp=(aDay+MonthEnd(0)).date() # change pd.TimeStampe to dt.date  
        return self.count_trading_days(aDay.replace(day=1), tmp )
    
    def count_trading_days_in_a_week(self, aDay):
        # Monday weekday=0
        # Sunday weekday=6
        if type(aDay)==dt.datetime:
            aDay=aDay.date()
        tmp=aDay.weekday()
        start=aDay-dt.timedelta(days=tmp)
        end=start+dt.timedelta(days=4)
        return self.count_trading_days(start,end)
    
    def get_params_of_a_daytime(self, dayTime):
        return (self.nth_trading_day_of_month(dayTime),
                self.nth_trading_day_of_week(dayTime),
                dayTime.hour,
                dayTime.minute)
    
    def switch_goBack(self, startTime, endTime):
        if startTime>=endTime:
            print(__name__+'::switch_time: EXIT, startTime >= endTime')
            print ('startTime=',startTime)
            print ('endTime=', endTime)
            exit()
        #return str((endTime-startTime).days+1)+' D'
        return str(self.count_trading_days(startTime, endTime))+' D'
        
    def get_market_open_close_time(self, aDatetime):
        if self.trading_day(aDatetime):
            open = aDatetime.replace(hour=9, minute=30)
            close = aDatetime.replace(hour=16, minute=0)
            return open, close
        else:
            return None, None



   
def get_early_closes(start_date, end_date):
    a=[
    [dt.date(2017, 7, 3), 
     dt.datetime(2017,7,3,13,30,0, tzinfo=pytz.timezone('UTC')),
     dt.datetime(2017,7,3,17,0,0, tzinfo=pytz.timezone('UTC'))],
    [dt.date(2017, 11, 24), 
     dt.datetime(2017,11,24,14,30,0, tzinfo=pytz.timezone('UTC')),
     dt.datetime(2017,11,24,18,0,0, tzinfo=pytz.timezone('UTC'))],
    [dt.date(2018, 7, 3),
     dt.datetime(2018,7,3,13,30,0, tzinfo=pytz.timezone('UTC')),
     dt.datetime(2018,7,3,17,0,0, tzinfo=pytz.timezone('UTC'))],
    [dt.date(2018, 11, 23),
     dt.datetime(2018,11,23,14,30,0, tzinfo=pytz.timezone('UTC')),
     dt.datetime(2018,11,23,18,0,0, tzinfo=pytz.timezone('UTC'))],
    [dt.date(2018, 12, 24),
     dt.datetime(2018,12,24,14,30,0, tzinfo=pytz.timezone('UTC')),
     dt.datetime(2018,12,24,18,0,0, tzinfo=pytz.timezone('UTC'))],
    ]
    a = pd.DataFrame(a, columns=['date','market_open', 'market_close'])
    a = a.set_index(pd.DatetimeIndex(a['date']))
    del a['date']
    return a[(start_date <= a.index) & (a.index <= end_date)].index

if __name__ == '__main__':
    #print ('start')
    #print(get_trading_close_holidays(dt.date(2017,4,1), dt.date(2017,4,30)))
    #print(trading_day(dt.date(2017,9,1)))
    #print (nth_trading_day_of_week(dt.date(2017,11,30)))
    #print (nth_trading_day_of_month(dt.date(2017,9,1)))
    #print (count_trading_days(dt.date(2017,5,1), dt.date(2017,5,31)))
    #print (dt.date(2017,4,13)+MonthEnd(1))
    #print (count_trading_days(dt.date(2017,4,1), dt.date(2017,4,13)+MonthEnd(1)))
    #print (count_trading_days_in_a_month(dt.date(2017,5,13)))
    #print (count_trading_days_in_a_week(dt.date(2017,6,1)))
    #print (get_params_of_a_daytime(dt.datetime.now()))
    #print (get_params_of_a_daytime(dt.datetime(2017,5,30,12,30)))
    
    a = pytz.timezone('US/Eastern').localize(dt.datetime(2017, 11, 30, 17, 0))
    c = MarketCalendar()
    print (c.get_market_open_close_time(a))
    #print (c.nth_trading_day_of_month(dt.date(2017,11,30)))
    #print (c.nth_trading_day_of_week(dt.date(2017,11,30)))
    print (c.get_params_of_a_daytime(a))