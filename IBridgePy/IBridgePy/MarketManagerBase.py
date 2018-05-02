# -*- coding: utf-8 -*-
"""
Module MarketManager

"""
import datetime as dt
from IBridgePy.quantopian import MachineStateClass
from BasicPyLib.handle_calendar import MarketCalendar
from BasicPyLib.small_tools import _match
from sys import exit
import time
                 
class MarketManager(object):
    """ 
    Market Manager will run trading strategies according to the market hours.
    It should contain a instance of IB's client, properly initialize the connection
    to IB when market starts, and properly close the connection when market closes
    inherited from __USEasternMarketObject__. 
    USeasternTimeZone and run_according_to_market() are inherited
    init_obj(), run_algorithm() and destroy_obj() should be overwritten
    """    
    def __init__(self, trader, host='', port=7496, clientId=99, version='regular'):
        self.host=host
        self.port=port
        self.clientId=clientId
        self.aTrader=trader
        self.marketState = MachineStateClass()
        self.marketState.set_state(self.marketState.SLEEP)
        self.aTrader.log.notset(__name__+'::__init__')
        
        self.lastCheckConnectivityTime=dt.datetime.now()
        self.numberOfConnection=0
        
        # decide if check connectivity every 30 seconds
        self.checkConnectivityFlag=False
        
        # when disconnect between IBridgePY and IB Gateway/TWS, 
        # decide if auto re-connect, try 3 times
        self.autoConnectionFlag=False 
        
        # a flag to know if before_trading_start should run
        self.beforeTradingStartFlag = True
        
        # get market open/close time every day
        # run_like_quantopian will run following these times
        self.marketCalendar = MarketCalendar()
        self.runTraderToday = False
        self.todayMarketOpenTime = None
        self.todayMarketCloseTime = None
        
        # mode
        self.testMode = False
           
        # Record the last display message
        # When a new message is to be displayed, it will compare with the last
        # message. If they are not same, then display it
        self.lastDisplayMessage = ''
        
        # 
        #if version == 'regular':
        #    self.func = self.run_regular()
        #elif version == 'run_like_quantopian':
        #    self.func = self.run_like_quantopian()
            
    ######### this part do real trading with IB
    def init_obj(self):
        """
        initialzation of the connection to IB
        updated account
        """   
        self.aTrader.log.notset(__name__+'::init_obj')
        self.aTrader.disconnect()
        self.numberOfConnection+=1
        if self.aTrader.connect(self.host, self.port, self.clientId): # connect to server
            self.aTrader.log.debug(__name__ + ": " + "Connected to IB, port = " + 
            str(self.port) + ", ClientID = " + str(self.clientId))
            self.numberOfConnection=0 # reset counter after a successful connection
        else:
            self.aTrader.log.error(__name__+'::init_obj: Not connected')
        time.sleep(1)
                    
    def destroy_obj(self):
        """
        disconnect from IB and close log file
        """
        self.aTrader.log.info('IBridgePy: Disconnect')
        self.aTrader.disconnect()
    
    def run(self, func='run_regular'):
        self.aTrader.log.debug(__name__+'::run: START')
        self.init_obj()
        if func == 'run_regular':
            func = self.run_regular
        elif func == 'run_like_quantopian':
            func = self.run_like_quantopian
            self.aTrader.repBarFreq = 60
        else:
            print(__name__ + '::run: EXIT, cannot handle func = % s' %(func))
            exit()
        if self.aTrader.isConnected():
            self.aTrader.connectionGatewayToServer=True
            self.aTrader.initialize_Function() 
            while (not self.aTrader.wantToEnd):
                func()
        if not self.autoConnectionFlag:
            self.destroy_obj()
                     
    def run_regular(self):
        self.aTrader.log.notset(__name__+'::run_regular')
        self.aTrader.processMessages()
        if self.aTrader.connectionGatewayToServer:
            self.aTrader.repeat_Function()
            # Recording timeNow is moved to inside repeat_Function
            #self.aTrader.stime_previous = self.aTrader.get_datetime() #Important!!!
            if not self.testMode: #real mode
                time.sleep(1)
        else:
            if not self.testMode:
                time.sleep(1)
                 
    def run_like_quantopian(self):
        self.aTrader.log.notset(__name__+'::run_like_quantopian')
        # a new day start, calculate if today is a schedued day
        #if yes run handle_date
        #if not, run processMessage() only
        timeNow = self.aTrader.get_datetime()
        #print (__name__, timeNow, self.aTrader.stime_previous)
        
        # handling before_trading_start
        # it runs at 9:20AM every day.
        # If want to change the run time, remember to change the 
        # random data creator, 
        # otherwise, test mode will not run before_trading_start
        if self.aTrader.before_trading_start_quantopian \
        and timeNow.hour == 9 and timeNow.minute==20 \
        and self.beforeTradingStartFlag:
            self.aTrader.before_trading_start_quantopian(self.aTrader.context, self.aTrader.qData)
            self.beforeTradingStartFlag = False
            self.aTrader.stime_previous = timeNow #Important!!!
        if self.aTrader.before_trading_start_quantopian \
        and timeNow.hour == 9 and timeNow.minute==21 \
        and self.beforeTradingStartFlag == False:
            self.beforeTradingStartFlag = True
            self.aTrader.stime_previous = timeNow #Important!!!
       
        # At begining of a day, check if or not run trader today
        if timeNow.day != self.aTrader.stime_previous.day:
            self.runTraderToday = self.check_date_rules(timeNow.date(),
                    self.aTrader.scheduledFunctionList)
            
            # check what is scheduledFunctionList
            #for x in self.aTrader.scheduledFunctionList:
            #    print (x)
            
            # not run trade today, then sleep
            # else get today market open/close time
            if not self.runTraderToday:
                if not self.testMode:
                    self.aTrader.log.info(__name__+'::run_like_quantopian: %s not a trading day' %(str(timeNow.date()),))       
                    self.aTrader.log.info(__name__+'::run_like_quantopian: IBridgePy is still running')       
                self._slow_mode(timeNow)
                return       
            
            # get market open and close time
            # to decide if handle_data should run
            self.todayMarketOpenTime, self.todayMarketCloseTime = \
                    self.marketCalendar.get_market_open_close_time(timeNow)
            #print (timeNow)
            #print (self.todayMarketOpenTime, self.todayMarketCloseTime)
        if not self.todayMarketOpenTime:
            self.aTrader.log.debug(__name__+'::run_like_quantopian: market close %s' %(str(timeNow),))
            self._slow_mode(timeNow)
            return
        
        # if time is inbetween open and close, then run handle_data 
        # else sleep 1 second
        if self.todayMarketOpenTime <= timeNow < self.todayMarketCloseTime: 
            #print (timeNow)
            self.run_regular()
            #self.aTrader.stime_previous = timeNow #Important!!!
        else:
            self._display_message('Market is closed but IBridgePy is still running')
            if not self.testMode:
                self._slow_mode(timeNow)
            else:
                self.aTrader.stime_previous = timeNow #Important!!!
                
    def _display_message(self, message):           
        if message != self.lastDisplayMessage:
            print('MarketManager::' + message)
            self.lastDisplayMessage = message
        
    def _slow_mode(self, timeNow):
        self.aTrader.processMessages()
        time.sleep(1)
        self.aTrader.stime_previous = timeNow #Important!!!
        
    def runOnEvent(self):
        self.aTrader.log.debug(__name__+'::runOnEvent')
        self.init_obj()
        if self.aTrader.repBarFreq%5!=0:
            self.aTrader.log.error(__name__+'::runOnEvent: EXIT, cannot handle reqBarFreq=%s' %(str(self.aTrader.repBarFreq),))
            exit()
        self.aTrader.initialize_Function()
        while self.aTrader.realtimeBarTime==None:
            time.sleep(0.2)
            self.aTrader.processMessages()
            self.aTrader.log.notset(__name__+'::runOnEvent: waiting realtimeBarTime is called back')
            
        while self.aTrader.realtimeBarTime.second!=55:
        # when the realtimeBarTime.second==55 comes in, the IB server time.second==0
        # start the handle_data when second ==0
        # Set realtimeBarCount=0
        # realtimeBarCount+=1 when a new bar comes in
            time.sleep(0.2)
            self.aTrader.processMessages()
        self.aTrader.realtimeBarCount=0
        self.aTrader.event_Function()
        
        while(True):
            self.aTrader.processMessages()
            self.aTrader.event_Function()
            time.sleep(1)
 
    def run_auto_connection(self, tryTimes=3):
        self.aTrader.wantToEnd=False
        self.autoConnectionFlag=True
        self.checkConnectivityFlag=True
        while self.numberOfConnection<=tryTimes:
            self.run()
            if self.aTrader.wantToEnd:
                break
            else:
                self.aTrader.log.error(__name__+'::run_auto_connection:wait 30\
                seconds to reconnect')
                self.aTrader.connectionGatewayToServer=False
                time.sleep(30)
                if self.numberOfConnection>tryTimes:
                    break
        if not self.aTrader.wantToEnd:
            print (__name__+'::run_auto_connection: END. tried 3 times\
            but cannot conect to Gateway.' ) 
        else:
            print (__name__+'::run_auto_connection: END')             

    def check_connectivity(self):
        if self.aTrader.connectionGatewayToServer==False:
            return True
            
        setTimer=dt.datetime.now()
        #print (setTimer-self.lastCheckConnectivityTime).total_seconds()
        if (setTimer-self.lastCheckConnectivityTime).total_seconds()<30:
            return True
        self.aTrader.log.debug(__name__+'::check_connectivity')
        self.aTrader.nextId=None
        self.aTrader.reqIds(0)
        checkTimer=dt.datetime.now()
        while (checkTimer-setTimer).total_seconds()<0.5:
            self.aTrader.processMessages()
            if self.aTrader.nextId!=None:
                self.lastCheckConnectivityTime=checkTimer
                self.aTrader.log.debug(__name__+'::check_connectivity:GOOD')
                return True
            self.aTrader.log.debug(__name__+'::check_connectivity:checking ...')
            time.sleep(0.05)
            checkTimer=dt.datetime.now()
        self.aTrader.log.debug(__name__+'::check_connectivity:BAD')
        return False

    def check_date_rules(self, aDay, scheduledFunctionList):
        '''
        Input:
        aDay: dt.date only for faster
        
        Algo:
        if schedule_funtion is [], then run everyday
        else, strictly follow schedule_function defines !!! IMPORTANT
        
        Output:
        set self.runToday to True(run repeat_func today) or False 
        '''
        self.aTrader.log.debug(__name__+'::check_date_rules: aDay=%s' %(str(aDay),))
        #if type(aDay) == dt.datetime:
        #    aDay = aDay.date()
        self.aTrader.monthDay = self.marketCalendar.nth_trading_day_of_month(aDay)
        self.aTrader.weekDay = self.marketCalendar.nth_trading_day_of_week(aDay)
        #print (monthDay, weekDay)
        if self.aTrader.monthDay == None or self.aTrader.weekDay == None:
            self.aTrader.log.debug(__name__+'::check_date_rules: %s = not trading date' %(str(aDay),))
            return False
        else:
            if scheduledFunctionList==[]:
                return True
            for ct in scheduledFunctionList:
                #print (ct.onNthMonthDay, ct.onNthWeekDay)
                if _match(ct.onNthMonthDay, self.aTrader.monthDay, 'monthWeek')\
                         and _match(ct.onNthWeekDay, self.aTrader.weekDay, 'monthWeek'):
                    return True
            self.aTrader.log.debug(__name__+'::check_date_rules: %s = not trading date' %(str(aDay),))
            return False
        
                
if __name__=='__main__':                            
    c=test()
    d=MarketManager(c)
    d.init_obj()
          