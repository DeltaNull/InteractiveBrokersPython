# -*- coding: utf-8 -*-
'''
Created on Sat Feb 10 09:26:48 2018

@author: IBridgePy

There is a risk of loss in stocks, futures, forex and options trading. Please
trade with capital you can afford to lose. Past performance is not necessarily 
indicative of future results. Nothing in this computer program/code is intended
to be a recommendation to buy or sell any stocks or futures or options or any 
tradable securities. 
All information and computer programs provided is for education and 
entertainment purpose only; accuracy and thoroughness cannot be guaranteed. 
Readers/users are solely responsible for how they use the information and for 
their results.

If you have any questions, please send email to IBridgePy@gmail.com
'''
from sys import exit
import pandas as pd
import os

def search_security_in_file(df, secType, symbol, currency, param, waive=False):
    if secType=='CASH':
        if param=='exchange':
            return 'IDEALPRO'
        elif param=='primaryExchange':
            return 'IDEALPRO'
        else:
            error_messages(5, secType + ' ' + symbol + ' ' + param)
    else:
        tmp_df = df[(df['Symbol'] == symbol)&(df['secType'] == secType) & (df['currency'] == currency)]
        if tmp_df.shape[0] == 1: # found 1
            exchange = tmp_df['exchange'].values[0]
            primaryExchange=tmp_df['primaryExchange'].values[0]
            if param=='exchange':
                if type(exchange) == float:
                    if secType == 'STK':
                        return 'SMART'
                    else:
                        error_messages(4, secType + ' ' + symbol + ' ' + param)
                else:
                    return exchange
            elif param=='primaryExchange':
                if type(primaryExchange) == float:
                    return ''
                return primaryExchange
            else:
                error_messages(5, secType + ' ' + symbol + ' ' + param)
        elif tmp_df.shape[0] > 1: # found more than 1
            error_messages(3, secType + ' ' + symbol + ' ' + param)
        else: #found None    
            if waive:
                return 'NA'
            error_messages(4, secType + ' ' + symbol + ' ' + param)
        
def error_messages(n, st):
    if n == 1:
        print ('Definition of %s is not clear!' %(st,))
        print ('Please add this security in IBridgePy/security_info.csv')
        exit()
    elif n == 2:
        print ('Definition of %s is not clear!' %(st,))
        print ('Please use superSymbol to define a security')
        print (r'http://www.ibridgepy.com/ibridgepy-documentation/#superSymbol')
        exit()
    elif n == 3:
        print ('Found too many %s in IBridgePy/security_info.csv' %(st,))
        print ('%s must be unique.' %(' '.join(st.split(' ')[:-1]),))
        exit()
    elif n == 4:
        print ('Exchange of %s is missing.' %(' '.join(st.split(' ')[:-1]),))
        print ('Please add this security in IBridgePy/security_info.csv')
        exit()
    elif n == 5:
        print ('%s of %s is missing.' %(st.split(' ')[-1],' '.join(st.split(' ')[:-1])))
        print ('Please add this info in IBridgePy/security_info.csv')
        exit()
if __name__ == '__main__':
    df = pd.read_csv(str(os.path.dirname(os.path.realpath(__file__)))+'/security_info.csv')
    secType = 'CFD'
    symbol = 'EUR'
    currency = 'USD'
    param = 'exchange'
    #param = 'primaryExchange'
    print(search_security_in_file(df, secType, symbol, currency, param, waive=False))    


