"""
References: 
# https://algotrading101.com/learn/alpha-vantage-guide/
#https://www.cmcmarkets.com/en/trading-guides/leading-and-lagging-indicators
# https://pythonrepo.com/repo/RomelTorres-alpha_vantage-python-finance
"""
from alpha_vantage.timeseries import TimeSeries

import yfinance as yf
from datetime import date
import os
import logging
import pickle
logging.basicConfig(filename="dataPull.log", filemode = "w", level=logging.INFO) # add filemode="w" to overwrite
import pandas as pd

# tickerLst = ['XPO', 'BABA', 'NVDA', 'QQQ', 'ENPH']
# requestThd = 5
# dateFilt = None # '2021-8-15'


def getData(tickerLst, requestThd, dateFilt):

    """
    There is a limit of 5 requests/minute

    In order to bypass this, we will create a dictionary of tickers of interest and pull 5/minute, wait a minute, and repeate...
    """

    numReq = 0
    stockStruct = {}
    apiKey = 'WSEJA9L2WSSHPFJZ'

    # os.getcwd()
    #os.chdir(r"\\Users\\malikf1")
    #os.mkdir('folderName')
    #os.chdir('folderName)
    #os.getcwd()
    
    ts = TimeSeries(key=apiKey, output_format='pandas')

    for ticker in tickerLst:
        if numReq <= requestThd:
            # Create Dictionary Entry
            stockStruct[ticker] = {}
            # Pull Data
            logging.info("Pulling {} Data...".format(ticker))
            data, metaData = ts.get_daily_adjusted(symbol=ticker) # get_daily_adjusted <---- does not seem to work outputsize = 'full' --> not acceptable
            #data, meta_data = ts.get_intraday(symbol='xpo',interval='60min', outputsize='full') # compact
            #data, meta_data = ts.get_monthly_adjusted(symbol = 'xpo')
            #data, meta_data = ts.get_daily_adjusted(symbol='xpo', outputsize='full')
            print(data.columns)
            # Data Preprocessing
            data.rename(columns={'1. open': 'open', '2. high': 'high', '3. low': 'low', '4. close': 'close', '5. volume': 'volume'}, inplace=True)
            # data.rename(columns={'1. open': 'open', '2. high': 'high', '3. low': 'low', '4. close': 'close', '5. adjusted close': 'adjusted_close', '6. volume': 'volume', '7. dividend amount': 'dividend_amount', '8. split coefficient': 'split_coefficient'}, inplace=True)
            data.reset_index(inplace=True)
            data['date'] = pd.to_datetime(data['date'], errors='coerce')
            data['symbol'] = ticker
            new_dtypes = {'open': float, 'high': float, 'low': float, 'close': float, 'close': float, 'volume': float}
            #new_dtypes = {'open': float, 'high': float, 'low': float, 'close': float, 'close': float, 'adjusted_close': float, 'volume': float, 'dividend_amount': float, 'split_coefficient': float}
            data = data.astype(new_dtypes)
            data.sort_values(by=['date'], inplace = True)
            data.reset_index(drop = True, inplace = True)

            # Filter Data
            if dateFilt:
                data = data[data['date'] > dateFilt]

            # Save Data to Structure
            stockStruct[ticker]['data'] = data
            stockStruct[ticker]['metaData'] = metaData
            numReq +=1 
        else: 
            logging.info('Too many requests!')

    # Write to Disk
    with open("stockData.file", "wb") as f:
        pickle.dump(stockStruct, f, pickle.HIGHEST_PROTOCOL)
        logging.info("serialization of stockData complete...")


# getData(tickerLst, requestThd, dateFilt)

def getYfinData(tickerLst, dateFilt):
    stockStruct = {}
    for ticker in tickerLst:
        data = yf.Ticker(ticker)
        data = data.history(period="max")
        data = data.reset_index()
        data['symbol'] = ticker
        data['load_date'] = today = date.today()
        data.rename(columns={'Date': 'date', 'Open': 'open', 'Low': 'low', 'High': 'high', 'Close': 'close', 'Volume': 'volume', 'Dividends': 'dividends', 'Stock Splits': 'stock_splits'}, inplace=True)
        data['date'] = pd.to_datetime(data['date'], format = '%Y-%m-%d', errors = 'coerce') # Error in dates --> NaT. 
        data.dropna(inplace = True)
        data.reset_index(inplace=True, drop = True)
        # Filter Data
        if dateFilt:
            data = data[data['date'] > dateFilt]
        stockStruct[ticker] = data
    
    # Write to Disk
    with open("stockData.file", "wb") as f:
        pickle.dump(stockStruct, f, pickle.HIGHEST_PROTOCOL)
        logging.info("serialization of stockData complete...")