import time
import pandas as pd
import sqlalchemy as sa
import datetime as dt
import urllib
import yfinance as yf
from jugaad_data import nse
from common_utils import read_write_sql_data as rd

startdate = dt.date(2007, 1, 1)
enddate = dt.date.today()


def adj_close(data, stock):
    df_adj = data.copy()
    df_adj.sort_values(by='Date', inplace=True)
    no_of_splits = split_count_dict[stock]
    stock_split_info = split_df[split_df['SYMBOL'] == stock].copy()
    stock_split_info['DATE'] = pd.to_datetime(stock_split_info['DATE'])
    stock_split_info.sort_values(by='DATE', inplace=True)
    for i in range(no_of_splits):
        try:
            to_be_split = (df_adj['Date'] < stock_split_info.iloc[i, 1])
        except:
            continue
        df_adj.loc[to_be_split, ['Open', 'High', 'Low', 'Close']] = \
            round(df_adj.loc[to_be_split, ['Open', 'High', 'Low', 'Close']]*stock_split_info.iloc[i, 3], 2)
    return df_adj

# Use this for windows authentication
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=IN01-9MCXZH3\SQLEXPRESS;"
                                 "DATABASE=NSEDATA;"
                                 "Trusted_Connection=yes")

'''
#Use this for SQL server authentication
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=dagger;"
                                 "DATABASE=test;"
                                 "UID=user;"
                                 "PWD=password")
'''

# Connection String
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

# Connect to the required SQL Server
conn = engine.connect()

query_split = "SELECT * FROM dbo.SPLIT_BONUS_DATA"
# query_split = "SELECT * FROM DBO.STOCK_SPLIT_DATA WHERE STOCK_INDEX = 'NIFTY 50'"
split_df = pd.read_sql(query_split, con=conn, parse_dates=True)

query_stocks = """select SYMBOL from [NSEDATA].[dbo].[ALL_STOCKS] where stk_index = 'NIFTY 200'
except
(
SELECT  [SYMBOL]  FROM [NSEDATA].[dbo].[ALL_STOCKS] where stk_index = 'NIFTY 50'
union
SELECT  [SYMBOL]  FROM [NSEDATA].[dbo].[ALL_STOCKS] where stk_index = 'NIFTY NEXT 50'
union
SELECT  [SYMBOL]  FROM [NSEDATA].[dbo].[ALL_STOCKS] where stk_index = 'NIFTY MIDCAP 50'
)"""
# query = "SELECT SYMBOL FROM DBO.ALL_STOCKS WHERE STK_INDEX = 'NIFTY 50'"
stocks_df = pd.read_sql(query_stocks, con=conn)
stocks = stocks_df['SYMBOL'].tolist()
stocks = ['M&MFIN']

split_count_dict = dict(split_df['SYMBOL'].value_counts())
split_stocks_list = split_df['SYMBOL'].unique().tolist()
x = 0

for stock in stocks:
    print("Extracting Data from NSE for the stock : {}" .format(stock))
    # data = get_history(symbol=stock, start=startdate, end=enddate)
    # data = nse.stock_df(symbol=stock, from_date=startdate, to_date=enddate)
    data = yf.Ticker(stock+'.NS').history(period='max', interval='1d')[['Open', 'High', 'Low', 'Close', 'Volume']]
    # data = nse.stock_df(symbol=stock, from_date=startdate, to_date=enddate, series='EQ')
    data.reset_index(inplace=True)
    # data.rename(columns={'DATE': 'Date', 'OPEN': 'Open', 'HIGH': 'High',
    #                      'LOW': 'Low', 'CLOSE': 'Close', 'VOLUME': 'Volume'},
    #                      inplace=True)
    data['Date'] = pd.to_datetime(data['Date'], format='%Y-%m-%d')
    data['Date'] = data['Date'].dt.tz_localize(None)
    data.ffill(inplace=True, axis=0)
    stock_data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']].copy()
    # Calculate stock prices based on stock split or bonus shares issued
    if stock in split_stocks_list:
        stock_data = adj_close(stock_data, stock)
    print("Start Data Load for the stock : {}".format(stock))
    # Remove special characters in stock names
    if stock == 'BAJAJ-AUTO':
        stock = 'BAJAJAUTO'
    if stock == 'M&M':
        stock = 'MM'
    if stock == 'MCDOWELL-N':
        stock = 'MCDOWELL'
    if stock == 'L&TFH':
        stock = 'LTFH'
    if stock == 'M&MFIN':
        stock = 'MMFIN'
    # Write data extracted for each stock to SQL Server table
    # stock_data.to_sql(name=stock, con=conn, if_exists='replace', index=False)
    stock_data['Open'] = round(stock_data['Open'], 2)
    stock_data['High'] = round(stock_data['High'], 2)
    stock_data['Low'] = round(stock_data['Low'], 2)
    stock_data['Close'] = round(stock_data['Close'], 2)
    rd.load_sql_data(data_to_load=stock_data, table_name=stock)
    print("Data Load done for the stock : {}".format(stock))
    # Add 5 seconds delay to data extraction for every 2 stocks
    x += 1
    if x != 0 and x % 2 == 0:
        time.sleep(5)

print('Data Load is complete for all stocks!!!')
