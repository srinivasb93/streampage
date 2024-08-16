import nsepy
import pandas as pd
import numpy as np
import datetime as dt
import sqlalchemy as sa
import urllib.parse
import re
from candle import find_candle

# Use this for windows authentication
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=IN01-9MCXZH3\SQLEXPRESS;"
                                 "DATABASE=NSEDATA;"
                                 "Trusted_Connection=yes")

# Connection String
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
# Connect to the required SQL Server
conn = engine.connect()

# Fetch all table details
stock_list = conn.execute('select * from dbo.stocks')
stock_list = [stock[0] for stock in stock_list]
# stock_list = ['MOTHERSUMI', 'BAJFINANCE']
date_today = pd.datetime.dt.today()
start_dt = date_today - dt.timedelta(35)
start_dt = start_dt.strftime(format='%Y-%m-%d')

stock_list_wk = conn.execute('select * from dbo.stocks_weekly')
stock_list_wk = [stock[0] for stock in stock_list_wk]
# stock_list_wk = ['MOTHERSUMI_W', 'BAJFINANCE_W']
start_dt_wk = date_today - dt.timedelta(weeks=2)
start_dt_wk = start_dt_wk.strftime(format='%Y-%m-%d')

stock_list_mn = conn.execute('select * from dbo.stocks_monthly')
stock_list_mn = [stock[0] for stock in stock_list_mn]
# stock_list_mn = ['MOTHERSUMI_M', 'BAJFINANCE_M']
start_dt_mn = date_today - pd.offsets.DateOffset(months=2)
start_dt_mn = start_dt_mn.strftime(format='%Y-%m-%d')

# Empty df to hold consolidated data for all stocks
daily_data = pd.DataFrame()
weekly_data = pd.DataFrame()
monthly_data = pd.DataFrame()

for stock in stock_list:
    get_query = "select * from dbo." + stock + " where DATE >= '" + str(start_dt) +"' ORDER BY DATE ASC"
    data = pd.read_sql_query(get_query, parse_dates=True, con=conn)
    data['Symbol'] = stock
    data['Range'] = round(data['High'] - data['Low'], 2)
    data['Close_Chg_D'] = round(data['Close'].pct_change()*100, 1)
    data['Close_Chg_5D'] = round(data['Close'].pct_change(5)*100, 1)
    data['Close_Chg_20D'] = round(data['Close'].pct_change(20)*100, 1)
    data['High_20D'] = data['High'].rolling(20).max()
    find_candle.find_candle(data)
    daily_data = daily_data.append(data.tail(1), ignore_index=True)

for stock in stock_list_wk:
    get_query_wk = "SELECT Volume,Wk_Cls_Chg,Range,Candle,High_52_Wk,Low_52_Wk,Wk_EMA_20,ATH,ATL,Max_Wk_Chg,Max_Wk_Vol FROM dbo."\
                   + stock + " where DATE >= '" + str(start_dt_wk) + "' ORDER BY DATE ASC"
    data_wk = pd.read_sql_query(get_query_wk, parse_dates=True, con=conn)
    data_wk['Symbol'] = stock.split('_')[0]
    weekly_data = weekly_data.append(data_wk.tail(1), ignore_index=True)

# print(weekly_data.head())

for stock in stock_list_mn:
    get_query_mn = "SELECT Volume,Mth_Cls_Chg,Range,Candle,Max_Mth_Vol,Max_Mth_Range from dbo."\
                   + stock + " where DATE >= '" + str(start_dt_mn) + "' ORDER BY DATE ASC"
    data_mn = pd.read_sql_query(get_query_mn, parse_dates=True, con=conn)
    data_mn['Symbol'] = stock.split('_')[0]
    monthly_data = monthly_data.append(data_mn.tail(1), ignore_index=True)

final_data = pd.merge(daily_data,weekly_data,on='Symbol',suffixes=('_D','_W'))
final_data = pd.merge(final_data,monthly_data,on='Symbol',suffixes=('','_M'))

final_data.to_sql('FINAL_DAILY_DATA',con=conn,if_exists='replace',index=False)