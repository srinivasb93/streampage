import pandas as pd
import datetime as dt
import yfinance as yf
import sqlalchemy as sa
import urllib.parse

# Use this for windows authentication
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=IN01-9MCXZH3\SQLEXPRESS;"
                                 "DATABASE=NSEDATA;"
                                 "Trusted_Connection=yes")

'''
# Use this for SQL server authentication
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

query_stock_names = """select SYMBOL from [NSEDATA].[dbo].[ALL_STOCKS] where stk_index = 'NIFTY 200'
except
(
SELECT  [SYMBOL]  FROM [NSEDATA].[dbo].[ALL_STOCKS] where stk_index = 'NIFTY 50'
union
SELECT  [SYMBOL]  FROM [NSEDATA].[dbo].[ALL_STOCKS] where stk_index = 'NIFTY NEXT 50'
union
SELECT  [SYMBOL]  FROM [NSEDATA].[dbo].[ALL_STOCKS] where stk_index = 'NIFTY MIDCAP 50'
)"""
# query_stock_names = "SELECT SYMBOL  FROM [dbo].[ALL_STOCKS] WHERE STK_INDEX = 'NIFTY 50'"
all_data = pd.read_sql(query_stock_names, con=conn)
stocks = all_data['SYMBOL'].values
start_date = dt.date(2007, 1, 1)
end_date = dt.date.today()
stk_split_data = pd.DataFrame()
stocks = ['MOTHERSON']

for stock in stocks:
    print(stock)
    get_stocks = yf.Ticker(stock+'.NS')
    df_yahoo = get_stocks.history(start=start_date, end=end_date, interval='1d')
    df_yahoo['Stock'] = stock
    df_yahoo.reset_index(inplace=True)
    split_data = df_yahoo[['Date', 'Stock Splits', 'Stock']][df_yahoo['Stock Splits'] > 0]
    stk_split_data = stk_split_data.append(split_data)
    print(split_data)

stk_split_data['Date'] = pd.to_datetime(stk_split_data['Date']).dt.date
stk_split_data['STOCK_INDEX'] = 'NIFTY 200'
stk_split_data.to_sql('STOCK_SPLIT_DATA', con=conn, index=False, if_exists='append')
