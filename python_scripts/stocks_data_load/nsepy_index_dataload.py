import pandas as pd
import sqlalchemy as sa
import datetime as dt
import pyodbc
import urllib
import yfinance as yf
from jugaad_data import nse
from nsepython import nse_get_index_list, index_history



startdate = dt.date(2024, 2, 15)
enddate = dt.date.today()

# startdate = "01-Feb-2024"
# enddate = "01-Mar-2024"

# indices = ['NIFTY 50', 'NIFTY NEXT 50', 'NIFTY BANK', 'INDIA VIX', 'NIFTY 100', 'NIFTY 500', 'NIFTY MIDCAP 100', 'NIFTY MIDCAP 50', 'NIFTY INFRA', 'NIFTY REALTY', 'NIFTY ENERGY', 'NIFTY FMCG', 'NIFTY MNC', 'NIFTY PHARMA', 'NIFTY PSE', 'NIFTY PSU BANK', 'NIFTY SERV SECTOR', 'NIFTY IT', 'NIFTY SMLCAP 100', 'NIFTY 200', 'NIFTY AUTO', 'NIFTY MEDIA', 'NIFTY METAL', 'NIFTY COMMODITIES', 'NIFTY CONSUMPTION', 'NIFTY CPSE', 'NIFTY FIN SERVICE', 'NIFTY SMLCAP 50', 'NIFTY SMLCAP 250']
indices = ['NIFTY 50']

# index_data = index_history(symbol=indices[0], start_date=startdate, end_date=enddate)
# data = nse.index_df(symbol=indices[0], from_date=startdate, to_date=enddate)
# print(data)
df = yf.Ticker('GLAND.NS').history(period='max',
                                             interval='1d')
print(df)
exit()
#Use this for windows authentication
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

#Connection String
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

# Connect to the required SQL Server
conn=engine.connect()

for stk_index in indices:
    print("Extracting Data from NSE for the index : {}" .format(stk_index))
    data = nse.index_raw(symbol=stk_index, from_date=startdate, to_date=enddate)
    data.reset_index(inplace=True)
    data.rename(columns={'HistoricalDate': 'Date', 'OPEN': 'Open', 'HIGH': 'High',
                         'LOW': 'Low', 'CLOSE': 'Close', 'VOLUME': 'Volume'},
                inplace=True)
    data['Date'] = pd.to_datetime(data['Date'], format='%Y-%m-%d')
    stock_data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']].copy()
    stock_data = stock_data.fillna(method='bfill')

    if stock_index == 'NIFTY Smallcap 100':
        stock_index = 'NIFTY_SMLCAP_100'
    if stock_index == 'Nifty Smallcap 250':
        stock_index = 'NIFTY_SMLCAP_250'
    if stock_index == 'Nifty Smallcap 50':
        stock_index = 'NIFTY_SMLCAP_50'
    if stock_index == 'Nifty Infrastructure':
        stock_index = 'NIFTY_INFRA'
    if stock_index == 'Nifty Services Sector':
        stock_index = 'NIFTY_SERV_SECTOR'
    if stock_index == 'Nifty India Consumption':
        stock_index = 'NIFTY_CONSUMPTION'
    if stock_index == 'Nifty Financial Services':
        stock_index = 'NIFTY_FIN_SERVICE'

    print("Start Data Load for the index : {}".format(stk_index))

    stock_data.to_sql(name=stk_index, con=conn, if_exists='replace', index=False)
    print("Data Load done for the index : {}".format(stk_index))

