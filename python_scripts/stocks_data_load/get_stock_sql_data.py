
import pandas as pd
import sqlalchemy as sa
from datetime import datetime as dt
import pyodbc
import urllib
import yfinance as yf
from nsepy import get_history

stock = 'TATAMOTORS'


#Use this for windows authentication
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=IN01-9MCXZH3\SQLEXPRESS;"
                                 "DATABASE=NSEDATA;"
                                 "Trusted_Connection=yes")


"""
#Use this for SQL server authentication
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=IN01-9MCXZH3\SQLEXPRESS;"
                                 "DATABASE=NSEDATA;"
                                 "UID=user;"
                                 "PWD=password")
"""

#Connection String
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

# Connect to the required SQL Server
conn=engine.connect()

# date_today = dt.today()
# start_dt = date_today - dt.timedelta(1825)
# start_dt = start_dt.strftime(format='%Y-%m-%d')

query = "SELECT SYMBOL FROM dbo.ALL_STOCKS"
stocks_data = conn.execute(query)
stocks = stocks_data.fetchall()
stock_list = [stock[0] for stock in stocks]
# stock_list = ['TATAMOTORS']

for stock in stock_list:
    query_get_data = "select * from dbo." + stock + " where DATE >= '" + str(start_dt) +"' ORDER BY DATE ASC"
    data = pd.read_sql_query(query_get_data, con=conn, parse_dates=True)
    # data['Date'] = pd.to_datetime(data['Date']).dt.strftime('%m/%d/%Y')
    # data['Symbol'] = stock
    data.to_csv('C:\\Users\\admin\\PycharmProjects\\pythonProject\\PyAnalysis\\data\\'+stock+'.csv',index=False)
    print('Data extraction is successful for {0}'.format(stock))

print('Data extraction is successful for all stocks')
