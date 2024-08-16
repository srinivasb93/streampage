import time

import pandas as pd
import sqlalchemy as sa
import datetime as dt
import urllib
import yfinance as yf
from nsepy import get_history

startdate = dt.date(2007,1,1)
enddate = dt.date.today()

split_df = pd.read_csv(r"C:\Users\srinivas\PycharmProjects\data_analysis_all\ref_data\Stock_SplitData_N50.csv")
prices = pd.read_csv(r'C:\Users\srinivas\PycharmProjects\data_analysis_all\ref_data\Stock_Symbol.csv')
#prices=pd.read_csv('Nifty_Midcap_50.csv')
# prices=pd.read_csv('Nifty_Next_50.csv')
# stocks = prices['Symbol'].tolist()
stocks = ['MOTHERSUMI']

split_count_dict = dict(split_df['Stock'].value_counts())
split_stocks_list = split_df['Stock'].unique().tolist()

def adj_close(data,stock):
    df_adj = data.copy()
    no_of_splits = split_count_dict[stock]
    stock_split_info = split_df[split_df['Stock']==stock].copy()
    stock_split_info['Date'] = pd.to_datetime(stock_split_info['Date'])
    for i in range(no_of_splits):
        try:
            to_be_split = (df_adj['Date']<stock_split_info.iloc[i,0])
        except:
            continue
        df_adj.loc[to_be_split,['Open','High','Low','Close']] = round(df_adj.loc[to_be_split,['Open','High','Low','Close']]/stock_split_info.iloc[i,1],2)
    return df_adj

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

x = 0

for stock in stocks:
    print("Extracting Data from NSE for the stock : {}" .format(stock))
    data = get_history(symbol=stock, start=startdate, end=enddate)
    data.reset_index(inplace=True)
    data['Date'] = pd.to_datetime(data['Date'],format='%Y-%m-%d')
    stock_data = data[['Date','Open','High','Low','Close','Volume']].copy()
    if stock in split_stocks_list:
        stock_data = adj_close(stock_data,stock)

    #Read data from .csv file
    # df=pd.read_csv('Results2.csv')
    # df.fillna('',inplace=True)
    print("Start Data Load for the stock : {}".format(stock))
    #Write data read from .csv to SQL Server table
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
    stock_data.to_sql(name=stock,con=conn,if_exists='replace',index=False)
    print("Data Load done for the stock : {}".format(stock))
    if x != 0 and x%2 == 0:
        time.sleep(10)
#Read data from a SQL server table
# df1=pd.read_sql_table('STOCK_DATA',con=conn)
# print(df1.head())

'''
# Condition to not insert duplicate values into sql server table

for i in range(len(df)):
    try:
        df.iloc[i:i+1].to_sql(name="Table_Name",if_exists='append',con = Engine)
    except IntegrityError:
        pass #or any other action

'''