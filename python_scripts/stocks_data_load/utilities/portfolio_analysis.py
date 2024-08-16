""" 
Author: Utkarsh Singhal: https://www.linkedin.com/in/utkarsh-singhal-584770181
Article Link on TradeWithPython: https://tradewithpython.com/portfolio-analysis-using-python
"""

# Imprting Libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb
from datetime import date
from nsepy import get_history as gh
import sqlalchemy as sa
import urllib.parse
plt.style.use('fivethirtyeight') #setting matplotlib style

# Use this for windows authentication
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=IN01-9MCXZH3\SQLEXPRESS;"
                                     "DATABASE=NSEDATA;"
                                     "Trusted_Connection=yes")

'''
#Use this for SQL server authentication
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=IN01-9MCXZH3\SQLEXPRESS;"
                                 "DATABASE=test;"
                                 "UID=user;"
                                 "PWD=password")
'''

# Connection String
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

# Connect to the required SQL Server
conn = engine.connect()

# Defining Parameters
stocksymbols = ['TATAMOTORS','DABUR', 'ICICIBANK','WIPRO','BPCL','IRCTC','INFY','RELIANCE']
startdate = date(2019,10,14)
end_date = date.today()
print(end_date)
print(f"You have {len(stocksymbols)} assets in your porfolio" )

# Fetching Data
df = pd.DataFrame()
for i in range(len(stocksymbols)):
    query = "SELECT [Close] FROM DBO." + stocksymbols[i] + " WHERE DATE >= '2020-01-01 00:00:00' ORDER BY DATE ASC"
    # query = "SELECT * FROM DBO." + "SBIN" + " WHERE DATE >= '2021-02-02 00:00:00'"
    # query = "SELECT * FROM DBO.SONATSOFTW ORDER BY DATE ASC"
    stock_data = pd.read_sql(query, con=conn)
    stock_data.rename(columns={'Close': stocksymbols[i]}, inplace=True)
    if i == 0:
        df = stock_data
    if i != 0:
        df = df.join(stock_data)
    print(f"Processing completed for stock - {stocksymbols[i]}")
print(df)

# Analysis
fig, ax = plt.subplots(figsize=(15, 8))
for i in df.columns.values :
    ax.plot(df[i], label = i)
ax.set_title("Portfolio Close Price History")
ax.set_xlabel('Date', fontsize=18)
ax.set_ylabel('Close Price INR (₨)' , fontsize=18)
ax.legend(df.columns.values , loc = 'upper left')
fig.show()

# Correlation Matrix

correlation_matrix = df.corr(method='pearson')
print(correlation_matrix)

fig1 = plt.figure()
sb.heatmap(correlation_matrix,xticklabels=correlation_matrix.columns, yticklabels=correlation_matrix.columns,
cmap='YlGnBu', annot=True, linewidth=0.5)
print('Correlation between Stocks in your portfolio')
fig1.show()

# Risk & Return

daily_simple_return = df.pct_change(1)
daily_simple_return.dropna(inplace=True)
print(daily_simple_return)

print('Daily simple returns')
fig, ax = plt.subplots(figsize=(15,8))

for i in daily_simple_return.columns.values :
    ax.plot(daily_simple_return[i], lw =2 ,label = i)

ax.legend( loc = 'upper right' , fontsize =10)
ax.set_title('Volatility in Daily simple returns ')
ax.set_xlabel('Date')
ax.set_ylabel('Daily simple returns')
fig.show()

#Average Daily returns
print('Average Daily returns(%) of stocks in your portfolio')
Avg_daily = daily_simple_return.mean()
print(Avg_daily*100)

# Risk Box-Plot
daily_simple_return.plot(kind = "box",figsize = (20,10), title = "Risk Box Plot")

print('Annualized Standard Deviation (Volatality(%), 252 trading days) of individual stocks in your portfolio on the basis of daily simple returns.')
print(daily_simple_return.std() * np.sqrt(252) * 100)

# Return Per Unit Of Risk:
print(Avg_daily / (daily_simple_return.std() * np.sqrt(252)) *100)

# Cumulative Returns:

daily_cummulative_simple_return =(daily_simple_return+1).cumprod()
print(daily_cummulative_simple_return)

#visualize the daily cummulative simple return
print('Cummulative Returns')
fig, ax = plt.subplots(figsize=(18,8))

for i in daily_cummulative_simple_return.columns.values :
    ax.plot(daily_cummulative_simple_return[i], lw =2 ,label = i)

ax.legend( loc = 'upper left' , fontsize =10)
ax.set_title('Daily Cummulative Simple returns/growth of investment')
ax.set_xlabel('Date')
ax.set_ylabel('Growth of ₨ 1 investment')
fig.show()

