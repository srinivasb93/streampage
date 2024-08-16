import pandas as pd
import pyodbc
from nsetools import Nse
import urllib.parse
import sqlalchemy as sa
from sqlalchemy import text
import matplotlib.pyplot as plt
import gc
import numpy as np

nse = Nse()

connection = pyodbc.connect(
                            'Driver={SQL Server};'
                            'Server=IN01-9MCXZH3\SQLEXPRESS;'
                            'Database=NSEDATA;'
                            'Trusted_Connection=yes;'
                           )

# Use this for windows authentication
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=IN01-9MCXZH3\SQLEXPRESS;"
                                 "DATABASE=StockAnalysis;"
                                 "Trusted_Connection=yes")

# Connection String
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

# Connect to the required SQL Server
connection2 = engine.connect()

class LongTermStrategy:
    """ Class for analysing long term investing startegy for stocks"""
    def __init__(self,stock_data=None,req_percent=.05):
        self.stock_data = stock_data
        self.req_percent = req_percent
        # self.stock_data['EMA_20'] = self.stock_data['Close'].ewm(span=20).mean()
        self.stock_data['High_Signal'] = round(self.stock_data['High'] - self.stock_data['High'].shift(1), 2)
        self.stock_data['Low_Signal'] = round(self.stock_data['Low'] - self.stock_data['Low'].shift(1), 2)
        self.base_price = self.base_price1 = self.base_price2 = 0
        self.buy_quantity = 0
        self.fixed_amount = 2000
        self.top_up_amount = self.available_amount = 0
        self.total_quantity = 0
        self.accrued_amount = 0
        self.total_invested_amount = 0
        self.buy_at_base_price = self.buy_at_base_price1 = self.buy_at_base_price2 = False
        self.stock_data.loc[self.stock_data.index[0], "Available_Amt"] = self.available_amount
        self.stock_data.loc[self.stock_data.index[0], "Total_Qty"] = self.total_quantity
        self.stock_data.loc[self.stock_data.index[0], "Accr_Amt"] = self.accrued_amount
        self.stock_data.loc[self.stock_data.index[0], "Invested_Amt"] = self.total_invested_amount

    def apply_strategy(self, idx, bought_at='Base Price'):
        """ Execute buy order at the required base price """
        if bought_at == 'Base Price':
            self.buy_quantity = self.available_amount // self.base_price
            invested_amt = self.buy_quantity * self.base_price
            self.accrued_amount = round((self.available_amount - invested_amt),
                                        2) if self.buy_quantity > 0 else self.accrued_amount
            self.total_invested_amount += (self.buy_quantity * self.base_price)
            self.buy_at_base_price = False
            # stock_data.loc[idx, "Buy_Qty"] = self.buy_quantity
            # stock_data.loc[idx, "Bought_At"] = bought_at
        elif bought_at == 'Base Price 1':
            self.top_up_amount = self.available_amount + self.fixed_amount
            self.buy_quantity = self.top_up_amount // self.base_price1
            invested_amt = self.buy_quantity * self.base_price1
            self.accrued_amount = round((self.top_up_amount - invested_amt), 2) if self.buy_quantity > 0 else self.accrued_amount

            self.total_invested_amount += (self.buy_quantity * self.base_price1)
            self.buy_at_base_price1 = False

            # stock_data.loc[idx, "Buy_Qty"] = self.buy_quantity
            # stock_data.loc[idx, "Bought_At"] = bought_at
        elif bought_at == 'Base Price 2':
            self.top_up_amount = self.available_amount + self.fixed_amount*2
            self.buy_quantity = self.top_up_amount // self.base_price2
            invested_amt = self.buy_quantity * self.base_price2
            self.accrued_amount = round((self.top_up_amount - invested_amt), 2) if self.buy_quantity > 0 else self.accrued_amount
            self.total_invested_amount += invested_amt
            self.buy_at_base_price2 = False
            # stock_data.loc[idx, "Buy_Qty"] = self.buy_quantity
            # stock_data.loc[idx, "Bought_At"] = bought_at

        if self.buy_quantity > 0:
            self.available_amount = self.fixed_amount + self.accrued_amount
            self.total_quantity += self.buy_quantity
            stock_data.loc[idx, "Bought_At"] = bought_at
        else:
            self.available_amount += self.fixed_amount
            if bought_at == 'Base Price': stock_data.loc[idx, "Bought_At"] = 'No Funds_BP'
            elif bought_at == 'Base Price 1': stock_data.loc[idx, "Bought_At"] = 'No Funds_BP1'
            else : stock_data.loc[idx, "Bought_At"] = 'No Funds_BP2'
        try:
            stock_data.loc[idx, "Average_Price"] = self.total_invested_amount // self.total_quantity
        except ZeroDivisionError:
            pass
        stock_data.loc[idx, "Buy_Qty"] = self.buy_quantity
        stock_data.loc[idx, "Total_Qty"] = self.total_quantity
        stock_data.loc[idx, "Accr_Amt"] = self.accrued_amount
        stock_data.loc[idx, "Available_Amt"] = self.available_amount
        # stock_data.loc[idx, "Bought_At"] = bought_at if self.buy_quantity > 0 else 'Insufficient Funds'
        stock_data.loc[idx, "Invested_Amt"] = self.total_invested_amount

    def calculate_base_price(self):
        """ Calculate base price for the stock """
        for idx, data in enumerate(self.stock_data.itertuples()):
            if data.Month_Chg != 0:
                self.available_amount += self.fixed_amount
                stock_data.loc[data.Index, "Available_Amt"] = self.available_amount
            if data.High_Signal > 0:
                self.base_price = round((1-self.req_percent)*data.Close, 2)
                # self.base_price = round((1 - self.req_percent) * data.High, 2)
                self.base_price1 = round(self.base_price*(1 - self.req_percent), 2)
                self.base_price2 = round(self.base_price1*(1 - self.req_percent), 2)
                stock_data.loc[data.Index, 'Base_Price'] = self.base_price
                stock_data.loc[data.Index, 'Base_Price1'] = self.base_price1
                stock_data.loc[data.Index, 'Base_Price2'] = self.base_price2
                self.buy_at_base_price = self.buy_at_base_price1 = self.buy_at_base_price2 = True
                stock_data.loc[data.Index, "Current_Value"] = round(stock_data.loc[data.Index, "Close"]
                                                                    * self.total_quantity, 2)
                continue

            if data.Low <= self.base_price and self.buy_at_base_price:
                self.apply_strategy(data.Index, 'Base Price')

            if data.Low <= self.base_price1 and self.buy_at_base_price1:
                self.apply_strategy(data.Index, 'Base Price 1')

            if data.Low <= self.base_price2 and self.buy_at_base_price2:
                self.apply_strategy(data.Index, 'Base Price 2')

            stock_data.loc[data.Index, "Current_Value"] = round(stock_data.loc[data.Index, "Close"]
                                                                * self.total_quantity, 2)

# Take backup of the existing data in the database
# query1 = 'select * from dbo.'
# data_combined = pd.read_sql(query1+'LONGTERM_COMBINED', con=connection2)
# data_combined.to_sql(name='LONGTERM_COMBINED_BKP',con=connection2,if_exists='replace')
# del data_combined
# gc.collect()
# data_ind = pd.read_sql(query1+'LONGTERM_IND', con=connection2)
# data_ind.to_sql(name='LONGTERM_IND_BKP',con=connection2,if_exists='replace')
# del data_ind
# gc.collect()
# data_all = pd.read_sql(query1+'ALL_DATA', con=connection2)
# data_all.to_sql(name='ALL_DATA_BKP',con=connection2,if_exists='replace')
# del data_all
# gc.collect()
# connection2.execute('DROP TABLE IF EXISTS dbo.LONGTERM_COMBINED_BKP')
# connection2.execute('DROP TABLE IF EXISTS dbo.LONGTERM_IND_BKP')
# connection2.execute('DROP TABLE IF EXISTS dbo.ALL_DATA_BKP')
#
# connection2.execute('SELECT * INTO dbo.LONGTERM_COMBINED_BKP FROM LONGTERM_COMBINED')
# connection2.execute('SELECT * INTO dbo.LONGTERM_IND_BKP FROM LONGTERM_IND')
# connection2.execute('SELECT * INTO dbo.ALL_DATA_BKP FROM ALL_DATA')

cursor = connection.cursor()
# cursor.execute('select * from dbo.stocks_list')
# cursor.execute('select top 1* from dbo.SBIN')
# stock_list = cursor.fetchall()
cursor.close()
final_data = pd.DataFrame()
all_data = pd.DataFrame()
stock_list = ['MOTHERSON', 'BAJFINANCE', 'HDFCBANK', 'ASIANPAINT', 'RELAXO', 'SONATSOFTW', 'ICICIBANK', 'NESTLEIND'
              , 'KOTAKBANK', 'BRITANNIA', 'RELIANCE', 'TITAN', 'HINDUNILVR', 'PIDILITIND', 'ITC', 'INFY', 'TCS']
# stock_list = ['MOTHERSUMI','HDFCBANK']
for stock in stock_list:
    print(f'Processing Stock : {stock}')
    query = "SELECT * FROM DBO." + stock + " WHERE DATE >= '2020-01-01 00:00:00' ORDER BY DATE ASC"
    # query = "SELECT * FROM DBO." + "SBIN" + " WHERE DATE >= '2021-02-02 00:00:00'"
    # query = "SELECT * FROM DBO.SONATSOFTW ORDER BY DATE ASC"
    stock_data = pd.read_sql(query, con=connection)
    stock_data['Month'] = pd.to_datetime(stock_data['Date'], format='%Y-%m-%d').dt.month
    stock_data.set_index('Date', inplace=True)
    stock_data['Month_Chg'] = stock_data['Month'] - stock_data['Month'].shift(1)
    stock_trigger = LongTermStrategy(stock_data)
    stock_trigger.calculate_base_price()
    stock_data['Symbol'] = stock
    values = {'Buy_Qty' : 0, 'Bought_At' : ' '}
    stock_data.fillna(value=values, inplace=True)
    stock_data.fillna(method='ffill', inplace=True)
    stock_data['PnL'] = round(stock_data['Current_Value'] - stock_data['Invested_Amt'], 2)
    all_data = all_data.append(stock_data)
    final_data = final_data.append(stock_data.tail(1))
    print(f'Processing done for Stock : {stock}')

tot_investment = round(final_data['Invested_Amt'].sum(), 2)
curr_value = round(final_data['Current_Value'].sum(), 2)
all_data.reset_index(inplace=True)
all_data.to_sql(name='ALL_DATA',con=connection2,if_exists='replace', index=False)
all_data_grouped = all_data.groupby(['Date'])
grouped_dict = {}

for group_name,group_data in all_data_grouped:
    act_date = group_name
    tot_invested = group_data['Invested_Amt'].sum()
    tot_curr_val = group_data['Current_Value'].sum()
    tot_profit = group_data['PnL'].sum()
    grouped_dict.update({act_date:{'Total_Invested':tot_invested,
                         'Current_Value':tot_curr_val,'PnL':tot_profit}})

combined_data = pd.DataFrame.from_dict(grouped_dict, orient='index')
combined_data['Invested_on_day'] = round(combined_data['Total_Invested'] - combined_data['Total_Invested'].shift(1), 2)
combined_data['Value_Change'] = round(combined_data['Current_Value'] - combined_data['Current_Value'].shift(1), 2)
combined_data['PnL_Change'] = round(combined_data['PnL'] - combined_data['PnL'].shift(1))
combined_data['PnL_%'] = round((combined_data['PnL']/combined_data['Total_Invested'])*100,2)
combined_data = combined_data[['Total_Invested','Invested_on_day','Current_Value',
                         'Value_Change','PnL','PnL_Change','PnL_%']]
combined_data.reset_index(inplace=True)
combined_data.to_sql(name='LONGTERM_COMBINED', con=connection2, if_exists='replace', index=False)
# combined_data.plot(y=['Total_Invested','Current_Value'])
# plt.show()

print('Total Investment : {:,}'.format(tot_investment))
print('Present Market Value : {:,}'.format(curr_value))
print(f'PnL : {round((curr_value-tot_investment), 2)}')

final_data = final_data[['Symbol','Total_Qty','Average_Price','Close','Invested_Amt','Current_Value','PnL']]
final_data['PnL_%'] = round((final_data['PnL']/final_data['Invested_Amt'])*100,2)
final_data.to_sql(name='LONGTERM_IND', con=connection2, if_exists='replace',index=False)

connection.close()
connection2.close()