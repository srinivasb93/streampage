from common_utils import read_write_sql_data as rd
import pandas as pd
import datetime as dt
from dateutil import parser
from python_scripts.stocks_data_load import bhav_copy_extract as bhav

conn = rd.create_connection()

indices_df = rd.get_table_data(query="SELECT name FROM NSEDATA.dbo.INDICES_LIST")
indices_list = indices_df['name'].unique()

stocks_df = rd.get_table_data(selected_database="ANALYTICS",
                              selected_table="EQUITY_HOLDINGS")
my_stocks_list = stocks_df[['Broker_Name', 'Stock_Symbol', 'Quantity']].to_dict(orient="records")
# print(my_stocks_list)


def fetch_my_funds_codes():
    mfunds_df = rd.get_table_data(selected_database="ANALYTICS",
                                  selected_table="MF_HOLDINGS")
    my_fund_codes = mfunds_df[['Scheme_Code', 'Quantity']].to_dict(orient="records")
    # print(my_fund_codes)
    return my_fund_codes


def fetch_fund_table_names(my_funds_codes):
    """
    Fetch fund table names from SQL
    :return:
    """
    mf_query = 'SELECT name FROM MFDATA.SYS.TABLES'
    fund_names_df = rd.get_table_data(selected_database="ANALYTICS",
                                      selected_table="MF_HOLDINGS",
                                      query=mf_query)

    all_fund_names = fund_names_df['name'].unique()
    my_fund_table_names = {fund_code['Scheme_Code']: table_name for fund_code in my_funds_codes
                           for table_name in all_fund_names if str(fund_code['Scheme_Code']) in table_name}
    # print(my_fund_table_names)
    return my_fund_table_names


stock_db = "NSEDATA"
mf_db = "MFDATA"
my_fund_names = fetch_fund_table_names(fetch_my_funds_codes())
stock_change_df = pd.DataFrame()
mf_change_df = pd.DataFrame()
benchmark_df = pd.DataFrame()


# Function to fetch historical stock/index data from SQL Server
def fetch_stock_index_mf_data(table_name, start_date, data_type='stock'):
    if data_type in ['stock', 'index']:
        query = f"SELECT Date, [Close] FROM {stock_db}.dbo.{table_name} WHERE Date >= '{start_date}' ORDER BY Date"
        data_df = pd.read_sql(query, conn, parse_dates=['Date'], index_col='Date')
        data_df.rename(columns={'Close': table_name}, inplace=True)
    elif data_type == 'mf':
        query = f"SELECT date, [nav] FROM {mf_db}.dbo.{table_name} WHERE date >= '{start_date}' ORDER BY Date"
        data_df = pd.read_sql(query, conn, parse_dates=['date'], index_col='date')
        data_df.rename(columns={'nav': table_name}, inplace=True)
    # Fetch data from SQL Server

    return data_df


def fetch_portfolio_data(my_holdings, start_date, holding_type='stock'):
    if holding_type == 'stock':
        symbols = list(set([holding['Stock_Symbol'] for holding in my_holdings]))
        try:
            symbols.remove('METALFORGE')
        except ValueError:
            pass
    elif holding_type == 'mf':
        symbols = [my_fund_names.get(holding['Scheme_Code']) for holding in my_holdings]
    print(symbols)

    portfolio_data = pd.concat(
        [fetch_stock_index_mf_data(symbol, start_date, holding_type) for symbol in symbols],
        axis=1,
        ignore_index=True)

    portfolio_data.columns = symbols
    return portfolio_data


def fetch_consolidated_data():
    """
    Fetch consolidated data
    :return:
    """
    portfolio_data_stocks = fetch_portfolio_data(my_stocks_list, '2023-01-01', holding_type='stock')
    portfolio_data_funds = fetch_portfolio_data(fetch_my_funds_codes(), '2023-01-01', holding_type='mf')
    return portfolio_data_stocks, portfolio_data_funds
# print(portfolio_data_stocks)
# print(portfolio_data_funds)


# Function to calculate daily returns
def calculate_returns(data):
    data['Returns'] = data['Close'].pct_change()
    data['Cumulative Returns'] = (1 + data['Returns']).cumprod() - 1
    data['Profit/Loss'] = (1 + data['Returns']).cumprod() * data['Close'].iloc[0] - data['Close']
    return data
