import pandas as pd
from common_utils import read_write_sql_data as rd

# symbol = 'NIFTY_100'
# get_query = f"select * from [NSEDATA].[dbo].{symbol} order by Date ASC"
# df = rd.get_table_data(query=get_query)
# df.set_index('Date', inplace=True, drop=True)
# df.index = pd.to_datetime(df.index)


def resample_daily_data(daily_data, resample_to='W'):
    """
    Resample daily stock data to Weekly(W), Monthly(M), Quarterly(Q) or Yearly(Y) data
    """
    logic = {"Open": 'first',
             "High": 'max',
             "Low": 'min',
             'Close': 'last',
             'Volume': 'sum'
             }

    agg_data = daily_data.resample(resample_to).agg(logic)
    agg_data.reset_index(inplace=True)
    agg_data['Percent_Chg'] = round(agg_data['Close'].pct_change()*100, 2)
    return agg_data


# print(resample_daily_data(df, 'Q'))