from common_utils import read_write_sql_data as rd
import pandas as pd
import datetime as dt

def fetch_stocks_data(data_type='Daily', equity_type='Stocks', bhav_copy=False,
                      fetch_count=False, fetch_date=dt.date.today()):
    stocks_data = pd.DataFrame()

    if bhav_copy:
        if equity_type == 'Stocks':
            stocks_data = rd.get_table_data(selected_table='BHAVCOPY')
        elif equity_type == 'Index':
            stocks_data = rd.get_table_data(selected_table='BHAVCOPY_INDICES')
    else:
        stock_list_df = rd.get_table_data(selected_table='STOCKS_IN_DB')
        stocks_list = stock_list_df['SYMBOL'].values.tolist()
        if data_type != 'Daily':
            stock_suffix = {'Weekly': '_W', 'Monthly': '_M', 'Yearly': '_Y'}
            stocks_list = [stk_name + stock_suffix.get(data_type, '') for stk_name in stocks_list]

        stocks_data = pd.DataFrame()
        for stock_name in stocks_list:
            stock_data = rd.get_table_data(selected_table=stock_name, sort_order='DESC', sort=True,
                                           sample=True, sample_count=1)
            stock_data.insert(0, 'Symbol', stock_name)
            if fetch_count:
                query = f"select count(*) from nsedata.public.{stock_name} where Date >= '{fetch_date}'"
                query_data = rd.get_table_data(query=query)
                row_count = query_data.values.tolist()[0][0]
                stock_data.insert(1, 'Row_Count', row_count)
            stocks_data = pd.concat([stocks_data, stock_data], axis=0, ignore_index=True)

    return stocks_data

data = fetch_stocks_data(fetch_count=True, fetch_date=dt.date(2024, 10, 10))