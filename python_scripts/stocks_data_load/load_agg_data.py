import pandas as pd
from common_utils import read_write_sql_data as rd
from python_scripts.candle import find_candle
import logging
import pandas_ta as ta

log = logging.getLogger()
logging.basicConfig(filename=r"C:\Users\sba400\MyProject\streampage\python_scripts\logfiles\AGG_DATA_LOAD.log",
                    format=f"%(asctime)s : %(name)s : %(message)s",
                    level='DEBUG')


def resample_daily_data(daily_data, resample_to='W'):
    """
    Resample daily stock data to Weekly(W), Monthly(M), Quarterly(Q) or Yearly(Y) data
    """
    logic = {"open": 'first',
             "high": 'max',
             "low": 'min',
             'close': 'last',
             'volume': 'sum'
             }
    agg_data = daily_data.copy()
    agg_data = agg_data.resample(resample_to + "E" if resample_to in ("M", "Y") else resample_to).agg(logic)
    agg_data.reset_index(inplace=True)

    agg_data['Percent_Chg_'+resample_to] = round(agg_data['close'].pct_change()*100, 2)
    if resample_to in ['M', 'W']:
        agg_data['Range_'+resample_to] = round(agg_data['high'] - agg_data['low'], 2)
        agg_data['Max_Chg_'+resample_to] = agg_data['Percent_Chg_' + resample_to].expanding().max()
        agg_data['Max_Vol_'+resample_to] = agg_data['volume'].expanding().max()
        # agg_data['Avg_Vol_6' + resample_to] = int(agg_data['volume'].rolling(6).mean())
        # agg_data['Candle'] = ta.cdl_pattern(agg_data.ta.ohlc4)
        agg_data['LR_6_' + resample_to] = round(ta.linreg(agg_data['close'], length=6), 2)
        agg_data['low_LR_6_' + resample_to] = round((agg_data['low'] + agg_data['LR_6_' + resample_to]) / 2, 2)
        agg_data['high_LR_6_' + resample_to] = round((agg_data['high'] + agg_data['LR_6_' + resample_to]) / 2, 2)
        # agg_data['Max_Range_'+resample_to] = round((agg_data['high'] - agg_data['low']).expanding().max(), 2)
    # find_candle.find_candle(agg_data, duration=resample_to)

    if resample_to == 'M':
        agg_data['Prev_Mth_Chg'] = round(agg_data['close'].pct_change(2) * 100, 2)
        agg_data['Mth_EMA_20'] = round(agg_data['close'].ewm(span=20).mean(), 2)
        agg_data['high_6M'] = agg_data['high'].rolling(6).max()
        agg_data['low_6M'] = agg_data['low'].rolling(6).min()

    elif resample_to == 'W':
        agg_data['high_52W'] = agg_data['high'].rolling(52).max()
        agg_data['low_52W'] = agg_data['low'].rolling(52).min()
        agg_data['high_6W'] = agg_data['high'].rolling(6).max()
        agg_data['low_6W'] = agg_data['low'].rolling(6).min()
        agg_data['Wk_EMA_13'] = round(agg_data['close'].ewm(span=13).mean(), 2)
        agg_data['Wk_EMA_52'] = round(agg_data['close'].ewm(span=52).mean(), 2)

    elif resample_to == 'Y':
        max_years = len(agg_data)
        agg_data['Prev_Year_Chg'] = round(agg_data['close'].pct_change(2) * 100, 2)
        agg_data['3_Year_Returns'] = round(agg_data['close'].pct_change(3) * 100, 2)
        agg_data['5_Year_Returns'] = round(agg_data['close'].pct_change(5) * 100, 2)
        agg_data['Max_Returns'] = round(agg_data['close'].pct_change(max_years-1) * 100, 2)

    return agg_data


# stocks = ['NIFTY_100']
def stocks_agg_data_load():
    stock_list_df = rd.get_table_data(selected_table='STOCKS_IN_DB')
    stock_list = stock_list_df['SYMBOL'].values.tolist()
    # indices_df = rd.get_table_data(selected_table="STOCK_INDICES")
    # indices_list = indices_df['name'].values.tolist()
    # sectors_df = rd.get_table_data(selected_table="STOCK_SECTORS")
    # sectors_list = sectors_df['name'].values.tolist()

    # stocks_indices_sectors = stock_list + indices_list + sectors_list

    stocks_indices_sectors = stock_list

    failed_agg_load = []
    combined_agg_data = pd.DataFrame()
    for stock in stocks_indices_sectors:
        if "-" in stock:
            stock = stock.replace("-", "_")
        log.info(stock)
        try:
            get_query = f"select * from public.\"{stock}\" order by timestamp ASC"
            daily_data = rd.get_table_data(query=get_query)
            daily_data["timestamp"] = pd.to_datetime(daily_data["timestamp"])
            # daily_data["timestamp"] = pd.to_datetime(daily_data["timestamp"]).dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
            daily_data.set_index(daily_data['timestamp'], inplace=True, drop=True)
            daily_data['Pct_Chg_D'] = round(daily_data['close'].pct_change() * 100, 1)
            daily_data.index = pd.to_datetime(daily_data.index)

            agg_weekly_data = resample_daily_data(daily_data, 'W')
            agg_monthly_data = resample_daily_data(daily_data, 'M')
            # agg_quarterly_data = resample_daily_data(df, 'Q')
            agg_yearly_data = resample_daily_data(daily_data, 'Y')

            log.info("Start data load for the stock : {}".format(stock))
            # Write data to SQL Server table
            msg1 = rd.load_sql_data(data_to_load=agg_weekly_data, table_name=stock + '_W')
            log.debug(msg1)
            msg2 = rd.load_sql_data(data_to_load=agg_monthly_data, table_name=stock + '_M')
            log.debug(msg2)
            # msg3 = rd.load_sql_data(data_to_load=agg_quarterly_data, table_name=stock + '_Q')
            # print(msg3)
            msg4 = rd.load_sql_data(data_to_load=agg_yearly_data, table_name=stock + '_Y')
            log.debug(msg4)
            log.info("Data load done for the stock : {}".format(stock))

            daily_data['Range_D'] = round(daily_data['high'] - daily_data['low'], 1)
            daily_data['Symbol'] = stock
            ath = max(daily_data['high'])
            daily_data['ATH_Date'] = daily_data['timestamp'][daily_data['high'] == ath]
            daily_data['ATH_Date'] = daily_data['ATH_Date'].ffill()

            atl = min(daily_data['low'])
            daily_data['ATL_Date'] = daily_data['timestamp'][daily_data['low'] == atl]
            daily_data['ATL_Date'] = daily_data['ATL_Date'].ffill()
            # find_candle.find_candle(daily_data, duration='D')

            # Extract the last row from each DataFrame
            last_row_weekly = agg_weekly_data.iloc[-1].copy()
            last_row_monthly = agg_monthly_data.iloc[-1].copy()
            last_row_yearly = agg_yearly_data.iloc[-1].copy()
            # Add 52-Week high Date to daily data
            daily_data['high_52W_Date'] = daily_data['timestamp'][daily_data['high'] == last_row_weekly['high_52W']]
            daily_data['low_52W_Date'] = daily_data['timestamp'][daily_data['low'] == last_row_weekly['low_52W']]
            daily_data['high_52W_Date'] = daily_data['high_52W_Date'].ffill()
            daily_data['low_52W_Date'] = daily_data['low_52W_Date'].ffill()


            last_row_daily = daily_data.iloc[-1].copy()
            
            last_row_weekly.rename({'open': 'open_W', 'high': 'high_W', 'low': 'low_W', 'close': 'close_W',
                                    'volume': 'volume_W'}, inplace=True)
            last_row_monthly.rename({'open': 'open_M', 'high': 'high_M', 'low': 'low_M', 'close': 'close_M',
                                    'volume': 'volume_M'}, inplace=True)
            last_row_yearly.rename({'open': 'open_Y', 'high': 'high_Y', 'low': 'low_Y', 'close': 'close_Y',
                                    'volume': 'volume_Y'}, inplace=True)

            weekly_cols = ['open_W', 'high_W', 'low_W', 'close_W', 'volume_W', 'Percent_Chg_W', 'Range_W',
                           'Max_Chg_W', 'Max_Vol_W', 'high_52W', 'low_52W', 'LR_6_W', 'low_LR_6_W',
                           'high_LR_6_W', 'high_6W', 'low_6W', 'Wk_EMA_13', 'Wk_EMA_52']

            monthly_cols = ['open_M', 'high_M', 'low_M', 'close_M', 'volume_M', 'Percent_Chg_M', 'Range_M',
                            'Max_Chg_M', 'Max_Vol_M', 'Prev_Mth_Chg', 'Mth_EMA_20',
                            'high_6M', 'low_6M', 'LR_6_M', 'low_LR_6_M', 'high_LR_6_M',]

            yearly_cols = ['open_Y', 'high_Y', 'low_Y', 'close_Y', 'volume_Y', 'Percent_Chg_Y',
                           'Prev_Year_Chg', '3_Year_Returns', '5_Year_Returns', 'Max_Returns']

            combined_data = pd.concat([last_row_daily,
                                       last_row_weekly[weekly_cols],
                                       last_row_monthly[monthly_cols],
                                       last_row_yearly[yearly_cols]]).to_frame().T
            combined_data_df = pd.DataFrame([combined_data.values.flatten()], columns=combined_data.columns)

            # combined_agg_data = pd.concat([combined_agg_data, combined_data], axis=0)
            combined_agg_data = pd.concat([combined_agg_data, combined_data_df], axis=0)

        except Exception as e:
            log.error(e)
            log.warning("Skipped data load for the stock : {}".format(stock))
            failed_agg_load.append(stock)
            continue

    msg5 = rd.load_sql_data(data_to_load=combined_agg_data, table_name='AGG_DATA')
    log.info(msg5)
    msg = 'Aggregated data load is successful for all the stocks'
    log.info(msg)
    if failed_agg_load:
        log.debug(f'Agg data load failed for stocks - {failed_agg_load}')
    return 'Success' if not failed_agg_load else 'Failed'


if __name__ == "__main__":
    stocks_agg_data_load()
