import pandas as pd
from common_utils import read_write_sql_data as rd
from python_scripts.candle import find_candle
import logging

log = logging.getLogger()
logging.basicConfig(filename=r"C:\Users\sba400\MyProject\streampage\python_scripts\logfiles\AGG_DATA_LOAD.log",
                    format=f"%(asctime)s : %(name)s : %(message)s",
                    level='DEBUG')


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
    agg_data = daily_data.copy()
    agg_data = agg_data.resample(resample_to).agg(logic)
    agg_data.reset_index(inplace=True)

    agg_data['Percent_Chg_'+resample_to] = round(agg_data['Close'].pct_change()*100, 2)
    agg_data['Range_'+resample_to] = round(agg_data['High'] - agg_data['Low'], 2)
    agg_data['Max_Chg_'+resample_to] = agg_data['Percent_Chg_' + resample_to].expanding().max()
    agg_data['Max_Vol_'+resample_to] = agg_data['Volume'].expanding().max()
    agg_data['Max_Range_'+resample_to] = round((agg_data['High'] - agg_data['Low']).expanding().max(), 2)
    # find_candle.find_candle(agg_data, duration=resample_to)

    if resample_to == 'M':
        agg_data['Prev_Mth_Chg'] = round(agg_data['Close'].pct_change(2) * 100, 2)
        agg_data['Mth_EMA_20'] = round(agg_data['Close'].ewm(span=20).mean(), 2)
        agg_data['High_6M'] = agg_data['High'].rolling(6).max()
        agg_data['Low_6M'] = agg_data['Low'].rolling(6).min()

    elif resample_to == 'W':
        agg_data['High_52W'] = agg_data['High'].rolling(52).max()
        agg_data['Low_52W'] = agg_data['Low'].rolling(52).min()
        agg_data['High_6W'] = agg_data['High'].rolling(6).max()
        agg_data['Low_6W'] = agg_data['Low'].rolling(6).min()
        agg_data['Wk_EMA_13'] = round(agg_data['Close'].ewm(span=13).mean(), 2)
        agg_data['Wk_EMA_52'] = round(agg_data['Close'].ewm(span=52).mean(), 2)

    elif resample_to == 'Y':
        max_years = len(agg_data)
        agg_data['Prev_Year_Chg'] = round(agg_data['Close'].pct_change(2) * 100, 2)
        agg_data['3_Year_Returns'] = round(agg_data['Close'].pct_change(3) * 100, 2)
        agg_data['5_Year_Returns'] = round(agg_data['Close'].pct_change(5) * 100, 2)
        agg_data['Max_Returns'] = round(agg_data['Close'].pct_change(max_years-1) * 100, 2)

    return agg_data


# stocks = ['NIFTY_100']
def stocks_agg_data_load():
    query = """select SYMBOL from [NSEDATA].[dbo].[ALL_STOCKS] where stk_index = 'NIFTY 200'
                UNION
                select [Stock_Symbol] from [ANALYTICS].[dbo].[EQUITY_HOLDINGS]
                UNION 
                SELECT NAME FROM dbo.STOCK_INDICES UNION SELECT NAME FROM dbo.STOCK_SECTORS"""
    stocks_data = rd.get_table_data(query=query)
    stocks = stocks_data['SYMBOL'].values.tolist()
    failed_agg_load = []
    combined_agg_data = pd.DataFrame()
    for stock in stocks:
        if "&" in stock or "-" in stock:
            stock = stock.replace("&", "").replace("-", "")
        log.info(stock)
        try:
            get_query = f"select * from [NSEDATA].[dbo].{stock} order by Date ASC"
            daily_data = rd.get_table_data(query=get_query)
            daily_data.set_index(daily_data['Date'], inplace=True, drop=True)
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

            daily_data['Range_D'] = round(daily_data['High'] - daily_data['Low'], 1)
            daily_data['Symbol'] = stock
            ath = max(daily_data['High'])
            daily_data['ATH_Date'] = daily_data['Date'][daily_data['High'] == ath]
            daily_data['ATH_Date'].ffill(inplace=True)
            atl = min(daily_data['Low'])
            daily_data['ATL_Date'] = daily_data['Date'][daily_data['Low'] == atl]
            daily_data['ATL_Date'].ffill(inplace=True)
            # find_candle.find_candle(daily_data, duration='D')

            # Extract the last row from each DataFrame
            last_row_daily = daily_data.iloc[-1].copy()
            last_row_weekly = agg_weekly_data.iloc[-1].copy()
            last_row_monthly = agg_monthly_data.iloc[-1].copy()
            last_row_yearly = agg_yearly_data.iloc[-1].copy()
            
            last_row_weekly.rename({'Open': 'Open_W', 'High': 'High_W', 'Low': 'Low_W', 'Close': 'Close_W',
                                    'Volume': 'Volume_W'}, inplace=True)
            last_row_monthly.rename({'Open': 'Open_M', 'High': 'High_M', 'Low': 'Low_M', 'Close': 'Close_M',
                                    'Volume': 'Volume_M'}, inplace=True)
            last_row_yearly.rename({'Open': 'Open_Y', 'High': 'High_Y', 'Low': 'Low_Y', 'Close': 'Close_Y',
                                    'Volume': 'Volume_Y'}, inplace=True)

            weekly_cols = ['Open_W', 'High_W', 'Low_W', 'Close_W', 'Volume_W', 'Percent_Chg_W', 'Range_W',
                           'Max_Chg_W', 'Max_Vol_W', 'Max_Range_W', 'High_52W', 'Low_52W',
                           'High_6W', 'Low_6W', 'Wk_EMA_13', 'Wk_EMA_52']

            monthly_cols = ['Open_M', 'High_M', 'Low_M', 'Close_M', 'Volume_M', 'Percent_Chg_M', 'Range_M',
                            'Max_Chg_M', 'Max_Vol_M', 'Max_Range_M', 'Prev_Mth_Chg', 'Mth_EMA_20',
                            'High_6M', 'Low_6M']

            yearly_cols = ['Open_Y', 'High_Y', 'Low_Y', 'Close_Y', 'Volume_Y', 'Percent_Chg_Y', 'Range_Y',
                            'Max_Chg_Y', 'Max_Vol_Y', 'Max_Range_Y', 'Prev_Year_Chg', '3_Year_Returns',
                            '5_Year_Returns', 'Max_Returns']

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
