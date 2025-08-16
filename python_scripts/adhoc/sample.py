import pandas as pd
import pandas_ta as ta
import numpy as np
from scipy.signal import argrelextrema
from lightweight_charts.widgets import StreamlitChart
from common_utils import read_write_sql_data as rd
from python_scripts.archive.daily_data_load_archive import Dataload
import time

print(time.time())

def extract_stock_data(stock_name, data_source='SQL', period_sql='Daily', period_yf='1y', interval_yf='1d'):
    periods = {"Weekly": "_W", "Monthly": "_M", "Quarterly": "_Q", "Yearly": "_Y"}
    stock_name += periods.get(period_sql, "")
    query = f'Select * from public.{stock_name} order by Date ASC'
    df = rd.get_table_data(query=query)
    df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
    df["Date"] = pd.to_datetime(df["Date"])
    df.set_index("Date", inplace=True)

    df["Price_Chg"] = round(df["Close"].pct_change() * 100, 1)
    return df

def remove_close_levels(levels, threshold=0.02):
    levels = sorted(levels)
    return [level for i, level in enumerate(levels) if
            i == 0 or all(abs(level - r) / r > threshold for r in levels[:i])]

def calculate_support_resistance(data, window=12):
    stock_name = 'TATAMOTORS'
    data_src = 'SQL'
    data = extract_stock_data(stock_name, data_source=data_src, period_sql="Daily")
    monthly_data = extract_stock_data(stock_name, data_source=data_src, period_sql="Monthly")
    weekly_data = extract_stock_data(stock_name, data_source=data_src, period_sql="Weekly")
    weekly_data = weekly_data.tail(104)


    daily_data = data.tail(200)
    low, high = daily_data['Low'].values, daily_data['High'].values
    low_idx = argrelextrema(low, np.less, order=20)[0]
    high_idx = argrelextrema(high, np.greater, order=20)[0]
    daily_support_raw = low[low_idx]
    daily_resistance_raw = high[high_idx]

    daily_support = remove_close_levels(low[low_idx])
    daily_resistance = remove_close_levels(high[high_idx])

    mth_low, mth_high = monthly_data['Low'].values, monthly_data['High'].values
    mth_low_idx = argrelextrema(mth_low, np.less, order=window)[0]
    mth_high_idx = argrelextrema(mth_high, np.greater, order=window)[0]
    mth_support_raw = mth_low[mth_low_idx]
    mth_resistance_raw = mth_high[mth_high_idx]

    mth_support = remove_close_levels(mth_low[mth_low_idx])
    mth_resistance = remove_close_levels(mth_high[mth_high_idx])

    wk_low, wk_high = weekly_data['Low'].values, weekly_data['High'].values
    wk_low_idx = argrelextrema(wk_low, np.less, order=window)[0]
    wk_high_idx = argrelextrema(wk_high, np.greater, order=window)[0]
    wk_support_raw = wk_low[wk_low_idx]
    wk_resistance_raw = wk_high[wk_high_idx]

    wk_support = remove_close_levels(wk_low[wk_low_idx])
    wk_resistance = remove_close_levels(wk_high[wk_high_idx])

    print(f"Daily Support Raw- {daily_support_raw}")
    print(f"Daily Support - {daily_support}")
    print(f"Weekly Support raw - {wk_support_raw}")
    print(f"Weekly Support - {wk_support}")
    print(f"Monthly Support raw- {mth_support_raw}")
    print(f"Monthly Support - {mth_support}")
    print(f"Daily Resistance - {daily_resistance}")
    print(f"Weekly Resistance - {wk_resistance}")
    print(f"Monthly Resistance - {mth_resistance}")
    support = remove_close_levels(daily_support + wk_support + mth_support)
    resistance = remove_close_levels(daily_resistance + wk_resistance + mth_resistance)

    return support, resistance

calculate_support_resistance(data={})