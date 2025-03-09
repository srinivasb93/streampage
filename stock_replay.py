import streamlit as st
import yfinance as yf
import pandas as pd
import pandas_ta as ta
from streamlit_lightweight_charts import renderLightweightCharts
from common_utils import read_write_sql_data as rd

# Function to fetch stock data
def extract_stock_data(stock_name, data_source='SQL', period_sql='Daily', period_yf='1y', interval_yf='1d'):
    """
    Retrieve stock data from source based on the data source selected
    period : str
                Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
                Either Use period parameter or use start and end
            interval : str
                Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
                Intraday data cannot extend last 60 days
    """
    if data_source == 'SQL':
        periods = {"Weekly": "_W", "Monthly": "_M", "Quarterly": "_Q", "Yearly": "_Y"}
        stock_name += periods.get(period_sql, "")
        query = f'Select * from dbo.{stock_name} order by Date ASC'
        df = rd.get_table_data(query=query)
        df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df["Date"] = pd.to_datetime(df["Date"], format='ISO8601')
    else:
        if data_source == "Yahoo":
            indices = {"NIFTY_50": "^NSEI", "NIFTY_BANK": "^NSEBANK",
                       "NIFTY_FIN_SERVICE": "NIFTY_FIN_SERVICE.NS", "NIFTY_MIDCAP_50": "^NSEMDCP50"}
            yf_index = indices.get(stock_name, "^NSEI")
            df = yf.Ticker(yf_index).history(period=period_yf,
                                             interval=interval_yf)[['Open', 'High', 'Low', 'Close', 'Volume']]
        else:
            df = yf.Ticker(stock_name+'.NS').history(period=period_yf,
                                                interval=interval_yf)[['Open', 'High', 'Low', 'Close', 'Volume']]
        df.reset_index(inplace=True)
    df["Price_Chg"] = round(df["Close"].pct_change()*100, 1)
    return df

# Function to add indicators
def add_indicators(data, indicators):
    if 'EMA' in indicators:
        data['EMA'] = ta.ema(data['Close'], length=20)
    if 'Lin_Reg' in indicators:
        data['Lin_Reg'] = ta.linreg(data['Close'], length=20)
    if 'RSI' in indicators:
        data['RSI'] = ta.rsi(data['Close'], length=14)
    if 'MACD' in indicators:
        macd = ta.macd(data['Close'])
        data['MACD'] = macd['MACD_12_26_9']
        data['MACD_Signal'] = macd['MACDs_12_26_9']
        data['MACD_Hist'] = macd['MACDh_12_26_9']
    if 'Bollinger Bands' in indicators:
        bb = ta.bbands(data['Close'])
        data['BB_Upper'] = bb['BBU_20_2.0']
        data['BB_Lower'] = bb['BBL_20_2.0']
    return data

# Streamlit app layout
st.title("Stock Data Replay with TradingView API")

ticker = st.text_input("Enter Stock Ticker", "AAPL")
start_date = st.date_input("Select Start Date")
end_date = st.date_input("Select End Date")
indicators = st.multiselect("Select Indicators", ['EMA', 'Lin_Reg', 'RSI', 'MACD', 'Bollinger Bands'])

if st.button("Fetch Data"):
    data = extract_stock_data(ticker, start_date, end_date)
    data = add_indicators(data, indicators)
    st.write(data)

    # Prepare data for chart
    candlestick_data = data[['Date', 'Open', 'High', 'Low', 'Close']].to_dict('records')
    series = [{"type": "Candlestick", "data": candlestick_data}]

    if 'EMA' in indicators:
        ema_data = data[['Date', 'EMA']].dropna().to_dict('records')
        series.append({"type": "Line", "data": ema_data, "options": {"color": "blue"}})
    if 'Lin_Reg' in indicators:
        linreg_data = data[['time', 'Lin_Reg']].dropna().to_dict('records')
        series.append({"type": "Line", "data": linreg_data, "options": {"color": "green"}})
    if 'RSI' in indicators:
        rsi_data = data[['time', 'RSI']].dropna().to_dict('records')
        series.append({"type": "Line", "data": rsi_data, "options": {"color": "red"}})
    if 'MACD' in indicators:
        macd_data = data[['time', 'MACD']].dropna().to_dict('records')
        macd_signal_data = data[['time', 'MACD_Signal']].dropna().to_dict('records')
        macd_hist_data = data[['time', 'MACD_Hist']].dropna().to_dict('records')
        series.append({"type": "Line", "data": macd_data, "options": {"color": "purple"}})
        series.append({"type": "Line", "data": macd_signal_data, "options": {"color": "orange"}})
        series.append({"type": "Histogram", "data": macd_hist_data, "options": {"color": "gray"}})
    if 'Bollinger Bands' in indicators:
        bb_upper_data = data[['time', 'BB_Upper']].dropna().to_dict('records')
        bb_lower_data = data[['time', 'BB_Lower']].dropna().to_dict('records')
        series.append({"type": "Line", "data": bb_upper_data, "options": {"color": "cyan"}})
        series.append({"type": "Line", "data": bb_lower_data, "options": {"color": "magenta"}})

    renderLightweightCharts([{"chart": {}, "series": series}], key="chart")

# Replay controls
if st.button("Start Replay"):
    st.write("Replay started")
if st.button("Pause/Resume Replay"):
    st.write("Replay paused/resumed")
if st.button("Stop Replay"):
    st.write("Replay stopped")
