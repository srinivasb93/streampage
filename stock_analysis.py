import streamlit as st
import yfinance as yf
import pandas as pd
import plotly.graph_objs as go
from common_utils import read_write_sql_data as rd
from lightweight_charts.widgets import StreamlitChart
from python_scripts.archive.daily_data_load_archive import Dataload
import pandas_ta as ta


# ##########Function ############
state = st.session_state

if "indicators" not in state:
    state.indicators = dict()
    state.clear_indicator = False


# Some of the utility function
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
        if data_src == "Yahoo" and asset == "Index":
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


def calculate_stock_technical_summary(stock_df):
    if len(stock_df) > 20:
        stock_df.ta.ema(length=20, append=True)
    if len(stock_df) > 200:
        stock_df.ta.ema(length=200, append=True)
    if len(stock_df) > 14:
        stock_df.ta.rsi(length=14, append=True)
        stock_df.ta.adx(length=14, append=True)
        stock_df.ta.atr(length=14, append=True)
    return stock_df


# Function to get emoji based on returns value
def get_returns_emoji(ret_val):
    emoji = ":white_check_mark:"
    if ret_val < 0:
        emoji = ":red_circle:"
    return emoji


# Function to get emoji based on ema value
def get_ema_emoji(ltp, ema):
    emoji = ":white_check_mark:"
    if ltp < ema:
        emoji = ":red_circle:"
    return emoji


# Function to get emoji based on rsi value
def get_rsi_emoji(rsi):
    emoji = ":red_circle:"
    if 30 < rsi < 70:
        emoji = ":white_check_mark:"
    return emoji


# Function to get emoji based on adx value
def get_adx_emoji(adx):
    emoji = ":red_circle:"
    if adx > 25:
        emoji = ":white_check_mark:"
    return emoji


# Function to create chart
def create_chart(df):
    candlestick_chart = go.Figure(data=[go.Candlestick(x=df.index,open=df['Open'],high=df['High'],low=df['Low'],close=df['Close'])])
    ema20 = go.Scatter(x = df.EMA_20.index,y = df.EMA_20.values,name = 'EMA20')
    ema200 = go.Scatter(x = df.EMA_200.index,y = df.EMA_200.values,name = 'EMA200')
    # Create the candlestick chart
    candlestick_chart.update_layout(title=f'{stock_name} Historical Candlestick Chart',
                                        xaxis_title='Date',
                                        yaxis_title='Price',
                                        xaxis_rangeslider_visible=True)
    candlestick_chart.add_trace(ema20)
    candlestick_chart.add_trace(ema200)
    return candlestick_chart
# #########End of function #######


header_col, theme_col, hide_ind_col = st.columns([1.2, .5, .5], vertical_alignment="bottom")
with header_col:
    st.subheader(""" :rainbow[Srini's  Stock :green[Technical] :red[Analysis] Dashboard!]  """)

with theme_col:
    dark_theme = st.checkbox(":blue-background[Dark Theme]")

with hide_ind_col:
    hide_indicators = st.checkbox(":blue-background[Hide Indicators Section]")

# Sidebar Components
data_src = st.sidebar.radio(label="SQL or Yahoo",
                            options=["SQL", "Yahoo"],
                            label_visibility="collapsed",
                            horizontal=True)

asset = st.sidebar.radio(label="Stock or Index",
                         options=["Stock", "Index"],
                         label_visibility="collapsed",
                         horizontal=True)

dataload = Dataload()
stock_db = 'NSEDATA'
if data_src == "Yahoo" and asset == "Index":
    tables_list = ["NIFTY_50", "NIFTY_BANK", "NIFTY_FIN_SERVICE", "NIFTY_MIDCAP_50"]
else:
    tables_list = sorted(dataload.get_stocks_index_data(asset))
default_asset = "TATAMOTORS" if asset == 'Stock' else "NIFTY_50"

stock_name = st.sidebar.selectbox("Select Stock Symbol", tables_list, index=tables_list.index(default_asset))
if data_src == 'SQL':
    timeframe_option = st.sidebar.selectbox("Choose Timeframe", ('Daily', 'Weekly', 'Monthly', 'Yearly'))
else:
    period_col, interval_col = st.sidebar.columns(2)
    with period_col:
        yf_period = st.sidebar.selectbox("Period", ("1y", "5d", "1mo", "3mo", "6mo", "1d",
                                                    "2y", "5y", "10y", "ytd", "max"))
    with interval_col:
        yf_interval = st.sidebar.selectbox("Interval", ("1d", "1m", "2m", "5m", "15m", "30m", "60m", "90m",
                                                        "1h", "5d", "1wk", "1mo", "3mo"))
show_summary = st.sidebar.checkbox(label="Show Summary")
show_data = st.sidebar.checkbox(label="Show Data")
show_chart = st.sidebar.checkbox(label="Show Chart", value=True)

if data_src == 'SQL':
    df = extract_stock_data(stock_name, data_source=data_src, period_sql=timeframe_option)
else:
    df = extract_stock_data(stock_name, data_source=data_src, period_yf=yf_period, interval_yf=yf_interval)

if data_src == 'SQL':
    daily_data_cond = True if timeframe_option == 'Daily' else False
else:
    daily_data_cond = True if yf_interval == '1d' else False

# Column wise Display
if show_summary:
    if daily_data_cond:
        summary_data = calculate_stock_technical_summary(df)
        reversed_df = summary_data.iloc[::-1]
        row1_val = reversed_df.iloc[0]['Close']
        ema20_val = reversed_df.iloc[0]['EMA_20']
        if len(reversed_df) > 200:
            ema200_val = reversed_df.iloc[0]['EMA_200']
        rsi_val = reversed_df.iloc[0]['RSI_14']
        adx = reversed_df.iloc[0]['ADX_14']
        dmp = reversed_df.iloc[0]['DMP_14']
        dmn = reversed_df.iloc[0]['DMN_14']
        # row1_date =  reversed_df.iloc[0]['time']
        row20_val = reversed_df.iloc[20]['Close']  # 1 month return
        day20_ret_percent = (row1_val - row20_val) / row20_val * 100
        day20_ret_val = (row1_val - row20_val)

        if len(reversed_df) > 60:
            row60_val = reversed_df.iloc[60]['Close']  # 3 months return
            day60_ret_percent = (row1_val - row60_val) / row60_val * 100
            day60_ret_val = (row1_val - row60_val)

        if len(reversed_df) > 120:
            row120_val = reversed_df.iloc[120]['Close']  # 6 months return
            day120_ret_percent = (row1_val - row120_val) / row120_val * 100
            day120_ret_val = (row1_val - row120_val)

        if len(reversed_df) > 240:
            row240_val = reversed_df.iloc[240]['Close']  # 12 months return
            day240_ret_percent = (row1_val - row240_val) / row240_val * 100
            day240_ret_val = (row1_val - row240_val)

            col1, col2, col3 = st.columns(3)
            with col1:
                st.subheader("Returns")
                st.markdown(f"- 1  MONTH : {round(day20_ret_percent,2)}% {get_returns_emoji(round(day20_ret_percent, 2))}")
                if len(reversed_df) > 60:
                    st.markdown(f"- 3  MONTHS : {round(day60_ret_percent,2)}% {get_returns_emoji(round(day60_ret_percent, 2))}")
                if len(reversed_df) > 120:
                    st.markdown(f"- 6  MONTHS : {round(day120_ret_percent,2)}% {get_returns_emoji(round(day120_ret_percent, 2))}")
                if len(reversed_df) > 240:
                    st.markdown(f"- 12 MONTHS : {round(day240_ret_percent,2)}% {get_returns_emoji(round(day240_ret_percent, 2))}")
            with col2:
                st.subheader("Momentum")
                st.markdown(f"- LTP : {round(row1_val,2)}")
                if len(reversed_df) > 20:
                    st.markdown(f"- EMA20 : {round(ema20_val,2)} {get_ema_emoji(round(row1_val,2),round(ema20_val,2))}")
                if len(reversed_df) > 200:
                    st.markdown(f"- EMA200 : {round(ema200_val,2)} {get_ema_emoji(round(row1_val,2),round(ema200_val,2))}")
                st.markdown(f"- RSI : {round(rsi_val,2)} {get_rsi_emoji(round(rsi_val,2))}")
            with col3:
                st.subheader("Trend Strength")
                st.markdown(f"- ADX : {round(adx,2)} {get_adx_emoji(round(adx,2))}")
                st.markdown(f"- DMP : {round(dmp,2)} ")
                st.markdown(f"- DMN : {round(dmn,2)} ")


def set_indicator_values(indicator, ind_duration, ind_color):
    # print("Clear Indicator",  state.clear_indicator)
    # print("Indicator before set", state.indicators)
    if not state.clear_indicator:
        state.indicators.update({f'{indicator}_{ind_duration}': {"color_code": ind_color, "ind_status": True}})
        # print("Indicator after set", state.indicators)
    state.clear_indicator = False


def clear_indicator_values():
    del state.indicators
    state.clear_indicator = True


def add_indicator_line(chart, df, indicator, period=10, indicator_color="#2596be"):
    indicator_line = chart.create_line(name=f'{indicator}_{period}', color=indicator_color, width=1.5, price_label=True)
    ind_df = pd.DataFrame(columns=['time', f'{indicator}_{period}'])
    try:
        ind_df.time = df.Date
    except Exception:
        # Added this exception as Date is part of index in Yahoo data
        ind_df.time = df.index
    if indicator == 'EMA':
        ind_df[f'{indicator}_{period}'] = df.Close.ewm(span=period).mean()
    elif indicator == 'LinReg':
        ind_df[f'{indicator}_{period}'] = df.ta.linreg(period)
    indicator_line.set(ind_df.dropna())


def create_tv_chart(chart, data, theme='default'):
    chart.legend(True,  font_size=20, color_based_on_candle=True, color="#1e81b0")
    chart.topbar.textbox('symbol', stock_name, align='left')
    chart.set(data)
    # print(state.indicators)
    for indicator_with_duration, ind_color_state in state.indicators.items():
        indicator = indicator_with_duration.split("_")[0]
        ind_duration = int(indicator_with_duration.split("_")[1])
        ind_color = ind_color_state['color_code']
        add_indicator_line(chart, data, indicator, ind_duration, ind_color)

    if theme == 'default':
        chart.layout(background_color="#FFFFFF", text_color="#000000")
        chart.grid(style='dashed', color='#E0E0D0')
    else:
        chart.layout(background_color="#161616")

    chart.load()


if show_chart:
    # print("Chart starting", state.indicators)
    chart_obj = StreamlitChart(width=1250, height=600, toolbox=True)

    form_col, clear_btn = st.columns([4, 2], vertical_alignment="center", gap="small")
    if not hide_indicators:
        with form_col:
            with st.form("set_indicators"):
                ind, period_col, color_pick, sub_btn = st.columns([1, 1, .6, .6], vertical_alignment="bottom")
                with ind:
                    selected_indicator = st.selectbox("Indicators", options=["EMA", "LinReg"])
                with period_col:
                    ind_period = st.number_input("period", value=5, min_value=2, max_value=100)
                    # set_indicator_values(selected_indicator, ind_period)
                with color_pick:
                    color_code_chosen = st.color_picker(label="Choose Color", value='#2596be')
                with sub_btn:
                    st.form_submit_button("Add Indicator", on_click=set_indicator_values,
                                          args=[selected_indicator, ind_period, color_code_chosen])
        with clear_btn:
            with st.form("clear_indicators"):
                indicators_selected, clear_btn = st.columns(2, vertical_alignment="bottom")
                indicators_to_clear = indicators_selected.multiselect(
                    label="Indicators Set",
                    options=list(state.indicators.keys()) + ["All"] if state.indicators.keys() else [])
                clear_btn.form_submit_button("Clear Indicator(s)", on_click=clear_indicator_values)

    create_tv_chart(chart_obj, df, theme='dark' if dark_theme else 'default')
    # create_chart(df)

    # st.plotly_chart(create_chart(df))


if show_data:
    st.write(df.loc[::-1])
