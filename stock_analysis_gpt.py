import streamlit as st
import yfinance as yf
import pandas as pd
import pandas_ta as ta
import numpy as np
from scipy.signal import argrelextrema
from lightweight_charts.widgets import StreamlitChart
from common_utils import read_write_sql_data as rd
from python_scripts.archive.daily_data_load_archive import Dataload
import time

# Initialize session state
if "indicators" not in st.session_state:
    st.session_state.indicators = {}
if "current_replay_index" not in st.session_state:
    st.session_state.current_replay_index = -1
if "last_replay_date" not in st.session_state:
    st.session_state.last_replay_date = None
if "is_playing" not in st.session_state:
    st.session_state.is_playing = False


@st.cache_data
def extract_stock_data(stock_name, data_source='SQL', period_sql='Daily', period_yf='1y', interval_yf='1d'):
    if data_source == 'SQL':
        periods = {"Weekly": "_W", "Monthly": "_M", "Quarterly": "_Q", "Yearly": "_Y"}
        stock_name += periods.get(period_sql, "")
        query = f'Select * from dbo.{stock_name} order by Date ASC'
        df = rd.get_table_data(query=query)
        df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df["Date"] = pd.to_datetime(df["Date"])
        df.set_index("Date", inplace=True)
    else:
        ticker = f"^{stock_name}" if data_source == "Yahoo" and asset == "Index" else f"{stock_name}.NS"
        df = yf.Ticker(ticker).history(period=period_yf, interval=interval_yf)[
            ['Open', 'High', 'Low', 'Close', 'Volume']]
    df["Price_Chg"] = round(df["Close"].pct_change() * 100, 1)
    return df


def calculate_stock_technical_summary(stock_df, indicators):
    for indicator, params in indicators.items():
        if indicator == 'EMA':
            stock_df.ta.ema(length=params['period'], append=True)
        elif indicator == 'SMA':
            stock_df.ta.sma(length=params['period'], append=True)
        elif indicator == 'WMA':
            stock_df.ta.wma(length=params['period'], append=True)
        elif indicator == 'BBANDS':
            stock_df.ta.bbands(length=params['period'], std=params['std'], append=True)
        elif indicator == 'MACD':
            stock_df.ta.macd(fast=params['fast'], slow=params['slow'], signal=params['signal'], append=True)
        elif indicator == 'RSI':
            stock_df.ta.rsi(length=params['period'], append=True)
        elif indicator == 'LINREG':
            stock_df.ta.linreg(length=params['period'], append=True)
    return stock_df


def calculate_stock_summary(stock_df):
    if len(stock_df) > 20:
        stock_df.ta.ema(length=20, append=True)
    if len(stock_df) > 200:
        stock_df.ta.ema(length=200, append=True)
    if len(stock_df) > 14:
        stock_df.ta.rsi(length=14, append=True)
        stock_df.ta.adx(length=14, append=True)
        stock_df.ta.atr(length=14, append=True)
    return stock_df


def get_emoji(value, condition):
    return ":white_check_mark:" if condition(value) else ":red_circle:"


def create_tv_chart(chart, data, theme='default'):
    chart.legend(True, font_size=20, color_based_on_candle=True, color="#1e81b0")
    chart.topbar.textbox('symbol', stock_name, align='center')
    chart.set(data, keep_drawings=True)

    # Create subcharts for RSI and MACD
    subcharts = {}
    for indicator in ['RSI', 'MACD']:
        if indicator in st.session_state.indicators:
            # if both RSI and MACD are to be seen, then
            if 'RSI' in st.session_state.indicators and 'MACD' in st.session_state.indicators:
                subcharts[indicator] = chart.create_subchart(position='bottom', height=0.2, width=1, sync=True)
                chart.resize(width=1, height=.6)
            else:
                subcharts[indicator] = chart.create_subchart(position='bottom', height=0.2, width=1, sync=True)
                chart.resize(width=1, height=.8)
            subcharts[indicator].time_scale(visible=False)
            subcharts[indicator].layout(background_color="#161616" if theme == 'dark' else "#FFFFFF",
                                        text_color="#FFFFFF" if theme == 'dark' else "#000000")
            subcharts[indicator].fit()
            subcharts[indicator].legend(True)
            # chart.time_scale(visible=False)

    for indicator, params in st.session_state.indicators.items():
        if indicator in ['RSI', 'MACD']:
            # Add indicator to its subchart
            add_indicator_line(subcharts[indicator], data, indicator, params)
        else:
            # Add other indicators to the main chart
            add_indicator_line(chart, data, indicator, params)

    chart.layout(background_color="#161616" if theme == 'dark' else "#FFFFFF",
                 text_color="#FFFFFF" if theme == 'dark' else "#000000")
    chart.grid(style='dashed', color='#D3D3D3' if theme != 'dark' else "#262616")
    # chart.watermark(stock_name)
    chart.load()


def add_indicator_line(chart, df, indicator, params):
    color = params['color_code']
    if indicator == 'BBANDS':
        upper = chart.create_line(name=f'BBU_{params["period"]}', color='red',
                                  width=1.25, price_label=True, price_line=False)
        middle = chart.create_line(name=f'BBM_{params["period"]}', color='cyan',
                                   width=1.25, price_label=True, price_line=False)
        lower = chart.create_line(name=f'BBL_{params["period"]}', color='green',
                                  width=1.25, price_label=True, price_line=False)

        ind_df = pd.DataFrame({
            'time': df.index,
            f'BBU_{params["period"]}': df[f'BBU_{params["period"]}_{params["std"]}'],
            f'BBM_{params["period"]}': df[f'BBM_{params["period"]}_{params["std"]}'],
            f'BBL_{params["period"]}': df[f'BBL_{params["period"]}_{params["std"]}']
        })
        upper.set(ind_df[['time', f'BBU_{params["period"]}']].dropna())
        middle.set(ind_df[['time', f'BBM_{params["period"]}']].dropna())
        lower.set(ind_df[['time', f'BBL_{params["period"]}']].dropna())
    elif indicator == 'MACD':
        macd_line = chart.create_line(name='MACD', color=color, width=1.5, price_line=False)
        signal_line = chart.create_line(name='MACD_Signal', color='red', width=1.5, price_line=False)
        histogram = chart.create_histogram(name='MACD_Hist', color=color, price_line=False)

        ind_df = pd.DataFrame({
            'time': df.index,
            'MACD': df[f'MACD_{params["fast"]}_{params["slow"]}_{params["signal"]}'],
            'MACD_Signal': df[f'MACDs_{params["fast"]}_{params["slow"]}_{params["signal"]}'],
            'MACD_Hist': df[f'MACDh_{params["fast"]}_{params["slow"]}_{params["signal"]}']
        })
        macd_line.set(ind_df[['time', 'MACD']])
        signal_line.set(ind_df[['time', 'MACD_Signal']])
        histogram.set(ind_df[['time', 'MACD_Hist']])
    elif indicator == 'RSI':
        rsi_line = chart.create_line(name=f'RSI_{params["period"]}', color=color, width=1.5, price_line=False)
        ind_df = pd.DataFrame({'time': df.index, f'RSI_{params["period"]}': df[f'RSI_{params["period"]}']})
        rsi_line.set(ind_df)
    elif indicator == 'LINREG':
        linreg_line = chart.create_line(name=f'LR_{params["period"]}', color=color, width=1.5,
                                        price_label=True, price_line=False)
        ind_df = pd.DataFrame({'time': df.index, f'LR_{params["period"]}': df[f'LR_{params["period"]}']})
        linreg_line.set(ind_df.dropna())
    else:
        indicator_line = chart.create_line(name=f'{indicator}_{params["period"]}', color=color, width=1.5,
                                           price_label=True, price_line=False)
        ind_df = pd.DataFrame(
            {'time': df.index, f'{indicator}_{params["period"]}': df[f'{indicator}_{params["period"]}']})
        indicator_line.set(ind_df.dropna())


def calculate_support_resistance(data, window=10):
    low, high = data['Low'].values, data['High'].values
    low_idx = argrelextrema(low, np.less, order=window)[0]
    high_idx = argrelextrema(high, np.greater, order=window)[0]
    support = remove_close_levels(low[low_idx])
    resistance = remove_close_levels(high[high_idx])
    return support, resistance


def remove_close_levels(levels, threshold=0.02):
    levels = sorted(levels)
    return [level for i, level in enumerate(levels) if
            i == 0 or all(abs(level - r) / r > threshold for r in levels[:i])]


# Streamlit UI
# st.set_page_config(layout="wide")
header_col, replay_button, pattern_col, theme_col = st.columns([.25, .43, .12, .1],
                                                               vertical_alignment='center',
                                                               gap='small')

# Sidebar
data_src = st.sidebar.radio("Data Source", ["SQL", "Yahoo"], horizontal=True)
asset = st.sidebar.radio("Asset Type", ["Stock", "Index"], horizontal=True)

dataload = Dataload()
tables_list = sorted(dataload.get_stocks_index_data(asset))
default_asset = "TATAMOTORS" if asset == 'Stock' else "NIFTY_50"
stock_name = st.sidebar.selectbox("Select Stock Symbol", tables_list, index=tables_list.index(default_asset))

if data_src == 'SQL':
    timeframe_option = st.sidebar.selectbox("Choose Timeframe", ('Daily', 'Weekly', 'Monthly', 'Yearly'))
    df = extract_stock_data(stock_name, data_source=data_src, period_sql=timeframe_option)
else:
    yf_period = st.sidebar.selectbox("Period", ("1y", "5d", "1mo", "3mo", "6mo", "1d", "2y", "5y", "10y", "ytd", "max"))
    yf_interval = st.sidebar.selectbox("Interval", (
    "1d", "1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "5d", "1wk", "1mo", "3mo"))
    df = extract_stock_data(stock_name, data_source=data_src, period_yf=yf_period, interval_yf=yf_interval)

# Add replay date selector
replay_date = st.sidebar.date_input("Select Replay Date", value=df.index[-1].date(), min_value=df.index[0].date(), max_value=df.index[-1].date())

# Update current_replay_index only when the page loads for the first time or when the replay_date changes
if st.session_state.current_replay_index == -1 or replay_date != st.session_state.last_replay_date:
    st.session_state.current_replay_index = df.index.get_loc(pd.Timestamp(replay_date))
    st.session_state.last_replay_date = replay_date

show_summary = st.sidebar.checkbox("Show Summary")
show_chart = st.sidebar.checkbox("Show Chart", value=True)
show_data = st.sidebar.checkbox("Show Data")

# Move Add and Clear indicator widgets to sidebar
st.sidebar.subheader("Indicator Management")
with st.sidebar.expander("Add Indicator"):
    selected_indicator = st.selectbox("Indicators", options=["EMA", "SMA", "WMA", "BBANDS", "MACD", "RSI", "LINREG"])

    # Dynamic parameter inputs based on selected indicator
    params = {}
    if selected_indicator in ['EMA', 'SMA', 'WMA', 'RSI', 'LINREG']:
        params['period'] = st.number_input(f"{selected_indicator} Period", value=14, min_value=2, max_value=100)
    elif selected_indicator == 'BBANDS':
        params['period'] = st.number_input("Bollinger Bands Period", value=20, min_value=2, max_value=100)
        params['std'] = st.number_input("Standard Deviation", value=2.0, min_value=0.1, max_value=5.0, step=0.1)
    elif selected_indicator == 'MACD':
        params['fast'] = st.number_input("Fast Period", value=12, min_value=2, max_value=100)
        params['slow'] = st.number_input("Slow Period", value=26, min_value=2, max_value=100)
        params['signal'] = st.number_input("Signal Period", value=9, min_value=2, max_value=100)

    params['color_code'] = st.color_picker("Choose Color", value='#2596be')
    submitted = st.button("Add Indicator")

    if submitted:
        st.session_state.indicators[selected_indicator] = params

with st.sidebar.expander("Clear Indicators"):
    indicators_to_clear = st.multiselect("Indicators to Clear",
                                         options=list(st.session_state.indicators.keys()) + ["All"])
    clear_submitted = st.button("Clear Indicator(s)")

    if clear_submitted:
        if "All" in indicators_to_clear:
            st.session_state.indicators.clear()
        else:
            for ind in indicators_to_clear:
                st.session_state.indicators.pop(ind, None)

if show_summary and len(df) > 20:
    summary_data = calculate_stock_summary(df.iloc[:st.session_state.current_replay_index + 1])
    latest_data = summary_data.iloc[-1]

    col1, col2, col3 = st.columns(3)
    with col1:
        st.subheader("Returns")
        for period in [20, 60, 120, 240]:
            if len(summary_data) > period:
                ret_percent = (latest_data['Close'] - summary_data.iloc[-period]['Close']) / summary_data.iloc[-period][
                    'Close'] * 100
                st.markdown(
                    f"- {period // 20} {'MONTH' if period // 20 == 1 else 'MONTHS'}: {ret_percent:.2f}% {get_emoji(ret_percent, lambda x: x > 0)}")

    with col2:
        st.subheader("Momentum")
        st.markdown(f"- LTP: {latest_data['Close']:.2f}")
        for ema in ['EMA_20', 'EMA_200']:
            if ema in latest_data:
                st.markdown(
                    f"- {ema}: {latest_data[ema]:.2f} {get_emoji(latest_data['Close'], lambda x: x > latest_data[ema])}")
        st.markdown(f"- RSI: {latest_data['RSI_14']:.2f} {get_emoji(latest_data['RSI_14'], lambda x: 30 < x < 70)}")

    with col3:
        st.subheader("Trend Strength")
        st.markdown(f"- ADX: {latest_data['ADX_14']:.2f} {get_emoji(latest_data['ADX_14'], lambda x: x > 25)}")
        st.markdown(f"- DMP: {latest_data['DMP_14']:.2f}")
        st.markdown(f"- DMN: {latest_data['DMN_14']:.2f}")

if show_chart:
    dark_theme = theme_col.checkbox("Dark Theme", value=True)
    apply_patterns = pattern_col.checkbox("Apply Patterns")

    # Calculate total height based on number of subcharts
    num_subcharts = len([ind for ind in st.session_state.indicators if ind in ['RSI', 'MACD']])
    total_height = 620 + (num_subcharts * 30)  # Increase height for each subchart

    # Create a placeholder for the entire chart area
    chart_placeholder = st.empty()

    # Function to update chart data
    def update_chart_data():
        replay_data = df.iloc[:st.session_state.current_replay_index + 1].copy()
        replay_data = calculate_stock_technical_summary(replay_data, st.session_state.indicators)
        # print(replay_data.tail())

        with chart_placeholder.container():
            chart_obj = StreamlitChart(width=1300, height=total_height, toolbox=True)

            if apply_patterns:
                support, resistance = calculate_support_resistance(replay_data)
                for level in support:
                    chart_obj.horizontal_line(level, color='green')
                for level in resistance:
                    chart_obj.horizontal_line(level, color='red')

            create_tv_chart(chart_obj, replay_data, theme='dark' if dark_theme else 'default')

    with replay_button:
        # Add replay controls
        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            if st.button("⏪ Previous Day"):
                st.session_state.current_replay_index = max(0, st.session_state.current_replay_index - 1)
                update_chart_data()

        with col2:
            if st.button("⏩ Next Day"):
                st.session_state.current_replay_index = min(len(df) - 1, st.session_state.current_replay_index + 1)
                update_chart_data()

        with col3:
            if st.button("▶️ Play/Pause"):
                st.session_state.is_playing = not st.session_state.is_playing

    # Play functionality
    if st.session_state.is_playing:
        for i in range(st.session_state.current_replay_index, len(df)):
            if not st.session_state.is_playing:
                break
            st.session_state.current_replay_index = i
            update_chart_data()
            time.sleep(0.5)  # Adjust speed as needed
        st.session_state.is_playing = False

    # Ensure the chart is updated initially and after any changes
    update_chart_data()
    header_col.subheader(f":rainbow[{stock_name}]")
    # header_col.subheader(":rainbow[Stock :red[Analysis] Dashboard!]")

if show_data:
    st.dataframe(df.iloc[:st.session_state.current_replay_index + 1].sort_index(ascending=False))