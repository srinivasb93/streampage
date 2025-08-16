import streamlit as st
import pandas as pd
import pandas_ta as ta
import numpy as np
from scipy.signal import argrelextrema
from lightweight_charts.widgets import StreamlitChart
from common_utils import read_write_sql_data as rd
import time
import seaborn as sns
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os
import threading
import upstox_client
import statsmodels.api as sm
from common_utils import upstox_utils
from common_utils.utils import fetch_indicies_sectors_list
import datetime as dt

# Load environment variables (for Upstox integration)
load_dotenv()
UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")

st.set_page_config(layout="wide")

# Upstox API Initialization
def init_upstox_api():
    config = upstox_client.Configuration()
    config.access_token = st.session_state.get("access_token", UPSTOX_ACCESS_TOKEN)
    api_client = upstox_client.ApiClient(config)
    return {
        "order": upstox_client.OrderApi(api_client),
        "portfolio": upstox_client.PortfolioApi(api_client),
        "history": upstox_client.HistoryApi(api_client)
    }


# Initialize upstox API
apis = init_upstox_api()

# Custom CSS to reduce whitespace and improve layout
st.markdown("""
    <style>
    .main { padding: 10px; }
    .stButton>button { background-color: #4C76A5; color: white; border-radius: 5px; margin: 2px; }
    .stCheckbox { margin: 5px 0; }
    .block-container { padding-top: 2.6rem; padding-bottom: 1rem; padding-left: 1rem; padding-right: 1rem;}
    .chart-container { margin-top: 10px; }
    </style>
""", unsafe_allow_html=True)

# Initialize session state
if "indicators" not in st.session_state:
    st.session_state.indicators = {}
if "current_replay_index" not in st.session_state:
    st.session_state.current_replay_index = -1
if "last_replay_date" not in st.session_state:
    st.session_state.last_replay_date = None
if "is_playing" not in st.session_state:
    st.session_state.is_playing = False
if "live_data" not in st.session_state:
    st.session_state.live_data = {"ltp": 0}


# Data Extraction
@st.cache_data
def extract_stock_data(stock_name, data_source='SQL', period_sql='Daily', period_upstox='days'):
    df = pd.DataFrame()
    if data_source == 'SQL':
        periods = {"Weekly": "_W", "Monthly": "_M", "Quarterly": "_Q", "Yearly": "_Y"}
        stock_name += periods.get(period_sql, "")
        query = f'select * from public."{stock_name}" order by timestamp ASC'
        df = rd.get_table_data(query=query)
        if period_sql == 'Daily':
            try:
                df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
            except:
                pass
        df.set_index("timestamp", inplace=True)
    elif data_source == 'Upstox':
        instruments_df = rd.get_table_data(selected_table='instruments', selected_database='trading_db')
        instrument_token = instruments_df[instruments_df['trading_symbol'] == stock_name]['instrument_token'].iloc[0]

        to_date = pd.Timestamp.now().strftime('%Y-%m-%d')
        response = upstox_utils.get_historical_data(instrument_token, period_upstox, end_date=to_date)
        df = pd.DataFrame(response.data.candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
        # df["Date"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)
        # df.sort_values(by='Date', inplace=True)
        # df.set_index('Date', inplace=True)
        # df = df[['open', 'high', 'low', 'close', 'volume']]
        # df.columns = ['open', 'high', 'low', 'close', 'volume']

    df["Price_Chg"] = round(df["close"].pct_change() * 100, 1)
    return df


def get_stocks_index_data(data_type='Stock'):
    """ Method to get stocks and indices list from SQL database """
    if data_type == 'Stock':
        stocks_in_db = rd.get_table_data(selected_table='STOCKS_IN_DB')
        my_holdings = rd.get_table_data(selected_database='analytics', selected_table='EQUITY_HOLDINGS')
        stocks = list(set(stocks_in_db['SYMBOL'].values.tolist() + my_holdings['Stock_Symbol'].values.tolist()))
    else:
        indices = fetch_indicies_sectors_list(required='indices')
        sectors = fetch_indicies_sectors_list(required='sectors')
        all_symbols = indices + sectors
    return stocks if data_type == 'Stock' else all_symbols


# Technical Indicators and Calculations
def calculate_monthly_returns(data):
    data['Monthly Return'] = data['close'].pct_change().dropna()
    data['Year'] = data.index.year
    data['Month'] = data.index.month
    return data.pivot_table(index='Year', columns='Month', values='Monthly Return')


def slope(ser, n):
    x = np.array(range(len(ser)))
    slopes = [0] * (n - 1)
    reg_prices = [0] * (n - 1)
    for i in range(n, len(ser) + 1):
        y_scaled = ser[i - n:i]
        x_scaled = sm.add_constant(x[i - n:i])
        model = sm.OLS(y_scaled, x_scaled)
        results = model.fit()
        slopes.append(results.params[-1])
        reg_prices.append(model.predict(results.params)[-1])
    return reg_prices

def calculate_stock_technical_summary(stock_df, indicators):
    for indicator, instances in indicators.items():
        for params in instances:
            if indicator == 'EMA':
                col_name = f'EMA_{params["period"]}'
                stock_df[col_name] = ta.ema(stock_df["close"], length=params['period'])
            elif indicator == 'SMA':
                col_name = f'SMA_{params["period"]}'
                stock_df[col_name] = ta.sma(stock_df["close"], length=params['period'])
            elif indicator == 'WMA':
                col_name = f'WMA_{params["period"]}'
                stock_df[col_name] = ta.wma(stock_df["close"], length=params['period'])
            elif indicator == 'BBANDS':
                bbands = ta.bbands(stock_df["close"], length=params['period'], std=params['std'])
                stock_df = pd.concat([stock_df, bbands], axis=1)
            elif indicator == 'MACD':
                macd = ta.macd(stock_df["close"], fast=params['fast'], slow=params['slow'], signal=params['signal'])
                stock_df = pd.concat([stock_df, macd], axis=1)
            elif indicator == 'RSI':
                col_name = f'RSI_{params["period"]}'
                stock_df[col_name] = ta.rsi(stock_df["close"], length=params['period'])
            elif indicator == 'LINREG':
                col_name = f'LINREG_{params["period"]}'
                stock_df[col_name] = slope(stock_df["close"], n=params['period'])
    return stock_df


def calculate_stock_summary(stock_df):
    stock_df = stock_df.copy()
    if len(stock_df) > 20:
        stock_df.loc[:, 'EMA_20'] = ta.ema(stock_df["close"], length=20)
    if len(stock_df) > 50:
        stock_df.loc[:, 'EMA_50'] = ta.ema(stock_df["close"], length=50)  # Added for crossover
    if len(stock_df) > 200:
        stock_df.loc[:, 'EMA_200'] = ta.ema(stock_df["close"], length=200)
    if len(stock_df) > 14:
        stock_df.loc[:, 'RSI_14'] = ta.rsi(stock_df["close"], length=14)
        adx = ta.adx(stock_df["high"], stock_df["low"], stock_df["close"], length=14)
        stock_df = pd.concat([stock_df, adx], axis=1)
        stock_df.loc[:, 'ATR_14'] = ta.atr(stock_df["high"], stock_df["low"], stock_df["close"], length=14)
        # Calculate ADX slope for trend direction
        stock_df['ADX_Slope'] = adx['ADX_14'].diff()
    return stock_df


def get_emoji(value, condition):
    return ":white_check_mark:" if condition(value) else ":red_circle:"


def calculate_support_resistance(stock_name, data, window=12, data_src='SQL'):
    support_data = {}
    resistance_data = {}
    if data_src == 'SQL':
        monthly_data = extract_stock_data(stock_name, data_source=data_src, period_sql="Monthly")
        weekly_data = extract_stock_data(stock_name, data_source=data_src, period_sql="Weekly")
    else:
        monthly_data = extract_stock_data(stock_name, data_source=data_src, period_yf="10y", interval_yf="1mo")
        weekly_data = extract_stock_data(stock_name, data_source=data_src, period_yf="1y", interval_yf="1wk")

    daily_data = data.tail(200).copy()
    weekly_data = weekly_data.tail(104)
    monthly_data = monthly_data.tail(36)
    for timeframe, df in [("Daily", daily_data), ("Weekly", weekly_data), ("Monthly", monthly_data)]:
        low, high = df['low'].values, df['high'].values
        low_idx = argrelextrema(low, np.less, order=window)[0]
        high_idx = argrelextrema(high, np.greater, order=window)[0]
        support_data[timeframe] = remove_close_levels(low[low_idx])
        resistance_data[timeframe] = remove_close_levels(high[high_idx])
    return support_data, resistance_data


def remove_close_levels(levels, threshold=0.02):
    levels = sorted(levels)
    return [level for i, level in enumerate(levels) if
            i == 0 or all(abs(level - r) / r > threshold for r in levels[:i])]


def create_tv_chart(chart, stock_name, data, theme='default'):
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

    for indicator, instances in st.session_state.indicators.items():
        for params in instances:
            if indicator in ['RSI', 'MACD']:
                add_indicator_line(subcharts[indicator], data, indicator, params)
            else:
                add_indicator_line(chart, data, indicator, params)

    chart.layout(background_color="#161616" if theme == 'dark' else "#FFFFFF",
                 text_color="#FFFFFF" if theme == 'dark' else "#000000")
    chart.grid(style='dashed', color='#D3D3D3' if theme != 'dark' else "#262616")
    chart.watermark(stock_name, font_size=22)
    chart.price_scale(auto_scale=True)
    chart.load()


def add_indicator_line(chart, df, indicator, params):
    color = params['color_code']
    if indicator == 'BBANDS':
        for col, col_color in [('BBU', 'red'), ('BBM', 'cyan'), ('BBL', 'green')]:
            line = chart.create_line(name=f'{col}_{params["period"]}', color=col_color, width=1.25, price_label=True,
                                     price_line=False)
            ind_df = pd.DataFrame(
                {'time': df.index, f'{col}_{params["period"]}': df[f'{col}_{params["period"]}_{params["std"]}']})
            line.set(ind_df.dropna())
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
        line = chart.create_line(name=f'RSI_{params["period"]}', color=color, width=1.5, price_line=False)
        ind_df = pd.DataFrame({'time': df.index, f'RSI_{params["period"]}': df[f'RSI_{params["period"]}']})
        line.set(ind_df)
    else:
        # For EMA, SMA, WMA, LINREG
        line_name = f'{indicator}_{params["period"]}'
        line = chart.create_line(name=line_name, color=color, width=1.5, price_label=True, price_line=False)
        ind_df = pd.DataFrame({'time': df.index, line_name: df[f'{indicator}_{params["period"]}']})
        line.set(ind_df.dropna())


def stock_analysis():
    # Streamlit UI
    header_col, replay_button, pattern_col, theme_col = st.columns([0.25, 0.43, 0.12, 0.1], gap='small')

    # Sidebar
    with st.sidebar:
        st.header("Data Selection")
        data_src = st.radio("Data Source", ["SQL", "Upstox"], horizontal=True)
        asset = st.radio("Asset Type", ["Stock", "Index"], horizontal=True)
        tables_list = sorted(get_stocks_index_data(asset))
        default_asset = "TATAMOTORS" if asset == 'Stock' else "NIFTY 50"
        stock_name = st.selectbox("Select Stock Symbol", tables_list, index=tables_list.index(default_asset))

        if data_src == 'SQL':
            timeframe_option = st.selectbox("Timeframe", ('Daily', 'Weekly', 'Monthly', 'Yearly'))
            df = extract_stock_data(stock_name.replace("-", "_").replace(" ", "_"), data_source=data_src, period_sql=timeframe_option)
        elif data_src == 'Upstox':
            timeframe_option = st.selectbox("Timeframe", ('1minute', 'day', 'week', 'month'))
            df = extract_stock_data(stock_name, data_source='Upstox', period_upstox=timeframe_option)

        data_replay = st.checkbox("Replay Data")

        if data_replay:
            replay_date = st.date_input("Replay Date", value=df.index[-1].date(), min_value=df.index[0].date(),
                                        max_value=df.index[-1].date())
            replay_speed = st.slider("Replay Speed (seconds)", 0.1, 2.0, 0.5)

        st.header("Indicators")
        with st.expander("Add Indicator"):
            selected_indicator = st.selectbox("Indicators",
                                              options=["EMA", "SMA", "WMA", "BBANDS", "MACD", "RSI", "LINREG"])
            params = {}
            if selected_indicator in ['EMA', 'SMA', 'WMA', 'RSI', 'LINREG']:
                params['period'] = st.number_input(f"{selected_indicator} Period", value=14, min_value=2, max_value=100)
            elif selected_indicator == 'BBANDS':
                params['period'] = st.number_input("BB Period", value=20, min_value=2, max_value=100)
                params['std'] = st.number_input("Std Dev", value=2.0, min_value=0.1, max_value=5.0, step=0.1)
            elif selected_indicator == 'MACD':
                params['fast'] = st.number_input("Fast", value=12, min_value=2, max_value=100)
                params['slow'] = st.number_input("Slow", value=26, min_value=2, max_value=100)
                params['signal'] = st.number_input("Signal", value=9, min_value=2, max_value=100)
            params['color_code'] = st.color_picker("Color", value='#2596be')
            if st.button("Add"):
                if selected_indicator not in st.session_state.indicators:
                    st.session_state.indicators[selected_indicator] = []
                st.session_state.indicators[selected_indicator].append(params)

        if st.button("Clear All Indicators"):
            st.session_state.indicators.clear()

        st.header("View Options")
        show_summary = st.checkbox("Show Summary")
        show_heatmap = st.checkbox("Show Heatmap")
        show_analysis = st.checkbox("Show Analysis Tools")
        show_data = st.checkbox("Show Raw Data")

    # Chart Section (Always Visible)
    dark_theme = theme_col.checkbox("Dark Theme", value=True)
    apply_patterns = pattern_col.checkbox("Apply Patterns")
    header_col.subheader(f":rainbow[{stock_name}]")

    num_subcharts = len([ind for ind in st.session_state.indicators if ind in ['RSI', 'MACD']])
    total_height = 700 + (num_subcharts * 100)
    chart_placeholder = st.empty()

    def update_chart_data():
        replay_data = df.iloc[:st.session_state.current_replay_index + 1].copy()
        replay_data = calculate_stock_technical_summary(replay_data, st.session_state.indicators)
        with chart_placeholder.container():
            chart_obj = StreamlitChart(height=total_height, toolbox=True, scale_candles_only=True)
            if apply_patterns:
                support_data, resistance_data = calculate_support_resistance(
                    stock_name, replay_data, window=12, data_src=data_src)
                sup_colour_codes = {'Daily': '#0eed59', 'Weekly': '#42f59b', 'Monthly': '#1df0e2'}
                res_colour_codes = {'Daily': '#eb6e34', 'Weekly': '#f2a705', 'Monthly': '#d11b06'}
                for sup_type, sup_list in support_data.items():
                    for sup_level in sup_list:
                        chart_obj.horizontal_line(sup_level, color=sup_colour_codes.get(sup_type), width=1)
                for res_type, res_list in resistance_data.items():
                    for res_level in res_list:
                        chart_obj.horizontal_line(res_level, color=res_colour_codes.get(res_type), width=1)
            create_tv_chart(chart_obj, stock_name, replay_data, theme='dark' if dark_theme else 'default')


    with replay_button:
        col1, col2, col3 = st.columns([1, 1, 1], gap='small')
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

    if data_replay:
        if st.session_state.current_replay_index == -1 or replay_date != st.session_state.last_replay_date:
            st.session_state.current_replay_index = df.index.get_loc(pd.Timestamp(replay_date - dt.timedelta(days=1)))
            st.session_state.last_replay_date = replay_date

    if st.session_state.is_playing:
        for i in range(st.session_state.current_replay_index, len(df)):
            if not st.session_state.is_playing:
                break
            st.session_state.current_replay_index = i
            update_chart_data()
            time.sleep(replay_speed)
        st.session_state.is_playing = False

    update_chart_data()

    if data_src == 'Upstox':
        st.metric("Live LTP (Upstox)", f"₹{st.session_state.live_data['ltp']:.2f}")

    # Tabs for Other Sections (Conditional)
    if show_summary or show_heatmap or show_analysis or show_data:
        tabs = st.tabs([tab for tab, show in
                        [("Summary", show_summary), ("Heatmap", show_heatmap), ("Analysis Tools", show_analysis),
                         ("Raw Data", show_data)] if show])

        tab_index = 0
        if show_summary:
            with tabs[tab_index]:
                st.header("Stock Summary")
                summary_data = calculate_stock_summary(df.iloc[:st.session_state.current_replay_index + 1])
                latest_data = summary_data.iloc[-1]
                prev_data = summary_data.iloc[-2] if len(summary_data) > 1 else latest_data

                # Get support/resistance levels
                support_data, resistance_data = calculate_support_resistance(stock_name, summary_data)
                latest_close = latest_data['close']
                nearest_support = min([min(levels, default=latest_close) for levels in support_data.values() if levels],
                                      default=latest_close)
                nearest_resistance = min(
                    [min(levels, default=latest_close) for levels in resistance_data.values() if levels],
                    default=latest_close)

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.subheader("Momentum")
                    st.markdown(f"- LTP: {latest_close:.2f}")
                    if 'EMA_20' in latest_data and 'EMA_50' in latest_data:
                        ema_cross = "Bullish" if (
                                prev_data['EMA_20'] < prev_data['EMA_50'] and
                                latest_data['EMA_20'] > latest_data['EMA_50']) else \
                            "Bearish" if (prev_data['EMA_20'] > prev_data['EMA_50'] and latest_data['EMA_20'] < latest_data[
                                'EMA_50']) else "Neutral"
                        st.markdown(f"- EMA 20/50 Crossover: {ema_cross}")
                    for ema in ['EMA_20', 'EMA_200']:
                        if ema in latest_data:
                            st.markdown(
                                f"- {ema}: {latest_data[ema]:.2f} {get_emoji(latest_close, lambda x: x > latest_data[ema])}")
                    if 'RSI_14' in latest_data:
                        rsi_signal = "Overbought" if latest_data['RSI_14'] > 70 else "Oversold" if latest_data[
                                                                                                       'RSI_14'] < 30 else "Neutral"
                        st.markdown(f"- RSI: {latest_data['RSI_14']:.2f} ({rsi_signal})")

                with col2:
                    st.subheader("Trend & Volatility")
                    if 'ADX_14' in latest_data:
                        trend_dir = "Up" if latest_data['ADX_Slope'] > 0 else "Down" if latest_data[
                                                                                            'ADX_Slope'] < 0 else "Flat"
                        st.markdown(
                            f"- ADX: {latest_data['ADX_14']:.2f} (Trend: {trend_dir}) {get_emoji(latest_data['ADX_14'], lambda x: x > 25)}")
                        st.markdown(f"- DMP: {latest_data['DMP_14']:.2f}")
                        st.markdown(f"- DMN: {latest_data['DMN_14']:.2f}")
                    if 'ATR_14' in latest_data:
                        st.markdown(f"- ATR: {latest_data['ATR_14']:.2f} (Volatility)")
                    range_14 = summary_data['close'].tail(14).max() - summary_data['close'].tail(14).min()
                    st.markdown(f"- 14-Day Range: {range_14:.2f}")

                with col3:
                    st.subheader("Key Levels & Outlook")
                    st.markdown(
                        f"- Nearest Support: {nearest_support:.2f} ({(latest_close - nearest_support) / latest_close * 100:.1f}%)")
                    st.markdown(
                        f"- Nearest Resistance: {nearest_resistance:.2f} ({(nearest_resistance - latest_close) / latest_close * 100:.1f}%)")

                    # Refined Trading Outlook
                    signals = []
                    outlook_score = 0  # Positive for bullish, negative for bearish
                    if 'EMA_20' in latest_data:
                        if latest_close > latest_data['EMA_20']:
                            signals.append("Above EMA_20")
                            outlook_score += 1
                        else:
                            signals.append("Below EMA_20")
                            outlook_score -= 1
                    if 'EMA_20' in latest_data and 'EMA_50' in latest_data:
                        if ema_cross == "Bullish":
                            signals.append("EMA Cross Bullish")
                            outlook_score += 2
                        elif ema_cross == "Bearish":
                            signals.append("EMA Cross Bearish")
                            outlook_score -= 2
                    if 'RSI_14' in latest_data:
                        if latest_data['RSI_14'] > 70:
                            signals.append("Overbought")
                            outlook_score -= 1
                        elif latest_data['RSI_14'] < 30:
                            signals.append("Oversold")
                            outlook_score += 1
                    if 'ADX_14' in latest_data and latest_data['ADX_14'] > 25:
                        if latest_data['ADX_Slope'] > 0:
                            signals.append("Strong Uptrend")
                            outlook_score += 3
                        elif latest_data['ADX_Slope'] < 0:
                            signals.append("Strong Downtrend")
                            outlook_score -= 3
                    if abs(latest_close - nearest_support) / latest_close < 0.02:
                        signals.append("Near Support")
                        outlook_score += 1
                    if abs(latest_close - nearest_resistance) / latest_close < 0.02:
                        signals.append("Near Resistance")
                        outlook_score -= 1

                    outlook = "Bullish" if outlook_score > 2 else "Bearish" if outlook_score < 0 else "Neutral"
                    st.markdown(f"- **Trading Outlook**: {outlook} (Score: {outlook_score})")
                    st.markdown(f"- Signals: {', '.join(signals) if signals else 'None'}")
                tab_index += 1

        if show_heatmap:
            with tabs[tab_index]:
                st.header("Monthly Returns Heatmap")
                if st.button("Generate Heatmap"):
                    monthly_df = extract_stock_data(stock_name, data_source=data_src,
                                                    period_sql="Monthly")
                    monthly_returns = calculate_monthly_returns(monthly_df)
                    plt.figure(figsize=(10, 5))
                    sns.heatmap(monthly_returns, annot=True, fmt=".1%", cmap='RdYlGn', center=0)
                    plt.title(f'Monthly Returns Heatmap for {stock_name}')
                    st.pyplot(plt)
            tab_index += 1

        if show_analysis:
            with tabs[tab_index]:
                analysis_cols = st.columns(4)
                with analysis_cols[0]:
                    st.subheader("Trade Setup Summary")
                    summary_data = calculate_stock_summary(df.iloc[:st.session_state.current_replay_index + 1])
                    latest_data = summary_data.iloc[-1]
                    support_data, resistance_data = calculate_support_resistance(stock_name, summary_data)
                    nearest_support = min(
                        [min(levels, default=latest_data['close']) for levels in support_data.values() if levels],
                        default=latest_data['close'])
                    nearest_resistance = min(
                        [min(levels, default=latest_data['close']) for levels in resistance_data.values() if levels],
                        default=latest_data['close'])
                    atr = latest_data['ATR_14'] if 'ATR_14' in latest_data else 0

                    # Determine direction and setup
                    direction = "Long" if latest_data['close'] > latest_data.get('EMA_20',
                                                                                 latest_data['close']) and latest_data.get(
                        'ADX_Slope', 0) > 0 else \
                        "Short" if latest_data['close'] < latest_data.get('EMA_20',
                                                                          latest_data['close']) and latest_data.get(
                            'ADX_Slope', 0) < 0 else "Hold"
                    entry = latest_data['close']
                    stop = nearest_support - atr if direction == "Long" else nearest_resistance + atr
                    target = nearest_resistance if direction == "Long" else nearest_support
                    rr_ratio = abs(target - entry) / abs(entry - stop) if abs(entry - stop) > 0 else 0

                    st.markdown(f"- **Direction**: {direction}")
                    st.markdown(f"- Entry: ₹{entry:.2f}")
                    st.markdown(f"- Stop Loss: ₹{stop:.2f}")
                    st.markdown(f"- Target: ₹{target:.2f}")
                    st.markdown(f"- Risk-Reward Ratio: {rr_ratio:.2f}:1")
                    if direction != "Hold":
                        st.markdown(
                            f"- **Action**: {'Buy' if direction == 'Long' else 'Sell'} near {entry:.2f}, stop at {stop:.2f}, target {target:.2f}")
                    else:
                        st.markdown("- **Action**: Wait for clearer signal")

                st.subheader("Benchmark Comparison")
                benchmark = st.selectbox("Benchmark", ["NIFTY_50", "SENSEX"])
                bench_df = extract_stock_data(benchmark, data_source='SQL')
                # Ensure both DataFrames share a common index
                common_index = df.index.intersection(bench_df.index)
                if not common_index.empty:
                    stock_data = df["close"].reindex(common_index).ffill()
                    bench_data = bench_df["close"].reindex(common_index).ffill()
                    combined = pd.DataFrame({
                        "Stock": stock_data,
                        "Benchmark": bench_data
                    }).pct_change().cumsum().dropna()
                    if not combined.empty:
                        st.line_chart(combined, use_container_width=True)
                    else:
                        st.error("No overlapping data available for comparison after processing.")
                else:
                    st.error("No overlapping dates found between stock and benchmark data.")

                with analysis_cols[1]:
                    st.subheader("Breakout Detection")
                    breakout_window = st.number_input("Breakout Lookback (days)", value=20, min_value=5)
                    recent_data = df.iloc[-breakout_window:].copy()
                    support_data, resistance_data = calculate_support_resistance(stock_name, recent_data)
                    latest_close = recent_data["close"].iloc[-1]
                    for timeframe in ["Daily", "Weekly", "Monthly"]:
                        supports = support_data[timeframe]
                        resistances = resistance_data[timeframe]
                        for level in supports:
                            if abs(latest_close - level) / level < 0.01 and latest_close > level:
                                st.markdown(f"- **Bullish Breakout** above {timeframe} Support: {level:.2f}")
                        for level in resistances:
                            if abs(latest_close - level) / level < 0.01 and latest_close < level:
                                st.markdown(f"- **Bearish Breakout** below {timeframe} Resistance: {level:.2f}")
                    else:
                        st.markdown(f"No Breakouts detected")
                    if 'BBU_20_2.0' in recent_data and latest_close > recent_data['BBU_20_2.0'].iloc[-1]:
                        st.markdown(
                            f"- **Bullish BB Breakout**: Above Upper Band ({recent_data['BBU_20_2.0'].iloc[-1]:.2f})")

                with analysis_cols[2]:
                    st.subheader("Momentum Divergence")
                    div_period = st.number_input("Divergence Period", value=14, min_value=5)
                    recent_data = df.iloc[-div_period * 2:].copy()
                    recent_data['RSI_14'] = ta.rsi(recent_data["close"], length=14)
                    price_highs = argrelextrema(recent_data["close"].values, np.greater, order=5)[0]
                    price_lows = argrelextrema(recent_data["close"].values, np.less, order=5)[0]
                    rsi_highs = argrelextrema(recent_data["RSI_14"].values, np.greater, order=5)[0]
                    rsi_lows = argrelextrema(recent_data["RSI_14"].values, np.less, order=5)[0]
                    if len(price_highs) > 1 and len(rsi_highs) > 1:
                        if recent_data["close"].iloc[price_highs[-1]] > recent_data["close"].iloc[price_highs[-2]] and \
                                recent_data["RSI_14"].iloc[rsi_highs[-1]] < recent_data["RSI_14"].iloc[rsi_highs[-2]]:
                            st.markdown("- **Bearish RSI Divergence**: higher price, lower RSI")
                    if len(price_lows) > 1 and len(rsi_lows) > 1:
                        if recent_data["close"].iloc[price_lows[-1]] < recent_data["close"].iloc[price_lows[-2]] and \
                                recent_data["RSI_14"].iloc[rsi_lows[-1]] > recent_data["RSI_14"].iloc[rsi_lows[-2]]:
                            st.markdown("- **Bullish RSI Divergence**: lower price, higher RSI")

                st.subheader("Position Sizing")
                account_size = st.number_input("Account Size (₹)", value=20000.0, min_value=1000.0)
                risk_percent = st.slider("Risk % per Trade", 0.1, 5.0, 2.0)
                entry_price = st.number_input("Entry Price", value=df["close"].iloc[-1]*.995)
                stop_loss = st.number_input("Stop Loss", value=df["close"].iloc[-1] * 0.98)
                if st.button("Calculate Position Size"):
                    atr = ta.atr(df["high"], df["low"], df["close"], length=14).iloc[-1]
                    risk_amount = account_size * (risk_percent / 100)
                    price_risk = abs(entry_price - stop_loss)
                    atr_risk = atr * 2  # 2x ATR as alternative stop
                    size_price = int(risk_amount / price_risk) if price_risk > 0 else 0
                    size_atr = int(risk_amount / atr_risk) if atr_risk > 0 else 0
                    st.markdown(f"- Risk Amount: ₹{risk_amount:.2f}")
                    st.markdown(f"- Position Size (Price-based): {size_price} shares")
                    st.markdown(f"- Position Size (2x ATR): {size_atr} shares")

                with analysis_cols[3]:
                    st.subheader("Candlestick Patterns")
                    pattern_window = st.number_input("Pattern Lookback (days)", value=5, min_value=2)
                    recent_data = df.iloc[-pattern_window:].copy()
                    # Doji: Small body relative to range
                    doji = recent_data.apply(
                        lambda row: abs(row['close'] - row['open']) / (row['high'] - row['low']) < 0.1
                        if row['high'] != row['low'] else False,
                        axis=1)
                    if doji.iloc[-1]:
                        st.markdown(f"- **Doji** detected on {recent_data.index[-1].date()}: Potential reversal")
                    # Bullish Engulfing
                    if len(recent_data) > 1:
                        prev = recent_data.iloc[-2]
                        latest = recent_data.iloc[-1]
                        if latest['open'] < prev['close'] < prev['open'] < latest['close'] and latest['close'] > latest[
                            'open']:
                            st.markdown(f"- **Bullish Engulfing** on {latest.name.date()}: Potential bullish reversal")
                        # Bearish Engulfing
                        if latest['open'] > prev['close'] > prev['open'] > latest['close'] and latest['close'] < latest[
                            'open']:
                            st.markdown(f"- **Bearish Engulfing** on {latest.name.date()}: Potential bearish reversal")

                tab_index += 1

        if show_data:
            with tabs[tab_index]:
                st.header("Raw Data")
                st.dataframe(df.iloc[:st.session_state.current_replay_index + 1].sort_index(ascending=False))

if __name__ == "__main__":
    stock_analysis()