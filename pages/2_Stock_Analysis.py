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
import upstox_client
import statsmodels.api as sm
from common_utils import upstox_utils
from common_utils.utils import fetch_indicies_sectors_list
import datetime as dt
from common_utils.auth import require_authentication
from python_scripts.analysis.EOD_analysis import EODAnalysis

# Load environment variables (for Upstox integration)
load_dotenv()
UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")

st.set_page_config(layout="wide")

# Require authentication for this page
require_authentication()

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


if "additional_chart_timeframes" not in st.session_state:
    st.session_state.additional_chart_timeframes = []

if "eod_analysis_results" not in st.session_state:
    st.session_state.eod_analysis_results = {}

if "eod_analysis_status" not in st.session_state:
    st.session_state.eod_analysis_status = {}


SQL_TIMEFRAME_OPTIONS = [
    ("Daily", "Daily"),
    ("Weekly", "Weekly"),
    ("Monthly", "Monthly"),
    ("Yearly", "Yearly"),
]

UPSTOX_TIMEFRAME_OPTIONS = [
    ("Minutes", "minutes"),
    ("Hours", "hours"),
    ("Daily", "days"),
    ("Weekly", "weeks"),
    ("Monthly", "months"),
]

QUICK_INDICATOR_PRESETS = [
    {"label": "EMA 20", "indicator": "EMA", "period": 20, "color": "#F39C12"},
    {"label": "EMA 60", "indicator": "EMA", "period": 60, "color": "#1F77B4"},
    {"label": "EMA 200", "indicator": "EMA", "period": 200, "color": "#8E44AD"},
    {"label": "LINREG 5", "indicator": "LINREG", "period": 5, "color": "#27AE60"},
    {"label": "RSI 14", "indicator": "RSI", "period": 14, "color": "#E74C3C"},
]


def get_timeframe_options(source):
    return SQL_TIMEFRAME_OPTIONS if source == 'SQL' else UPSTOX_TIMEFRAME_OPTIONS


def get_timeframe_value(label, options):
    for display, value in options:
        if display == label:
            return value
    return options[0][1]


def sanitize_sql_symbol(symbol):
    return symbol.replace('-', '_').replace(' ', '_')

EMPTY_OHLC_COLUMNS = ['open', 'high', 'low', 'close', 'volume', 'Price_Chg']


def build_eod_signal_summary(latest_row: pd.Series) -> list[str]:
    """Create a list of human-friendly highlights from the latest EOD analysis row."""
    summary_lines: list[str] = []

    def cleaned(field_name):
        value = latest_row.get(field_name)
        if isinstance(value, str):
            value = value.strip()
        if value in (None, '', 'nan'):
            return None
        if isinstance(value, float) and pd.isna(value):
            return None
        return value

    stop_loss_hunt = cleaned('Stop_Loss_Hunt')
    if stop_loss_hunt:
        summary_lines.append(f"Stop-loss hunt pattern detected ({stop_loss_hunt}).")

    divergence = cleaned('RSI_Divergence')
    if divergence:
        summary_lines.append(f"RSI divergence signal: {divergence}.")

    reversal = cleaned('Reversal_Signals')
    if reversal:
        summary_lines.append(f"Reversal cues at key levels: {reversal}.")

    failed_breakout = cleaned('Failed_Breakout_Signals')
    if failed_breakout:
        summary_lines.append(f"Failed breakout flags: {failed_breakout}.")

    breakout_20 = cleaned('Breakout_20')
    if breakout_20:
        summary_lines.append(f"20-day breakout status: {breakout_20}.")

    sup_strength = cleaned('Support_Strength_Label')
    if sup_strength:
        count = cleaned('Support_Strength_Count')
        count_text = f" (touches: {int(count)})" if isinstance(count, (int, float)) and not pd.isna(count) else ""
        summary_lines.append(f"Support strength: {sup_strength}{count_text}.")

    res_strength = cleaned('Resistance_Strength_Label')
    if res_strength:
        count = cleaned('Resistance_Strength_Count')
        count_text = f" (touches: {int(count)})" if isinstance(count, (int, float)) and not pd.isna(count) else ""
        summary_lines.append(f"Resistance strength: {res_strength}{count_text}.")

    support_gap_pct = cleaned('Support_Gap_Pct')
    if isinstance(support_gap_pct, (int, float)) and abs(support_gap_pct) >= 0.01:
        summary_lines.append(f"Current support is {support_gap_pct:.2f}% away from the previous support.")

    resistance_gap_pct = cleaned('Resistance_Gap_Pct')
    if isinstance(resistance_gap_pct, (int, float)) and abs(resistance_gap_pct) >= 0.01:
        summary_lines.append(f"Current resistance is {resistance_gap_pct:.2f}% away from the previous resistance.")

    break_sup_res = cleaned('Break_Sup_Res')
    if break_sup_res:
        summary_lines.append(f"Support/resistance break status: {break_sup_res}.")

    return summary_lines


def load_openchart_stock_data(symbol, unit, interval='1'):
    interval_data = upstox_utils.get_openchart_history(symbol, unit=unit, interval=interval)
    if not isinstance(interval_data, pd.DataFrame) or interval_data.empty:
        return pd.DataFrame()
    interval_data = interval_data.copy()
    interval_data['timestamp'] = pd.to_datetime(interval_data['timestamp'])
    interval_data.sort_values('timestamp', inplace=True)
    return interval_data[['timestamp', 'open', 'high', 'low', 'close', 'volume']]


# Data Extraction
def extract_stock_data(stock_name, data_source='SQL', period='Daily', interval='1', asset_type='stock'):
    df = pd.DataFrame()

    if data_source == 'SQL':
        if (period in ["Daily", "Weekly", "Monthly", "Yearly"] and asset_type == 'stock') or (period == 'Daily' and asset_type == 'index'):
            periods = {"Weekly": "_W", "Monthly": "_M", "Quarterly": "_Q", "Yearly": "_Y"}
            table_suffix = periods.get(period, "")
            table_name = f"{sanitize_sql_symbol(stock_name)}{table_suffix}"
            query = f'select * from public."{table_name}" order by timestamp ASC'
            try:
                df = rd.get_table_data(query=query)
            except Exception:
                df = pd.DataFrame()
        else:
            period_map = {"minutes": "Minutes", "hours": "Hours", "days": "Daily", "weeks": "Weekly", "months": "Monthly"}
            period = period_map.get(period, "Daily")
            df = load_openchart_stock_data(stock_name, period, interval)

        if isinstance(df, pd.DataFrame) and not df.empty and 'timestamp' in df.columns:
            df = df.copy()
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
        else:
            df = load_openchart_stock_data(stock_name, period, interval)

    elif data_source == 'Upstox':
        try:
            instruments_df = rd.get_table_data(selected_table='instruments', selected_database='trading_db')
        except Exception:
            instruments_df = pd.DataFrame()

        instrument_token = None
        if isinstance(instruments_df, pd.DataFrame) and not instruments_df.empty:
            match = instruments_df[instruments_df['trading_symbol'] == stock_name]
            if not match.empty:
                instrument_token = match['instrument_token'].iloc[0]

        if period.endswith('minutes'):
            from_date = (pd.Timestamp.now() - dt.timedelta(days=10)).strftime('%Y-%m-%d')
        elif period.endswith('hours'):
            from_date = (pd.Timestamp.now() - dt.timedelta(days=30)).strftime('%Y-%m-%d')
        else:
            from_date = (pd.Timestamp.now() - dt.timedelta(days=3650)).strftime('%Y-%m-%d')
        to_date = pd.Timestamp.now().strftime('%Y-%m-%d')

        if instrument_token:
            df = upstox_utils.get_historical_data(
                instrument_token,
                interval=period,
                unit=interval,
                start_date=from_date,
                end_date=to_date
            )
        if isinstance(df, pd.DataFrame) and not df.empty and 'timestamp' in df.columns:
            df = df.copy()
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
        else:
            df = load_openchart_stock_data(stock_name, period, interval)

    else:
        df = load_openchart_stock_data(stock_name, period, interval)

    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame(columns=EMPTY_OHLC_COLUMNS)

    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)

    df.sort_index(inplace=True)
    if 'volume' not in df.columns:
        df['volume'] = 0
    if 'close' in df.columns:
        df['Price_Chg'] = (df['close'].pct_change() * 100).round(1)
    else:
        df['Price_Chg'] = pd.NA
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

def _format_numeric_suffix(value):
    """Consistently format numeric suffixes like periods or std deviations."""
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return str(value)
    if numeric_value.is_integer():
        return f"{int(numeric_value)}.0"
    return str(numeric_value).rstrip('0').rstrip('.')


def calculate_stock_technical_summary(stock_df, indicators):
    indicator_items = [(indicator, list(instances)) for indicator, instances in list(indicators.items())]
    for indicator, instances in indicator_items:
        for params in instances:
            if indicator == 'EMA':
                period = int(params['period'])
                col_name = f'EMA_{period}'
                stock_df[col_name] = ta.ema(stock_df["close"], length=period)
            elif indicator == 'SMA':
                period = int(params['period'])
                col_name = f'SMA_{period}'
                stock_df[col_name] = ta.sma(stock_df["close"], length=period)
            elif indicator == 'WMA':
                period = int(params['period'])
                col_name = f'WMA_{period}'
                stock_df[col_name] = ta.wma(stock_df["close"], length=period)
            elif indicator == 'BBANDS':
                period = int(params['period'])
                std_value = params.get('std', 2.0)
                std_float = float(std_value)
                std_suffix = _format_numeric_suffix(std_float)
                bbands = ta.bbands(
                    stock_df["close"],
                    length=period,
                    lower_std=std_float,
                    upper_std=std_float
                )
                if bbands is not None:
                    rename_map = {}
                    for col in bbands.columns:
                        if col.startswith(('BBL', 'BBM', 'BBU', 'BBB', 'BBP')):
                            parts = col.split('_')
                            if len(parts) >= 2:
                                base = parts[0]
                                rename_map[col] = f'{base}_{period}_{std_suffix}'
                    if rename_map:
                        bbands = bbands.rename(columns=rename_map)
                    stock_df = pd.concat([stock_df, bbands], axis=1)
            elif indicator == 'MACD':
                fast = int(params['fast'])
                slow = int(params['slow'])
                signal = int(params['signal'])
                macd = ta.macd(stock_df["close"], fast=fast, slow=slow, signal=signal)
                stock_df = pd.concat([stock_df, macd], axis=1)
            elif indicator == 'RSI':
                period = int(params['period'])
                col_name = f'RSI_{period}'
                stock_df[col_name] = ta.rsi(stock_df["close"], length=period)
            elif indicator == 'LINREG':
                period = int(params['period'])
                col_name = f'LINREG_{period}'
                stock_df[col_name] = slope(stock_df["close"], n=period)
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
        monthly_data = extract_stock_data(stock_name, data_source=data_src, period="Monthly", interval='1')
        weekly_data = extract_stock_data(stock_name, data_source=data_src, period="Weekly", interval='1')
    else:
        monthly_data = extract_stock_data(stock_name, data_source=data_src, period='months', interval='1')
        weekly_data = extract_stock_data(stock_name, data_source=data_src, period='weeks', interval='1')

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
    indicator_snapshot = {indicator: list(instances) for indicator, instances in st.session_state.indicators.items()}
    chart.legend(True, font_size=20, color_based_on_candle=True, color="#1e81b0")
    chart.topbar.textbox('symbol', stock_name, align='center')
    chart.set(data, keep_drawings=True)

    # Create subcharts for RSI and MACD
    subcharts = {}
    for indicator in ['RSI', 'MACD']:
        if indicator in indicator_snapshot:
            # if both RSI and MACD are to be seen, then
            if 'RSI' in indicator_snapshot and 'MACD' in indicator_snapshot:
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

    for indicator, instances in indicator_snapshot.items():
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
        period = int(params['period'])
        std_suffix = _format_numeric_suffix(params.get('std', 2.0))
        for col, col_color in [('BBU', 'red'), ('BBM', 'cyan'), ('BBL', 'green')]:
            column_name = f'{col}_{period}_{std_suffix}'
            if column_name not in df.columns:
                continue
            line = chart.create_line(name=f'{col}_{period}', color=col_color, width=1.25, price_label=True,
                                     price_line=False)
            ind_df = pd.DataFrame({'time': df.index, f'{col}_{period}': df[column_name]})
            line.set(ind_df.dropna())
    elif indicator == 'MACD':
        fast = int(params['fast'])
        slow = int(params['slow'])
        signal = int(params['signal'])
        macd_line = chart.create_line(name='MACD', color=color, width=1.5, price_line=False)
        signal_line = chart.create_line(name='MACD_Signal', color='red', width=1.5, price_line=False)
        histogram = chart.create_histogram(name='MACD_Hist', color=color, price_line=False)

        macd_key = f'MACD_{fast}_{slow}_{signal}'
        macd_signal_key = f'MACDs_{fast}_{slow}_{signal}'
        macd_hist_key = f'MACDh_{fast}_{slow}_{signal}'

        if not all(key in df.columns for key in [macd_key, macd_signal_key, macd_hist_key]):
            return

        ind_df = pd.DataFrame({
            'time': df.index,
            'MACD': df[macd_key],
            'MACD_Signal': df[macd_signal_key],
            'MACD_Hist': df[macd_hist_key]
        })
        macd_line.set(ind_df[['time', 'MACD']])
        signal_line.set(ind_df[['time', 'MACD_Signal']])
        histogram.set(ind_df[['time', 'MACD_Hist']])
    elif indicator == 'RSI':
        period = int(params['period'])
        column_name = f'RSI_{period}'
        if column_name not in df.columns:
            return
        line = chart.create_line(name=column_name, color=color, width=1.5, price_line=False)
        ind_df = pd.DataFrame({'time': df.index, column_name: df[column_name]})
        line.set(ind_df)
    else:
        # For EMA, SMA, WMA, LINREG
        period = int(params['period'])
        line_name = f'{indicator}_{period}'
        if line_name not in df.columns:
            return
        line = chart.create_line(name=line_name, color=color, width=1.5, price_label=True, price_line=False)
        ind_df = pd.DataFrame({'time': df.index, line_name: df[line_name]})
        line.set(ind_df.dropna())


def stock_analysis():
    # Streamlit UI
    def indicator_exists(indicator_name: str, period: int) -> bool:
        return any(int(instance.get('period', 0)) == int(period) for instance in st.session_state.indicators.get(indicator_name, []))

    def quick_indicator_exists(indicator_name: str, period: int) -> bool:
        return any(int(instance.get('period', 0)) == int(period) and instance.get('source') == 'quick_toggle' for instance in st.session_state.indicators.get(indicator_name, []))

    def add_quick_indicator(indicator_name: str, period: int, color: str) -> None:
        indicator_list = st.session_state.indicators.setdefault(indicator_name, [])
        if quick_indicator_exists(indicator_name, period) or indicator_exists(indicator_name, period):
            return
        indicator_list.append({"period": period, "color_code": color, "source": 'quick_toggle'})

    def remove_quick_indicator(indicator_name: str, period: int) -> None:
        indicator_list = st.session_state.indicators.get(indicator_name, [])
        if not indicator_list:
            return
        filtered = []
        removed = False
        for instance in indicator_list:
            matches = int(instance.get('period', 0)) == int(period) and instance.get('source') == 'quick_toggle'
            if matches and not removed:
                removed = True
                continue
            filtered.append(instance)
        if filtered:
            st.session_state.indicators[indicator_name] = filtered
        elif indicator_name in st.session_state.indicators:
            del st.session_state.indicators[indicator_name]

    # Sidebar
    with st.sidebar:
        st.header("Data Selection")
        data_src = st.radio("Data Source", ["SQL", "Upstox"], horizontal=True)
        asset = st.radio("Asset Type", ["Stock", "Index"], horizontal=True)
        tables_list = sorted(get_stocks_index_data(asset))
        default_asset = "SBIN" if asset == 'Stock' else "NIFTY 50"
        stock_name = st.selectbox("Select Stock Symbol", tables_list, index=tables_list.index(default_asset))

        timeframe_options = get_timeframe_options(data_src)
        timeframe_labels = [label for label, _ in timeframe_options]
        default_primary_label = 'Daily' if 'Daily' in timeframe_labels else timeframe_labels[0]
        primary_label = st.selectbox("Primary Timeframe", timeframe_labels, index=timeframe_labels.index(default_primary_label))
        primary_value = get_timeframe_value(primary_label, timeframe_options)

        if data_src == 'SQL':
            df = extract_stock_data(stock_name, data_source='SQL', period=primary_value, interval='1')
        else:
            df = extract_stock_data(stock_name, data_source='Upstox', period=primary_value, interval='1')

        if df.empty:
            st.warning("No data available for the selected source/timeframe.")
            st.stop()

        max_additional = max(0, len(timeframe_labels) - 1)
        previous_count = min(len(st.session_state.additional_chart_timeframes), max_additional)
        default_extra = previous_count if previous_count else min(2, max_additional)
        additional_chart_count = st.slider("Additional Charts", 0, max_additional, default_extra, help="Add parallel charts with independent timeframes.") if max_additional > 0 else 0
        current_configs = st.session_state.additional_chart_timeframes[:additional_chart_count]
        available_defaults = [label for label in timeframe_labels if label != primary_label] or timeframe_labels
        idx = 0
        while len(current_configs) < additional_chart_count:
            current_configs.append(available_defaults[idx % len(available_defaults)])
            idx += 1
        st.session_state.additional_chart_timeframes = current_configs

        data_replay = st.checkbox("Replay Data", value=False)

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
                params['period'] = st.number_input(f"{selected_indicator} Period", value=14, min_value=2, max_value=200)
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
            for preset in QUICK_INDICATOR_PRESETS:
                toggle_key = f"quick_toggle_{preset['indicator']}_{preset['period']}"
                if toggle_key in st.session_state:
                    st.session_state[toggle_key] = False

        st.header("View Options")
        show_summary = st.checkbox("Show Summary")
        show_heatmap = st.checkbox("Show Heatmap")
        show_analysis = st.checkbox("Show Analysis Tools")
        show_eod = st.checkbox("Show EOD Analysis")
        show_data = st.checkbox("Show Raw Data")

    # Chart Section (Always Visible)
    if 'chart_theme' not in st.session_state:
        st.session_state.chart_theme = 'dark'
    current_theme = st.session_state.chart_theme

    header_col, toggle_controls_col, replay_controls_col, theme_col = st.columns([0.08, 0.58, 0.29, 0.05], gap='small', vertical_alignment='top')
    header_col.subheader(f":rainbow[{stock_name}]")

    theme_symbol = "\U0001f319" if current_theme == 'dark' else "\u2600\ufe0f"
    if theme_col.button(theme_symbol, help="Toggle chart theme"):
        st.session_state.chart_theme = 'light' if current_theme == 'dark' else 'dark'
        current_theme = st.session_state.chart_theme
        theme_symbol = "\U0001f319" if current_theme == 'dark' else "\u2600\ufe0f"

    toggle_widget = getattr(st, "toggle", st.checkbox)

    def _chunk_controls(items, size):
        for idx in range(0, len(items), size):
            yield items[idx:idx + size]

    toggle_entries = [{
        'kind': 'patterns',
        'label': "Apply Patterns",
        'key': "apply_patterns_toggle"
    }]
    for preset in QUICK_INDICATOR_PRESETS:
        toggle_entries.append({
            'kind': 'preset',
            'label': preset['label'],
            'key': f"quick_toggle_{preset['indicator']}_{preset['period']}",
            'preset': preset
        })

    apply_patterns_state = st.session_state.get("apply_patterns_toggle", False)
    with toggle_controls_col:
        for row in _chunk_controls(toggle_entries, 6):
            cols = st.columns(len(row), gap='small')
            for entry, col in zip(row, cols):
                with col:
                    if entry['kind'] == 'patterns':
                        apply_patterns_state = toggle_widget(entry['label'], key=entry['key'])
                    else:
                        preset = entry['preset']
                        toggle_key = entry['key']
                        if toggle_key not in st.session_state:
                            st.session_state[toggle_key] = quick_indicator_exists(preset['indicator'], preset['period'])
                        toggle_state = toggle_widget(preset['label'], key=toggle_key)
                        quick_active = quick_indicator_exists(preset['indicator'], preset['period'])
                        currently_active = indicator_exists(preset['indicator'], preset['period'])
                        if toggle_state and not quick_active and not currently_active:
                            add_quick_indicator(preset['indicator'], preset['period'], preset['color'])
                        elif not toggle_state and quick_active:
                            remove_quick_indicator(preset['indicator'], preset['period'])
    apply_patterns = apply_patterns_state

    with replay_controls_col:
        replay_button_cols = st.columns(3, gap='small')
        prev_clicked = replay_button_cols[0].button("Prev Day", icon="🔙")
        next_clicked = replay_button_cols[1].button("Next Day", icon="🔜")
        play_clicked = replay_button_cols[2].button("Play|Pause", icon="⏯️")

    num_subcharts = len([ind for ind in st.session_state.indicators if ind in ['RSI', 'MACD']])
    total_height = 700 + (num_subcharts * 100)
    chart_placeholder = st.empty()

    timeframe_lookup = {label: value for label, value in timeframe_options}
    theme_name = 'dark' if current_theme == 'dark' else 'default'
    subchart_height = 450

    if not data_replay and st.session_state.current_replay_index == -1 and not df.empty:
        st.session_state.current_replay_index = len(df) - 1

    def fetch_dataframe_for_label(label):
        value = timeframe_lookup[label]
        if data_src == 'SQL':
            return extract_stock_data(stock_name, data_source='SQL', period=value, interval='1').copy()
        return extract_stock_data(stock_name, data_source='Upstox', period=value, interval='1').copy()

    def update_chart_data():
        replay_data = df.iloc[:st.session_state.current_replay_index + 1].copy()
        if replay_data.empty:
            replay_data = df.copy()
        replay_data = calculate_stock_technical_summary(replay_data, st.session_state.indicators)
        with chart_placeholder.container():
            st.markdown(f"**Primary - {primary_label}**")
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
            create_tv_chart(chart_obj, stock_name, replay_data, theme=theme_name)

            for idx, label in enumerate(st.session_state.additional_chart_timeframes):
                st.divider()
                selector_cols = st.columns([0.7, 0.3])
                
                with selector_cols[1]:
                    selected_label = st.selectbox(
                        "Timeframe",
                        timeframe_labels,
                        index=timeframe_labels.index(label),
                        key=f"additional_timeframe_selector_{idx}"
                    )

                with selector_cols[0]:
                    st.markdown(f"**Chart {idx + 2} - {selected_label}**")
                if selected_label != label:
                    st.session_state.additional_chart_timeframes[idx] = selected_label
                    label = selected_label
                sub_df = fetch_dataframe_for_label(label)
                sub_df = calculate_stock_technical_summary(sub_df, st.session_state.indicators)
                sub_chart = StreamlitChart(height=subchart_height, toolbox=True, scale_candles_only=True)
                create_tv_chart(sub_chart, stock_name, sub_df, theme=theme_name)

    if prev_clicked:
        st.session_state.current_replay_index = max(0, st.session_state.current_replay_index - 1)
        update_chart_data()
    if next_clicked:
        st.session_state.current_replay_index = min(len(df) - 1, st.session_state.current_replay_index + 1)
        update_chart_data()
    if play_clicked:
        st.session_state.is_playing = not st.session_state.is_playing

    if data_replay:
        if st.session_state.current_replay_index == -1 or replay_date != st.session_state.last_replay_date:
            # Use nearest previous available index if the exact day is missing (holidays/weekends/gaps)
            target_ts = pd.Timestamp(replay_date) - dt.timedelta(days=1)
            idx_pos = df.index.get_indexer([target_ts], method='pad')[0]
            if idx_pos == -1:
                idx_pos = 0
            st.session_state.current_replay_index = int(idx_pos)
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
    if show_summary or show_heatmap or show_analysis or show_eod or show_data:
        tabs = st.tabs([tab for tab, show in
                        [("Summary", show_summary), ("Heatmap", show_heatmap), ("Analysis Tools", show_analysis),
                         ("EOD Analysis", show_eod), ("Raw Data", show_data)] if show])

        tab_index = 0
        if show_summary:
            with tabs[tab_index]:
                st.header("Stock Summary")
                summary_data = calculate_stock_summary(df.iloc[:st.session_state.current_replay_index + 1])
                
                # Add validation for empty dataframe
                if summary_data.empty or len(summary_data) == 0:
                    st.warning("Insufficient data for summary. Please select a different timeframe or stock.")
                    return  # Exit early from this tab section
                
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
                                                    period="Monthly", interval='1')
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
                    
                    # Add validation for empty dataframe
                    if summary_data.empty or len(summary_data) == 0:
                        st.warning("Insufficient data for analysis tools. Please select a different timeframe or stock.")
                        return  # Exit early from this tab section
                    
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
                        st.line_chart(combined, width='stretch')
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
                        st.markdown("No Breakouts detected")
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

        if show_eod:
            with tabs[tab_index]:
                st.header("EOD Analysis")
                sanitized_symbol = sanitize_sql_symbol(stock_name)
                table_name = f"{sanitized_symbol}_SUMMARY"
                st.caption(
                    "Trigger a fresh EOD run for this symbol and store the full dataset in "
                    f"`{table_name}` (`nsedata` database) for deeper review."
                )
                eod_days = st.number_input(
                    "Analysis Window (days)",
                    min_value=120,
                    max_value=1250,
                    value=365,
                    step=5,
                    key=f"eod_analysis_days_{sanitized_symbol}"
                )
                run_button = st.button(
                    "Run EOD Analysis",
                    key=f"run_eod_analysis_{sanitized_symbol}"
                )
                if run_button:
                    with st.spinner("Running EOD analysis and loading summary..."):
                        try:
                            eod_runner = EODAnalysis(
                                stocks_list=[sanitized_symbol],
                                analysis_days=int(eod_days)
                            )
                            eod_df = eod_runner.process_stock_data(sanitized_symbol).copy()
                            eod_df['Display_Symbol'] = stock_name
                            load_msg = rd.load_sql_data(
                                data_to_load=eod_df,
                                table_name=table_name,
                                database='nsedata',
                                load_type='replace',
                                schema='public'
                            )
                            st.session_state.eod_analysis_results[sanitized_symbol] = eod_df
                            st.session_state.eod_analysis_status[sanitized_symbol] = load_msg
                            st.success(load_msg)
                        except Exception as exc:
                            error_msg = f"Failed to run analysis: {exc}"
                            st.session_state.eod_analysis_status[sanitized_symbol] = error_msg
                            st.error(error_msg)

                if sanitized_symbol in st.session_state.eod_analysis_status:
                    status_msg = st.session_state.eod_analysis_status[sanitized_symbol]
                    st.markdown(f"**Target Table:** `public.\"{table_name}\"`")
                    st.markdown(f"**Last Operation:** {status_msg}")

                if sanitized_symbol in st.session_state.eod_analysis_results:
                    result_df = st.session_state.eod_analysis_results[sanitized_symbol]
                    if not result_df.empty and 'timestamp' in result_df.columns:
                        latest_row = result_df.sort_values('timestamp').iloc[-1]
                        latest_ts = latest_row['timestamp']
                        timestamp_text = latest_ts.strftime("%Y-%m-%d") if hasattr(latest_ts, 'strftime') else latest_ts
                        summary_lines = build_eod_signal_summary(latest_row)
                        st.subheader(f"Latest Signals - {timestamp_text}")
                        if summary_lines:
                            for line in summary_lines:
                                st.markdown(f"- {line}")
                        else:
                            st.info("No standout signals detected for the latest session.")

                    st.dataframe(
                        result_df.sort_values('timestamp', ascending=False).head(200),
                        width='stretch'
                    )

            tab_index += 1

        if show_data:
            with tabs[tab_index]:
                st.header("Raw Data")
                st.dataframe(df.iloc[:st.session_state.current_replay_index + 1].sort_index(ascending=False))

if __name__ == "__main__":
    stock_analysis()
