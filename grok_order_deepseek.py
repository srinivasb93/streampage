import streamlit as st
import upstox_client
from upstox_client.rest import ApiException
import pandas as pd
from datetime import datetime, timedelta
import time
import threading
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv
import os
import websocket
import json
import requests
from lightweight_charts.widgets import StreamlitChart
import numpy as np
import logging
import certifi
from common_utils import read_write_sql_data as rd
from python_scripts.backtest_strategy import backtest_etf
from websocket_manager import (initialize_websocket, subscribe_to_instrument, get_live_data,
                               is_connected, get_subscribed_instruments)
from queue import Queue
from threading import Thread
import uuid
import altair as alt
import schedule
import matplotlib.pyplot as plt

# Add a queue to manage scheduled orders
scheduled_orders_queue = Queue()

# Load environment variables
load_dotenv()
ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
SANDBOX_ACCESS_TOKEN = os.getenv("SANDBOX_ACCESS_TOKEN")
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

st.set_page_config(page_title="Upstox Trading Dashboard", layout="wide")
# Custom CSS for UI Enhancement
st.markdown("""
    <style>
    .main { background-color: #f5f5f5; padding: 20px; border-radius: 10px; margin-top: -5em;}
    .stButton>button { background-color: #4CAF50; color: white; border-radius: 5px; }
    .stTextInput>input { border-radius: 5px; }
    .sidebar .sidebar-content { background-color: #e0e0e0; padding: 10px; border-radius: 10px; }
    .metric-box { background-color: #336699; padding: 10px; border-radius: 5px; box-shadow: 2px 2px 5px rgba(0,0,0,0.1); }
    </style>
""", unsafe_allow_html=True)


# Upstox API Initialization
def init_upstox_api():
    config = upstox_client.Configuration()
    config.access_token = st.session_state.get("access_token", ACCESS_TOKEN)
    api_client = upstox_client.ApiClient(config)
    return {
        "order": upstox_client.OrderApi(api_client),
        "portfolio": upstox_client.PortfolioApi(api_client),
        "history": upstox_client.HistoryApi(api_client),
        "charges": upstox_client.ChargeApi(api_client),
        "market_data": upstox_client.MarketQuoteApi(api_client),
        "user": upstox_client.UserApi(api_client)
    }


# Force logging to console
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True)
logger = logging.getLogger(__name__)


if "data_source" not in st.session_state:
    st.session_state["data_source"] = "rest"

# Initialize WebSocket with access token
if "websocket_initialized" not in st.session_state and st.session_state["data_source"] == "websocket":
    access_token = st.session_state.get("access_token", ACCESS_TOKEN)
    if not access_token:
        st.error("Access token missing. Please fetch a new token.")
    else:
        initialize_websocket(access_token)
        st.session_state["websocket_initialized"] = True


# Fetch Access Token
def fetch_access_token(api_key, api_secret, redirect_uri, auth_code):
    url = "https://api.upstox.com/v2/login/authorization/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "client_id": api_key,
        "client_secret": api_secret,
        "redirect_uri": redirect_uri
    }
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        token = response.json()["access_token"]
        st.session_state["access_token"] = token
        with open(".env", "r+") as f:
            lines = f.readlines()
            f.seek(0)
            for line in lines:
                if not line.startswith("UPSTOX_ACCESS_TOKEN"):
                    f.write(line)
            f.write(f"UPSTOX_ACCESS_TOKEN={token}\n")
        return token
    else:
        st.error(f"Failed to fetch token: {response.text}")
        return None


# Email Notification
def send_email(subject, body):
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_SENDER
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
    except Exception as e:
        st.error(f"Failed to send email: {e}")

def notify(subject, body):
    if st.session_state.get("email_notifications", False):
        threading.Thread(target=send_email, args=(subject, body), daemon=True).start()


# Place Order
def place_order(api, instrument_token, transaction_type, quantity, price=0, order_type="MARKET",
                trigger_price=0, is_amo=False, product_type="D", validity='DAY', stop_loss=None, target=None):
    try:
        order = upstox_client.PlaceOrderRequest(
            quantity=quantity,
            product=product_type,
            validity=validity,
            price=price,
            tag="StreamlitOrder",
            instrument_token=instrument_token,
            order_type=order_type,
            transaction_type=transaction_type,
            disclosed_quantity=0,
            trigger_price=trigger_price,
            is_amo=is_amo
        )
        response = api.place_order(order, api_version="v2")
        primary_order_id = response.data.order_id
        notify(f"Order Placed: {transaction_type}", f"Order ID: {primary_order_id}")

        # Store stop-loss and target order IDs
        sl_order_id = None
        target_order_id = None

        # Place stop-loss order
        if stop_loss and transaction_type == "BUY":
            sl_response = place_order(api, instrument_token, "SELL", quantity, order_type="SL",
                                      trigger_price=stop_loss, price=stop_loss * 0.995, product_type=product_type)
            if sl_response:
                sl_order_id = sl_response.data.order_id
                notify("Stop-Loss Order Placed", f"SL Order ID: {sl_order_id}")
        elif stop_loss and transaction_type == "SELL":
            sl_response = place_order(api, instrument_token, "BUY", quantity, order_type="SL",
                                      trigger_price=stop_loss, price=stop_loss * 1.005, product_type=product_type)
            if sl_response:
                sl_order_id = sl_response.data.order_id
                notify("Stop-Loss Order Placed", f"SL Order ID: {sl_order_id}")

        # Place target order
        if target and transaction_type == "BUY":
            target_response = place_order(api, instrument_token, "SELL", quantity, order_type="LIMIT",
                                          price=target, product_type=product_type)
            if target_response:
                target_order_id = target_response.data.order_id
                notify("Target Order Placed", f"Target Order ID: {target_order_id}")
        elif target and transaction_type == "SELL":
            target_response = place_order(api, instrument_token, "BUY", quantity, order_type="LIMIT",
                                          price=target, product_type=product_type)
            if target_response:
                target_order_id = target_response.data.order_id
                notify("Target Order Placed", f"Target Order ID: {target_order_id}")

        # Start monitoring if stop-loss or target orders are placed
        if sl_order_id or target_order_id:
            def monitor_orders(primary_id, sl_id, target_id):
                # Wait for primary order to complete
                while True:
                    order_status = api.get_order_details(primary_id, api_version="v2").data.status
                    if order_status in ["complete", "rejected", "cancelled"]:
                        break
                    time.sleep(1)

                if order_status == "complete" and (sl_id or target_id):
                    while True:
                        sl_status = api.get_order_details(sl_id, api_version="v2").data.status if sl_id else "cancelled"
                        target_status = api.get_order_details(target_id, api_version="v2").data.status if target_id else "cancelled"

                        if sl_status == "complete" and target_id and target_status not in ["complete", "cancelled"]:
                            api.cancel_order(target_id, api_version="v2")
                            notify("Target Order Cancelled", f"Target ID {target_id} cancelled due to SL execution")
                            break
                        elif target_status == "complete" and sl_id and sl_status not in ["complete", "cancelled"]:
                            api.cancel_order(sl_id, api_version="v2")
                            notify("Stop-Loss Order Cancelled", f"SL ID {sl_id} cancelled due to Target execution")
                            break
                        elif sl_status in ["cancelled", "rejected"] and target_status in ["cancelled", "rejected"]:
                            break
                        time.sleep(10)

            Thread(target=monitor_orders, args=(primary_order_id, sl_order_id, target_order_id), daemon=True).start()

        return response
    except ApiException as e:
        st.error(f"Error placing order: {e}")
        return None


def manage_scheduled_orders():
    scheduled_orders = []
    while not scheduled_orders_queue.empty():
        scheduled_orders.append(scheduled_orders_queue.get())
    return scheduled_orders


def update_scheduled_order(order_id, new_quantity=None, new_price=None, new_trigger_price=None,
                           new_schedule_time=None, new_stop_loss=None, new_target=None):
    scheduled_orders = manage_scheduled_orders()
    for order in scheduled_orders:
        if order["order_id"] == order_id:
            if new_quantity:
                order["quantity"] = new_quantity
            if new_price:
                order["price"] = new_price
            if new_trigger_price:
                order["trigger_price"] = new_trigger_price
            if new_schedule_time:
                order["schedule_datetime"] = new_schedule_time
            if new_stop_loss is not None:
                order["stop_loss"] = new_stop_loss
            if new_target is not None:
                order["target"] = new_target
    # Requeue updated orders
    for order in scheduled_orders:
        scheduled_orders_queue.put(order)


# Fetch Historical Data
def get_historical_data(api, instrument_key, interval="day", sort_data=False, days=365):
    try:
        from_date = datetime.now() - timedelta(days=5*days)
        to_date = datetime.now()
        response = api.get_historical_candle_data1(
            instrument_key=instrument_key,
            interval=interval,
            to_date=to_date.strftime("%Y-%m-%d"),
            from_date=from_date.strftime("%Y-%m-%d"),
            api_version="v2"
        )
        df = pd.DataFrame(response.data.candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        if sort_data:
            df.sort_values(by='timestamp', inplace=True)
        return df
    except ApiException as e:
        st.error(f"Error fetching historical data: {e}")
        return None


def get_live_data(instrument_token=None):
    if st.session_state["data_source"] == "websocket":
        if is_connected() and instrument_token in get_subscribed_instruments():
            return get_live_data(instrument_token)  # Original WebSocket function from websocket_manager
        else:
            logger.warning("WebSocket not connected or instrument not subscribed. Falling back to REST.")
    # Fallback to REST API
    try:
        print(f"Inside Get live - {instrument_token}")
        quote = get_market_quote(apis["market_data"], instrument_token)
        return quote
        # return {
        #     "ltp": quote["ltp"],
        #     "depth": None  # REST API doesn't provide depth in this format; adjust as needed
        # }
    except Exception as e:
        logger.error(f"Error fetching REST data: {e}")
        return {"ltp": 0, "depth": None}


def get_user_profile_and_funds(api, api_version='2.0', user_profile=False):
    """Get user funds"""
    try:
        if user_profile:
            response = api.get_profile(api_version)
        else:
            response = api.get_user_fund_margin(api_version)
        return response.to_dict()
    except ApiException as e:
        logger.error(f"Exception when calling UserApi->get_funds: {e}")
        return None


def fetch_instruments():
    """Fetch instruments"""
    try:
        path = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
        instruments_df = pd.read_json(path)
        instruments_df = instruments_df[['trading_symbol', 'instrument_key']][(instruments_df['segment'] == 'NSE_EQ') & (instruments_df['instrument_type'] == 'EQ')]
        instruments_dict = dict(zip(instruments_df['trading_symbol'].values.tolist(),
                                    instruments_df['instrument_key'].values.tolist()))
        return instruments_dict
    except ApiException as e:
        logger.error(f"Exception when fetching instruments data: {e}")
        return None


def get_market_quote(api, instrument_tokens, mode="full"):
    """Get market quotes for instruments"""
    try:
        if mode == "full":
            api_response = api.get_full_market_quote(instrument_tokens, api_version="v2").data
        elif mode == 'ohlc':
            api_response = api.get_market_quote_ohlc(instrument_tokens, interval='1d', api_version="v2").data
        else:
            api_response = api.ltp(instrument_tokens, api_version="v2").data
        latest_data = {}
        for key, data in api_response.items():
            latest_data = data

        if mode == 'full':
            return_data = {"symbol": latest_data.instrument_token,
                            "ltp": latest_data.last_price,
                            "open": latest_data.ohlc.open,
                            "high": latest_data.ohlc.high,
                            "low": latest_data.ohlc.low,
                            "close": latest_data.ohlc.close
                        }
        elif mode == 'ohlc':
            return_data = {
                "symbol": latest_data.symbol,
                "ltp": latest_data.last_price,
                "open": latest_data.ohlc.open,
                "high": latest_data.ohlc.high,
                "low": latest_data.ohlc.low,
                "close": latest_data.ohlc.close,  # Previous close
                "volume": latest_data.volume  # Market depth
            }
        else:
            return_data = {
                "symbol": latest_data.instrument_token,
                "ltp": latest_data.last_price,
            }
        return return_data
    except ApiException as e:
        logger.error(f"Exception when calling MarketQuoteApi->get_quotes: {e}")
        return None


def calculate_brokerage(api, instrument_token, quantity, price, transaction_type, product_type='D'):
    try:
        response = api.get_brokerage(
            instrument_token=instrument_token,
            quantity=quantity,
            price=price,
            transaction_type=transaction_type,
            product=product_type,
            api_version="v2")
        return response.data.charges.total
    except ApiException as e:
        st.error(f"Error calculating brokerage: {e}")
        return 0


def format_currency(value):
    """Format currency values for display"""
    if isinstance(value, (int, float)):
        return f"₹{value:,.2f}"
    return value


# Calculate Indicators
def calculate_ema(df, period=20):
    return df["close"].ewm(span=period, adjust=False).mean()


def calculate_linear_regression(df, period=20):
    x = np.arange(len(df[-period:]))
    y = df["close"].iloc[-period:].values
    slope, intercept = np.polyfit(x, y, 1)
    return pd.Series(slope * x + intercept, index=df.index[-period:])


# Calculate RSI
def calculate_rsi(df, period=14):
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


# Define algorithmic trading strategies
def calculate_macd(df, fast_period=12, slow_period=26, signal_period=9):
    """Calculate MACD indicator."""
    # Calculate EMAs
    fast_ema = df['close'].ewm(span=fast_period, adjust=False).mean()
    slow_ema = df['close'].ewm(span=slow_period, adjust=False).mean()

    # Calculate MACD line and signal line
    macd_line = fast_ema - slow_ema
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()

    # Calculate histogram
    histogram = macd_line - signal_line

    return macd_line, signal_line, histogram


def calculate_bollinger_bands(df, period=20, num_std=2):
    """Calculate Bollinger Bands."""
    # Calculate rolling mean and standard deviation
    rolling_mean = df['close'].rolling(window=period).mean()
    rolling_std = df['close'].rolling(window=period).std()

    # Calculate upper and lower bands
    upper_band = rolling_mean + (rolling_std * num_std)
    lower_band = rolling_mean - (rolling_std * num_std)

    return rolling_mean, upper_band, lower_band


def calculate_stochastic_oscillator(df, k_period=14, d_period=3):
    """Calculate Stochastic Oscillator."""
    # Calculate %K
    low_min = df['low'].rolling(window=k_period).min()
    high_max = df['high'].rolling(window=k_period).max()
    k = 100 * ((df['close'] - low_min) / (high_max - low_min))

    # Calculate %D (signal line)
    d = k.rolling(window=d_period).mean()

    return k, d


def check_macd_crossover(macd_line, signal_line):
    """Check for MACD crossover signals."""
    if len(macd_line) < 2 or len(signal_line) < 2:
        return None

    # Check for bullish crossover (MACD crosses above signal line)
    if macd_line.iloc[-2] < signal_line.iloc[-2] and macd_line.iloc[-1] > signal_line.iloc[-1]:
        return "BUY"

    # Check for bearish crossover (MACD crosses below signal line)
    elif macd_line.iloc[-2] > signal_line.iloc[-2] and macd_line.iloc[-1] < signal_line.iloc[-1]:
        return "SELL"

    return None


def check_bollinger_band_signals(df, upper_band, lower_band):
    """Check for Bollinger Band signals."""
    if len(df) < 2:
        return None

    # Price breaks above upper band (overbought)
    if df['close'].iloc[-2] <= upper_band.iloc[-2] and df['close'].iloc[-1] > upper_band.iloc[-1]:
        return "SELL"

    # Price breaks below lower band (oversold)
    elif df['close'].iloc[-2] >= lower_band.iloc[-2] and df['close'].iloc[-1] < lower_band.iloc[-1]:
        return "BUY"

    return None


def check_stochastic_signals(k, d, overbought=80, oversold=20):
    """Check for Stochastic Oscillator signals."""
    if len(k) < 2 or len(d) < 2:
        return None

    # Oversold condition with bullish crossover
    if (k.iloc[-2] < d.iloc[-2] and k.iloc[-1] > d.iloc[-1]) and k.iloc[-1] < oversold:
        return "BUY"

    # Overbought condition with bearish crossover
    elif (k.iloc[-2] > d.iloc[-2] and k.iloc[-1] < d.iloc[-1]) and k.iloc[-1] > overbought:
        return "SELL"

    return None


def check_support_resistance_breakout(df, lookback=20):
    """Identify support and resistance breakouts."""
    if len(df) < lookback + 2:
        return None

    recent_high = df['high'].iloc[-lookback:-2].max()
    recent_low = df['low'].iloc[-lookback:-2].min()

    # Breakout above resistance
    if df['close'].iloc[-2] < recent_high and df['close'].iloc[-1] > recent_high:
        return "BUY"

    # Breakdown below support
    elif df['close'].iloc[-2] > recent_low and df['close'].iloc[-1] < recent_low:
        return "SELL"

    return None


# Backtest a strategy
def backtest_strategy(df, strategy_func, **kwargs):
    """Simple backtest framework."""
    # Make a copy of the dataframe
    df_copy = df.copy()

    # Apply strategy and get signals
    signals = strategy_func(df_copy, **kwargs)

    # Add signals to dataframe
    df_copy['signal'] = signals

    # Initialize positions and pnl columns
    df_copy['position'] = 0
    df_copy['pnl'] = 0

    # Calculate positions based on signals
    position = 0
    buy_price = 0

    for i, row in df_copy.iterrows():
        if row['signal'] == "BUY" and position == 0:
            position = 1
            buy_price = row['close']
            df_copy.at[i, 'position'] = position
        elif row['signal'] == "SELL" and position == 1:
            position = 0
            sell_price = row['close']
            df_copy.at[i, 'position'] = position
            df_copy.at[i, 'pnl'] = sell_price - buy_price

    # Calculate cumulative PnL
    df_copy['cumulative_pnl'] = df_copy['pnl'].cumsum()

    return df_copy


# Strategy implementations
def macd_strategy(df, fast_period=12, slow_period=26, signal_period=9):
    """MACD crossover strategy."""
    macd_line, signal_line, _ = calculate_macd(df, fast_period, slow_period, signal_period)

    # Initialize signals
    signals = pd.Series(index=df.index, dtype='object')
    signals[:] = None

    # Find crossovers
    for i in range(1, len(df)):
        if i > signal_period:
            if macd_line.iloc[i - 1] < signal_line.iloc[i - 1] and macd_line.iloc[i] > signal_line.iloc[i]:
                signals.iloc[i] = "BUY"
            elif macd_line.iloc[i - 1] > signal_line.iloc[i - 1] and macd_line.iloc[i] < signal_line.iloc[i]:
                signals.iloc[i] = "SELL"

    return signals


def bollinger_band_strategy(df, period=20, num_std=2):
    """Bollinger Bands mean reversion strategy."""
    _, upper_band, lower_band = calculate_bollinger_bands(df, period, num_std)

    # Initialize signals
    signals = pd.Series(index=df.index, dtype='object')
    signals[:] = None

    # Find band touches
    for i in range(1, len(df)):
        if i > period:
            if df['close'].iloc[i] < lower_band.iloc[i]:
                signals.iloc[i] = "BUY"
            elif df['close'].iloc[i] > upper_band.iloc[i]:
                signals.iloc[i] = "SELL"

    return signals


def rsi_strategy(df, period=14, overbought=70, oversold=30):
    """RSI strategy."""
    rsi = calculate_rsi(df, period)

    # Initialize signals
    signals = pd.Series(index=df.index, dtype='object')
    signals[:] = None

    # Find overbought/oversold conditions
    for i in range(1, len(df)):
        if i > period:
            if rsi.iloc[i - 1] > overbought and rsi.iloc[i] <= overbought:
                signals.iloc[i] = "SELL"
            elif rsi.iloc[i - 1] < oversold and rsi.iloc[i] >= oversold:
                signals.iloc[i] = "BUY"

    return signals


# Automated trading function
def auto_trade(api, strategy_name, instrument_token, quantity, stop_loss_percent=1.0, take_profit_percent=2.0):
    """Execute automated trading based on selected strategy."""
    try:
        # Get historical data
        data = get_historical_data(api, instrument_token)
        if data is None:
            return "Failed to fetch historical data"

        # Apply strategy
        signal = None

        if strategy_name == "MACD Crossover":
            macd_line, signal_line, _ = calculate_macd(data)
            signal = check_macd_crossover(macd_line, signal_line)

        elif strategy_name == "Bollinger Bands":
            _, upper_band, lower_band = calculate_bollinger_bands(data)
            signal = check_bollinger_band_signals(data, upper_band, lower_band)

        elif strategy_name == "RSI Oversold/Overbought":
            rsi = calculate_rsi(data)
            if rsi.iloc[-1] < 30:
                signal = "BUY"
            elif rsi.iloc[-1] > 70:
                signal = "SELL"

        elif strategy_name == "Stochastic Oscillator":
            k, d = calculate_stochastic_oscillator(data)
            signal = check_stochastic_signals(k, d)

        elif strategy_name == "Support/Resistance Breakout":
            signal = check_support_resistance_breakout(data)

        # Execute trade if there's a signal
        if signal:
            result = place_order(api, instrument_token, signal, quantity)

            # Set stop loss and take profit orders
            if result and result.data and result.data.order_id:
                current_price = data['close'].iloc[-1]

                if signal == "BUY":
                    stop_loss_price = current_price * (1 - stop_loss_percent / 100)
                    take_profit_price = current_price * (1 + take_profit_percent / 100)

                    # Place stop loss order
                    place_order(api, instrument_token, "SELL", quantity,
                                price=0, order_type="SL-M", trigger_price=stop_loss_price)

                    # Place take profit order
                    place_order(api, instrument_token, "SELL", quantity,
                                price=take_profit_price, order_type="LIMIT")

                elif signal == "SELL":
                    stop_loss_price = current_price * (1 + stop_loss_percent / 100)
                    take_profit_price = current_price * (1 - take_profit_percent / 100)

                    # Place stop loss order
                    place_order(api, instrument_token, "BUY", quantity,
                                price=0, order_type="SL-M", trigger_price=stop_loss_price)

                    # Place take profit order
                    place_order(api, instrument_token, "BUY", quantity,
                                price=take_profit_price, order_type="LIMIT")

                return f"{signal} signal detected and executed with stop loss and take profit"

            return f"{signal} signal detected but order failed"

        return "No trading signal detected"

    except Exception as e:
        return f"Error in auto_trade: {str(e)}"


# Scheduled trading function
def schedule_strategy_execution(api, strategy_name, instrument_token, quantity,
                                interval_minutes=5, run_hours=None):
    """Schedule a strategy to run at regular intervals."""
    if run_hours is None:
        run_hours = [(9, 15), (15, 30)]  # Default market hours

    def is_market_open():
        now = datetime.now()
        weekday = now.weekday()

        # Check if weekend (5=Saturday, 6=Sunday)
        if weekday >= 5:
            return False

        # Check if within trading hours
        for start_hour, end_hour in run_hours:
            start_time = now.replace(hour=start_hour, minute=0, second=0)
            end_time = now.replace(hour=end_hour, minute=0, second=0)
            if start_time <= now <= end_time:
                return True

        return False

    def run_strategy():
        while True:
            if is_market_open():
                result = auto_trade(api, strategy_name, instrument_token, quantity)
                print(f"Strategy execution result: {result}")
            else:
                print("Market closed. Waiting for next interval.")

            # Wait for the next interval
            time.sleep(interval_minutes * 60)

    # Start the strategy execution in a background thread
    strategy_thread = threading.Thread(target=run_strategy, daemon=True)
    strategy_thread.start()

    return "Strategy scheduled successfully"


# Streamlit App

# Sidebar
with st.sidebar:
    st.subheader("Navigation")
    page = st.radio("Go to",
                    ["Order Management", "Order Book", "Positions", "Portfolio", "Analytics", "Algo Trading",
                     "Strategy Backtest", "Get Token"])

    st.subheader("Data Source")
    st.session_state["data_source"] = st.radio(
        "Select Data Source", ["websocket", "rest"],
        index=0 if st.session_state["data_source"] == "websocket" else 1
    )
    st.subheader("Settings")
    st.session_state["email_notifications"] = st.checkbox("Enable Email Notifications", value=False)
    if st.button("Refresh Dashboard", key="refresh"):
        st.rerun()

# Initialize API
apis = init_upstox_api()

if 'instruments_data' not in st.session_state:
    st.session_state['instruments_data'] = fetch_instruments()
instruments = st.session_state['instruments_data'] if 'instruments_data' in st.session_state else {}

# Get Token Page
if page == "Get Token":
    st.subheader("Fetch Upstox Access Token")
    api_key = st.text_input("API Key", value=API_KEY)
    api_secret = st.text_input("API Secret", value=API_SECRET)
    redirect_uri = st.text_input("Redirect URI", "https://api.upstox.com/v2/login")

    # Create an authorization URL
    auth_url = f"https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={api_key}&redirect_uri={redirect_uri}"
    st.markdown(f"[Click here to authorize]({auth_url})")

    auth_code = st.text_input("Authorization Code (from redirect URL)")
    if st.button("Fetch Access Token") and auth_code:
        token = fetch_access_token(api_key, api_secret, redirect_uri, auth_code)
        if token:
            st.success(f"Successfully fetched Access Token: {token}")
            st.session_state["access_token"] = token

# Place Order Page
# Combined Order & Risk Management Page
elif page == "Order Management":
    if 'funds_data' not in st.session_state:
        st.session_state['funds_data'] = get_user_profile_and_funds(apis['user'])
    st.subheader("Order Management")

    col1, col2 = st.columns(2)

    with col1:
        funds_cols = st.columns(2)
        with funds_cols[0]:
            try:
                st.markdown(
                    f'<div class="metric-box">Funds Available:'
                    f' ₹{st.session_state["funds_data"]["data"]["equity"]["available_margin"]:.2f}</div>',
                    unsafe_allow_html=True)
            except:
                st.markdown(
                    f'<div class="metric-box"> Unable to fetch Funds data </div>',
                    unsafe_allow_html=True)

        subscribed_list = get_subscribed_instruments()

        single_multi_order = st.radio("Order Type", ["Single Order", "Multiple Orders"], horizontal=True,
                                      label_visibility='collapsed')

        if single_multi_order == 'Single Order':
            order_cols = st.columns(3)
            stock_symbol = order_cols[0].selectbox("Select Symbol", options=instruments.keys())
            instrument_token = instruments.get(stock_symbol)
            st.session_state["selected_symbol"] = stock_symbol

            ltp_placeholder = st.empty()
            depth_placeholder = st.empty()
            if "last_update" not in st.session_state:
                st.session_state["last_update"] = time.time()

            current_time = time.time()
            if current_time - st.session_state["last_update"] >= 1:
                live_data = get_live_data(instrument_token)
                ltp = live_data.get('ltp', 0)
                with ltp_placeholder.container():
                    st.metric("Last Traded Price", f"₹{ltp:.2f}")
                if "depth" in live_data and live_data["depth"]:
                    with depth_placeholder.container():
                        st.dataframe(pd.DataFrame(live_data["depth"]))
                st.session_state["last_update"] = current_time

            quantity = order_cols[1].number_input("Quantity", min_value=1, value=1)
            order_type = order_cols[2].selectbox("Order Type", ["MARKET", "LIMIT", "SL", "SL-M"])

            other_order_cols = st.columns(3)
            transaction_type = other_order_cols[0].radio("Transaction Type", ["BUY", "SELL"], horizontal=True)
            product_type = other_order_cols[1].radio("Product Type", ['I', 'D'], horizontal=True)
            amo_order = other_order_cols[2].checkbox("AMO Order")

            order_type_cols = st.columns(3)

            if order_type == "LIMIT":
                price = order_type_cols[0].number_input("Limit Price", min_value=0.0, value=float(ltp) if ltp else 100.0)
                trigger_price = 0
            elif order_type == "SL":
                price = order_type_cols[0].number_input("Limit Price", min_value=0.0, value=float(ltp) if ltp else 100.0)
                trigger_price = order_type_cols[1].number_input("Trigger Price", min_value=0.0, value=95.0)
            elif order_type == "SL-M":
                price = 0
                trigger_price = order_type_cols[0].number_input("Stoploss Trigger", min_value=0.0, value=95.0)
            else:
                price, trigger_price = 0, 0

            # Stop-loss and target inputs
            stop_loss = order_type_cols[0].number_input("Stop-Loss Price", min_value=0.0, value=0.0, step=0.05)
            target = order_type_cols[1].number_input("Target Price", min_value=0.0, value=0.0, step=0.05)

            other_orders = st.selectbox('Other Orders', ["Auto-sell if Open > Previous Close", "Schedule Order"],
                                        index=None)

            if other_orders == "Schedule Order":
                schedule_cols = st.columns(2)
                schedule_time = schedule_cols[0].time_input("Schedule Time", value="now", step=60)
                schedule_date = schedule_cols[1].date_input("Schedule Date", value=datetime.now(), min_value=datetime.now())
                schedule_datetime = datetime.combine(schedule_date, schedule_time) + timedelta(seconds=2)

            if instrument_token not in subscribed_list and is_connected():
                subscribe_to_instrument(instrument_token)
                st.write(f"Auto-subscribed to {instrument_token}")

            if st.button("Place Order"):
                with st.spinner("Processing order..."):
                    if other_orders == "Auto-sell if Open > Previous Close":
                        hist_data = get_historical_data(apis["history"], instrument_token)
                        if hist_data is not None:
                            prev_close = hist_data["close"].iloc[-2]
                            current_open = hist_data["close"].iloc[-1]
                            if current_open > prev_close:
                                result = place_order(apis["order"], instrument_token, "SELL", quantity,
                                                    stop_loss=stop_loss, target=target)
                                if result:
                                    st.success(f"Auto-sell order placed: Order ID {result.data.order_id}")
                            else:
                                st.info("Condition not met: Open price not greater than previous close")
                    elif other_orders == "Schedule Order":
                        if schedule_datetime > datetime.now():
                            order_data = {
                                "order_id": f"scheduled_{uuid.uuid4()}",
                                "instrument_token": instrument_token,
                                "transaction_type": transaction_type,
                                "quantity": quantity,
                                "price": price,
                                "order_type": order_type,
                                "trigger_price": trigger_price,
                                "is_amo": amo_order,
                                "product_type": product_type,
                                "schedule_datetime": schedule_datetime,
                                "validity": "DAY",
                                "stop_loss": stop_loss,
                                "target": target
                            }
                            scheduled_orders_queue.put(order_data)

                            def execute_scheduled_order(order):
                                time.sleep((order["schedule_datetime"] - datetime.now()).total_seconds())
                                result = place_order(apis["order"], order["instrument_token"], order["transaction_type"],
                                                    order["quantity"], order["price"], order["order_type"],
                                                    order["trigger_price"], order["is_amo"], order["product_type"],
                                                     order['validity'], order["stop_loss"], order["target"])
                                if result:
                                    st.success(f"Scheduled order executed: Order ID {result.data.order_id}")

                            threading.Thread(target=execute_scheduled_order, args=(order_data,), daemon=True).start()
                            st.write(f"Order scheduled for {schedule_datetime}")
                        else:
                            st.error("Schedule time must be in the future.")
                    else:
                        result = place_order(apis["order"], instrument_token, transaction_type, quantity, price,
                                            order_type, trigger_price, amo_order, product_type, stop_loss, target)
                        if result:
                            st.success(f"Order placed: Order ID {result.data.order_id}")

    if single_multi_order == 'Multiple Orders':
        num_orders = col2.number_input("Number of Orders", min_value=1, max_value=10, value=1, step=1)
        orders = []

        for i in range(num_orders):
            st.write(f"Order {i + 1}")
            with st.expander(f"Details for Order {i + 1}", expanded=True):
                multi_order_cols = st.columns(9)
                stock_symbol = multi_order_cols[0].selectbox("Select Symbol", options=instruments.keys(),
                                                       key=f"multi_order_{i}")
                instrument_token = instruments.get(stock_symbol)

                live_data = get_live_data(instrument_token)
                last_traded_price = st.markdown(f":rainbow[Last Traded Price] - {live_data.get('ltp', 0)}")

                quantity = multi_order_cols[1].number_input(
                    f"Quantity {i + 1}", min_value=1, value=1, key=f"multi_qty_{i}")
                order_type = multi_order_cols[2].selectbox(
                    f"Order Type {i + 1}", ["MARKET", "LIMIT", "SL", "SL-M"], key=f"multi_order_type_{i}")
                transaction_type = multi_order_cols[3].radio(
                    f"Transaction Type {i + 1}", ["BUY", "SELL"], key=f"multi_trans_{i}", horizontal=True)
                product_type = multi_order_cols[4].radio(
                    "Product Type", ['I', 'D'], horizontal=True, key=f"multi_prod_type_{i}")
                amo_order = multi_order_cols[5].checkbox("AMO Order", key=f"multi_amo_{i}")
                stop_loss = multi_order_cols[6].number_input(f"Stop-Loss {i + 1}", min_value=0.0, value=0.0, key=f"multi_sl_{i}")
                target = multi_order_cols[7].number_input(f"Target {i + 1}", min_value=0.0, value=0.0, key=f"multi_target_{i}")
                schedule_order = multi_order_cols[8].checkbox("Schedule", key=f"multi_schedule_{i}")

                if order_type == "LIMIT":
                    price = multi_order_cols[6].number_input(f"Limit Price {i + 1}", min_value=0.0, value=100.0,
                                            key=f"multi_price_{i}")
                    trigger_price = 0
                elif order_type == "SL":
                    price = multi_order_cols[6].number_input(f"Limit Price {i + 1}", min_value=0.0, value=100.0,
                                            key=f"multi_price_sl_{i}")
                    trigger_price = multi_order_cols[7].number_input(
                        f"Trigger Price {i + 1}", min_value=0.0, value=95.0, key=f"multi_trigger_{i}")
                elif order_type == "SL-M":
                    price = 0
                    trigger_price = multi_order_cols[6].number_input(
                        f"Stoploss Trigger {i + 1}", min_value=0.0, value=95.0, key=f"multi_trigger_slm_{i}")
                else:
                    price, trigger_price = 0, 0

                if schedule_order:
                    schedule_cols = st.columns(2)
                    schedule_time = schedule_cols[0].time_input(
                        f"Schedule Time {i + 1}", value="now", step=60, key=f"multi_time_{i}")
                    schedule_date = schedule_cols[1].date_input(
                        f"Schedule Date {i + 1}", value=datetime.now(), min_value=datetime.now(), key=f"multi_date_{i}")
                    schedule_datetime = datetime.combine(schedule_date, schedule_time)
                else:
                    schedule_datetime = None

                order_data = {
                    "order_id": f"multi_scheduled_{i}_{uuid.uuid4()}",
                    "instrument_token": instrument_token,
                    "quantity": quantity,
                    "product": product_type,
                    "validity": "DAY",
                    "price": price,
                    "tag": f"MultiOrder_{i + 1}",
                    "order_type": order_type,
                    "transaction_type": transaction_type,
                    "disclosed_quantity": 0,
                    "trigger_price": trigger_price,
                    "is_amo": amo_order,
                    "correlation_id": f"order_{i}",
                    "slice": True,
                    "schedule_datetime": schedule_datetime,
                    "stop_loss": stop_loss,
                    "target": target
                }
                orders.append(order_data)

        if st.button("Place Multiple Orders"):
            with st.spinner("Placing multiple orders..."):
                for order in orders:
                    if order["schedule_datetime"] and order["schedule_datetime"] > datetime.now():
                        scheduled_orders_queue.put(order)

                        def execute_multi_scheduled_order(order):
                            time.sleep((order["schedule_datetime"] - datetime.now()).total_seconds())
                            result = place_order(apis["order"], order["instrument_token"], order["transaction_type"],
                                                order["quantity"], order["price"], order["order_type"],
                                                order["trigger_price"], order["is_amo"], order["product"],
                                                order["validity"], order["stop_loss"], order["target"])
                            if result:
                                st.success(f"Scheduled multi-order executed: Order ID {result.data.order_id}")

                        threading.Thread(target=execute_multi_scheduled_order, args=(order,), daemon=True).start()
                        st.write(f"Order {order['tag']} scheduled for {order['schedule_datetime']}")
                    else:
                        result = place_order(apis["order"], order["instrument_token"], order["transaction_type"],
                                            order["quantity"], order["price"], order["order_type"],
                                            order["trigger_price"], order["is_amo"], order["product"],
                                            order["validity"], order["stop_loss"], order["target"])
                        if result:
                            st.success(f"Order {order['tag']} placed: Order ID {result.data.order_id}")


    st.subheader("Scheduled Orders")
    scheduled_orders = manage_scheduled_orders()
    if scheduled_orders:
        scheduled_df = pd.DataFrame([
            {
                "Order ID": order["order_id"],
                "Symbol": next((k for k, v in instruments.items() if v == order["instrument_token"]), ""),
                "Quantity": order["quantity"],
                "Type": order["order_type"],
                "Transaction": order["transaction_type"],
                "Schedule Time": order["schedule_datetime"].strftime("%Y-%m-%d %H:%M:%S"),
                "Stop-Loss": order["stop_loss"],
                "Target": order["target"]
            } for order in scheduled_orders
        ])
        st.dataframe(scheduled_df)

        selected_scheduled_order = st.selectbox("Select Scheduled Order to Modify/Cancel", options=scheduled_df["Order ID"])
        if selected_scheduled_order:
            order = next(o for o in scheduled_orders if o["order_id"] == selected_scheduled_order)
            new_quantity = st.number_input("New Quantity", min_value=1, value=order["quantity"])
            new_price = st.number_input("New Price", min_value=0.0, value=float(order["price"]))
            new_trigger_price = st.number_input("New Trigger Price", min_value=0.0, value=float(order["trigger_price"]))
            new_schedule_time = st.date_input("New Schedule Time", value=order["schedule_datetime"],
                                                  min_value=datetime.now())
            new_stop_loss = st.number_input("New Stop-Loss", min_value=0.0, value=order["stop_loss"])
            new_target = st.number_input("New Target", min_value=0.0, value=order["target"])

            col_mod, col_can = st.columns(2)
            with col_mod:
                if st.button("Modify Scheduled Order"):
                    update_scheduled_order(selected_scheduled_order, new_quantity, new_price, new_trigger_price,
                                           new_schedule_time, new_stop_loss, new_target)
                    st.success("Scheduled order updated")
            with col_can:
                if st.button("Cancel Scheduled Order"):
                    scheduled_orders = [o for o in scheduled_orders if o["order_id"] != selected_scheduled_order]
                    while not scheduled_orders_queue.empty():
                        scheduled_orders_queue.get()
                    for o in scheduled_orders:
                        scheduled_orders_queue.put(o)
                    st.success("Scheduled order cancelled")
    else:
        st.info("No scheduled orders")

    if single_multi_order == 'Single Order':
        with col2:
            st.subheader("Risk Calculator")
            risk_cols = st.columns(3)
            account_size = risk_cols[0].number_input("Account Size (₹)", min_value=1000.0, value=100000.0)
            stock_symbol = risk_cols[1].selectbox("Select Symbol", options=instruments.keys(), key=f"risk_symbol")
            instrument_token = instruments.get(stock_symbol)
            entry_price = risk_cols[2].number_input("Entry Price (₹)", min_value=0.0, value=100.0)
            trade_size = int((account_size/4)/entry_price)
            risk_type = risk_cols[0].radio("Risk Input Type", ["Percentage", "Amount"])
            if risk_type == "Percentage":
                risk_percent = risk_cols[1].slider("Risk Percentage", 0.1, 5.0, 1.0)
                risk_amount = account_size * (risk_percent / 100)
            else:
                risk_amount = risk_cols[1].number_input("Risk Amount (₹)", min_value=0.0, value=1000.0)
                risk_percent = (risk_amount / account_size) * 100

            profit_type = risk_cols[0].radio("Profit Target", ["Percentage", "Price"])
            if profit_type == "Percentage":
                profit_percent = risk_cols[1].slider("Profit Percentage", 0.1, 20.0, 5.0)
                target_price = entry_price * (1 + profit_percent / 100)
            else:
                target_price = risk_cols[1].number_input("Target Price (₹)", min_value=entry_price, value=entry_price + 5.0)
                profit_percent = ((target_price - entry_price) / entry_price) * 100

            product_type = st.selectbox("Product Type", options=['D', 'I'])

            # Calculate stop loss
            price_per_share_risk = risk_amount / trade_size
            stop_loss = entry_price - price_per_share_risk if transaction_type == "BUY" else entry_price + price_per_share_risk

            if st.button("Calculate Risk & Profit"):
                brokerage = calculate_brokerage(apis["charges"], instrument_token, trade_size,
                                                entry_price, "BUY", product_type)
                profit_amount = (target_price - entry_price) * trade_size - brokerage

                st.markdown(f'<div class="metric-box">Risk Amount: ₹{risk_amount:.2f} ({risk_percent:.2f}%)</div>',
                            unsafe_allow_html=True)
                st.markdown(f'<div class="metric-box">Stop Loss Price: ₹{stop_loss:.2f}</div>', unsafe_allow_html=True)
                st.markdown(f'<div class="metric-box">Target Price: ₹{target_price:.2f}</div>', unsafe_allow_html=True)
                st.markdown(f'<div class="metric-box">Position Size: {trade_size} shares</div>', unsafe_allow_html=True)
                st.markdown(f'<div class="metric-box">Expected Profit: ₹{profit_amount:.2f} ({profit_percent:.2f}%)</div>',
                            unsafe_allow_html=True)
                st.markdown(f'<div class="metric-box">Brokerage: ₹{brokerage:.2f}</div>', unsafe_allow_html=True)

# Order History Page (Enhanced Order Book)
elif page == "Order Book":
    st.subheader("Order History")
    orders_data = apis["order"].get_order_book(api_version="v2").data
    if orders_data:

        # Filters
        col1, col2, col3 = st.columns(3)
        with col1:
            status_filter = st.multiselect(
                "Filter by Status",
                options=["PENDING", "COMPLETED", "REJECTED", "CANCELLED", "MODIFIED"],
                default=[]
            )
        with col2:
            transaction_filter = st.multiselect(
                "Filter by Transaction Type",
                options=["BUY", "SELL"],
                default=[]
            )
        with col3:
            date_range = st.date_input(
                "Date Range",
                value=(
                    datetime.now() - timedelta(days=7),
                    datetime.now()
                )
            )
        # Create DataFrame for better display
        orders = [item.to_dict() for item in orders_data]
        orders_df = pd.DataFrame([
            {
                "Order ID": order.get("order_id", ""),
                "Symbol": order.get("trading_symbol", ""),
                "Exchange": order.get("exchange", ""),
                "Trans. Type": order.get("transaction_type", ""),
                "Order Type": order.get("order_type", ""),
                "Product": order.get("product", ""),
                "Quantity": order.get("quantity", 0),
                "Status": order.get("status", ""),
                "Price": order.get("price", 0),
                "Trigger Price": order.get("trigger_price", 0),
                "Avg. Price": order.get("average_price", 0),
                "Filled Qty": order.get("filled_quantity", 0),
                "Order Time": order.get("order_timestamp", ""),
                "Remarks": order.get("status_message", "")
            } for order in orders
        ])

        # Allow sorting
        st.dataframe(orders_df.sort_values(by="Order Time", ascending=False))

        col1, col2, _, col3 = st.columns([1, .75, .75, .36], vertical_alignment='bottom')
        # Select order for modification/cancellation
        selected_order_id = col1.selectbox(
            "Select Order to Modify/Cancel",
            options=orders_df["Order ID"][~orders_df["Status"].isin(
                ['complete', 'rejected', 'cancelled', 'cancelled after market order'])].tolist()
        )

        if selected_order_id:
            order_data = next((o for o in orders if o.get("order_id") == selected_order_id), None)

            with col2:
                # Cancel order button
                if order_data.get("status") not in ["complete", 'rejected',
                                                    'cancelled', 'cancelled after market order']:

                    if st.button("Cancel Order"):
                        response = apis["order"].cancel_order(selected_order_id, api_version="2.0").data

                        if response:
                            st.success(
                                f"Order cancelled successfully! {response}")
                        else:
                            st.error("Failed to cancel order. Please try again.")

            with col3:
                # Cancel all orders
                if st.button("Cancel All Orders", type='primary'):
                    response = apis["order"].cancel_multi_order().data

                    if response:
                        st.toast(
                            f"Order cancelled successfully! {response}")
                    else:
                        st.toast("Failed to cancel order. Please try again.")

            if order_data:
                # Display order details
                modify_cols = st.columns(2)
                with modify_cols[1]:
                    with st.expander("Order Details"):
                        row_cols = st.columns(2)
                        with row_cols[0]:
                            st.write(f"Symbol: {order_data.get('trading_symbol')}")
                            st.write(f"Exchange: {order_data.get('exchange')}")
                            st.write(f"Transaction Type: {order_data.get('transaction_type')}")
                            st.write(f"Order Type: {order_data.get('order_type')}")
                            st.write(f"Product: {order_data.get('product')}")
                        with row_cols[1]:
                            st.write(f"Quantity: {order_data.get('quantity')}")
                            st.write(f"Status: {order_data.get('status')}")
                            st.write(f"Price: {format_currency(order_data.get('price', 0))}")
                            st.write(f"Trigger Price: {format_currency(order_data.get('trigger_price', 0))}")

                with modify_cols[0]:
                    if order_data.get("status") not in ["complete", 'rejected',
                                                        'cancelled', 'cancelled after market order']:
                        # Modification form
                        st.subheader("Modify Order")

                        modify_order_cols = st.columns(3)

                        # Fields that can be modified
                        new_quantity = modify_order_cols[0].number_input(
                            "New Quantity",
                            min_value=1,
                            value=order_data.get("quantity", 1)
                        )

                        if order_data.get("order_type") in ["LIMIT", "SL"]:
                            new_price = st.number_input(
                                "New Price",
                                min_value=0.05,
                                step=0.05,
                                format="%.2f",
                                value=float(order_data.get("price", 0))
                            )

                        if order_data.get("order_type") in ["SL", "SL-M"]:
                            new_trigger_price = st.number_input(
                                "New Trigger Price",
                                min_value=0.05,
                                step=0.05,
                                format="%.2f",
                                value=float(order_data.get("trigger_price", 0))
                            )

                        new_disclosed_qty = modify_order_cols[1].number_input(
                            "New Disclosed Quantity",
                            min_value=0,
                            value=order_data.get("disclosed_quantity", 0)
                        )

                        order_type = modify_order_cols[2].selectbox(
                            "Order Type",
                            options=['LIMIT', 'MARKET', 'SL', 'SL-M'],
                            index=['LIMIT', 'MARKET', 'SL', 'SL-M'].index(order_data.get("order_type")),
                            key='modify_order_type'
                        )

                        if st.button("Modify Order"):
                            # Construct modification data
                            modify_data = {
                                "order_id": selected_order_id,
                                "quantity": new_quantity,
                                "disclosed_quantity": new_disclosed_qty if new_disclosed_qty > 0 else None,
                                "validity": 'DAY',
                                "order_type": order_type,
                                "trigger_price": 0,
                                "price": 0
                            }

                            if order_data.get("order_type") in ["LIMIT", "SL"] and "new_price" in locals():
                                modify_data["price"] = new_price

                            if order_data.get("order_type") in ["SL", "SL-M"] and "new_trigger_price" in locals():
                                modify_data["trigger_price"] = new_trigger_price

                            # Call modify order
                            response = apis['order'].modify_order(modify_data, api_version="2.0").data
                            if response:
                                st.success(
                                    f"Order modified successfully! {response}")
                                # st.rerun()  # Refresh to show updated order
                            else:
                                st.error("Failed to modify order. Please check the details and try again.")

    else:
        st.info("No orders found")

# Enhanced Positions Page
elif page == "Positions":
    st.subheader("Current Positions")
    positions = apis["portfolio"].get_positions(api_version="v2").data
    if positions:
        positions_df = pd.DataFrame([item.to_dict() for item in positions])
        # Summary metrics
        total_investment = positions_df['buy_value'].sum()
        total_pnl = positions_df['pnl'].sum() if 'pnl' in positions_df.columns else 0
        total_value = positions_df['value'].sum()

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Investment", f"₹{total_investment:.2f}")
        col2.metric("Total P&L", f"₹{total_pnl:.2f}",
                    f"{(total_pnl / total_investment * 100 if total_investment else 0):.2f}%")
        col3.metric("Current Value", f"₹{total_value:.2f}")

        # Display positions table
        st.dataframe(positions_df, use_container_width=True)

        # Square off buttons
        st.subheader("Position Actions")
        for idx, row in positions_df.iterrows():
            col1, col2 = st.columns([3, 1])
            col1.write(f"{row['trading_symbol']} - {row['quantity']} @ {row['average_price']} ({row['product']})")
            if col2.button("Square Off", key=f"squareoff_{idx}"):
                try:
                    # Create a square off order
                    order_response = place_order(
                        apis['order'],
                        instrument_token=row['instrument_token'],
                        quantity=abs(row['quantity']),
                        price=0,  # Market order
                        order_type="MARKET",
                        transaction_type="SELL" if row['quantity'] > 0 else "BUY",
                        product_type=row['product']
                    )
                    st.write(order_response)
                    if order_response and order_response.status == "success":
                        st.success(f"Square off order placed! Order ID: {order_response.data.order_id}")
                    else:
                        st.error("Failed to place square off order.")
                except Exception as e:
                    st.error(f"Error placing square off order: {str(e)}")

# Portfolio Page
elif page == "Portfolio":
    st.subheader("Portfolio Overview")
    portfolio = apis["portfolio"].get_holdings(api_version="v2").data
    if portfolio:
        df = pd.DataFrame([item.to_dict() for item in portfolio])
        total_value = sum(float(holding.last_price) * float(holding.quantity) for holding in portfolio)
        st.dataframe(df[["trading_symbol", "quantity", "last_price", "average_price", "pnl"]])
        st.metric('Total Portfolio Value', f'₹ {total_value:.2f}')


# Analytics Page with TradingView Chart & Indicators
elif page == "Analytics":
    st.write("Trade Analytics & Live Feed")
    analytics_cols = st.columns(5)
    stock_symbol = analytics_cols[0].selectbox("Select Symbol", options=instruments.keys(), key='symbol_analysis')
    instrument_token = instruments.get(stock_symbol)
    timeframe = analytics_cols[1].selectbox("Timeframe", ["1minute", "day", "week", "month", "30minute"], index=1)
    ema_period = analytics_cols[2].number_input("EMA Period", min_value=5, value=20, max_value=200)
    lr_period = analytics_cols[3].number_input("LR Period", min_value=5, value=20, max_value=200)
    rsi_period = analytics_cols[4].number_input("RSI Period", min_value=5, value=14, max_value=50)
    # days = st.slider("Historical Days", 1, 365, 30)

    # Chart Options
    period_cols = analytics_cols[2].columns(3)
    show_columns = st.columns(5)
    show_sr = show_columns[0].checkbox("Show Support & Resistance")
    show_trend = show_columns[1].checkbox("Show Trend Lines")
    show_ema = show_columns[2].checkbox("Show EMA")
    show_lr = show_columns[3].checkbox("Show Linear Regression")
    show_rsi = show_columns[4].checkbox("Show RSI")

    # Historical Data & TradingView Chart
    data = get_historical_data(apis["history"], instrument_token, timeframe)
    print(data.head())
    if data is not None:
        chart = StreamlitChart(height=600, toolbox=True, scale_candles_only=True)
        chart_data = data.rename(
            columns={"timestamp": "time", "open": "open", "high": "high", "low": "low", "close": "close"})
        chart_data.sort_values(by='time', inplace=True)
        chart.set(chart_data)
        chart.legend(True, color_based_on_candle=True, font_size=22, font_family='sans-serif')
        chart.watermark(stock_symbol)

        # Support & Resistance
        if show_sr:
            support = data["low"].min()
            resistance = data["high"].max()
            chart.horizontal_line(support, color="green", style='dashed', text="Support")
            chart.horizontal_line(resistance, color="red", style='dashed', text="Resistance")

        # Trend Lines
        if show_trend:
            trend_start = {"time": data["timestamp"].iloc[0], "value": data["close"].iloc[0]}
            trend_end = {"time": data["timestamp"].iloc[60], "value": data["close"].iloc[60]}
            chart.trend_line(trend_start['time'], trend_start['value'], trend_end['time'], trend_end['value'], line_color="blue")

        # EMA (Displayed as marker on latest value)
        if show_ema:
            ema = calculate_ema(data, ema_period)
            latest_ema = ema.iloc[-1]
            chart.marker(data["timestamp"].iloc[-1], color="orange", text="EMA")
            st.write(f"Latest EMA ({ema_period}): {latest_ema:.2f}")

        # Linear Regression (Displayed as marker on latest value)
        if show_lr:
            lr = calculate_linear_regression(data, lr_period)
            latest_lr = lr.iloc[-1]
            chart.marker(data["timestamp"].iloc[-1], color="purple", text='LR')
            st.write(f"Latest Linear Regression ({lr_period}): {latest_lr:.2f}")

        # RSI Indicator
        if show_rsi:
            rsi = calculate_rsi(data, rsi_period)
            rsi_data = pd.DataFrame({
                "time": data["timestamp"],
                "rsi": rsi
            }).dropna()
            rsi_line = chart.create_line(name="RSI", color="blue")
            rsi_line.set(rsi_data)
            latest_rsi = rsi.iloc[-1]
            st.write(f"Latest RSI ({rsi_period}): {latest_rsi:.2f}")

        chart.load()

# Add this new page option
elif page == "Algo Trading":
    st.subheader("Algorithmic Trading")

    # Strategy selection and parameters
    col1, col2 = st.columns(2)

    with col1:
        strategy = st.selectbox("Select Strategy", [
            "MACD Crossover",
            "Bollinger Bands",
            "RSI Oversold/Overbought",
            "Stochastic Oscillator",
            "Support/Resistance Breakout"
        ])

        stock_symbol = st.selectbox("Select Symbol", options=instruments.keys(), key='symbol_algo')
        instrument_token = instruments.get(stock_symbol)
        quantity = st.number_input("Quantity", min_value=1, value=1)

        # Strategy-specific parameters
        if strategy == "MACD Crossover":
            fast_period = st.number_input("Fast EMA Period", min_value=3, value=12)
            slow_period = st.number_input("Slow EMA Period", min_value=5, value=26)
            signal_period = st.number_input("Signal Period", min_value=3, value=9)

        elif strategy == "Bollinger Bands":
            bb_period = st.number_input("Bollinger Band Period", min_value=5, value=20)
            num_std = st.number_input("Number of Standard Deviations", min_value=1.0, value=2.0)

        elif strategy == "RSI Oversold/Overbought":
            rsi_period = st.number_input("RSI Period", min_value=5, value=14)
            overbought = st.number_input("Overbought Level", min_value=50, max_value=100, value=70)
            oversold = st.number_input("Oversold Level", min_value=0, max_value=50, value=30)

        elif strategy == "Stochastic Oscillator":
            k_period = st.number_input("K Period", min_value=5, value=14)
            d_period = st.number_input("D Period", min_value=3, value=3)

        elif strategy == "Support/Resistance Breakout":
            lookback = st.number_input("Lookback Period", min_value=5, value=20)

    with col2:
        st.subheader("Risk Management")
        stop_loss = st.number_input("Stop Loss (%)", min_value=0.1, value=1.0, max_value=10.0)
        take_profit = st.number_input("Take Profit (%)", min_value=0.1, value=2.0, max_value=20.0)

        st.subheader("Execution Settings")
        execution_type = st.radio("Execution Type", ["Manual", "Automatic"])

        if execution_type == "Automatic":
            interval = st.number_input("Check Interval (minutes)", min_value=1, value=5)
            start_hour = st.number_input("Market Start Hour", min_value=0, max_value=23, value=9)
            start_min = st.number_input("Market Start Minute", min_value=0, max_value=59, value=15)
            end_hour = st.number_input("Market End Hour", min_value=0, max_value=23, value=15)
            end_min = st.number_input("Market End Minute", min_value=0, max_value=59, value=30)

    # Strategy description
    st.subheader("Strategy Description")
    if strategy == "MACD Crossover":
        st.markdown("""
        **MACD Crossover Strategy**

        The MACD (Moving Average Convergence Divergence) strategy generates signals when the MACD line crosses over the signal line.
        - Buy signal: When MACD line crosses above the signal line
        - Sell signal: When MACD line crosses below the signal line

        Parameters:
        - Fast EMA Period: Period for the faster exponential moving average
        - Slow EMA Period: Period for the slower exponential moving average
        - Signal Period: Smoothing period for the MACD line
        """)
    elif strategy == "Bollinger Bands":
        st.markdown("""
        **Bollinger Bands Strategy**

        Uses Bollinger Bands to identify overbought and oversold conditions.
        - Buy signal: When price breaks below the lower band
        - Sell signal: When price breaks above the upper band

        Parameters:
        - Period: Lookback period for calculating the moving average
        - Standard Deviations: Multiplier for standard deviation to set band width
        """)
    elif strategy == "RSI Oversold/Overbought":
        st.markdown("""
        **RSI Strategy**

        Uses the Relative Strength Index to identify overbought and oversold conditions.
        - Buy signal: When RSI crosses above the oversold level from below
        - Sell signal: When RSI crosses below the overbought level from above

        Parameters:
        - RSI Period: Lookback period for calculating RSI
        - Overbought Level: RSI value above which the asset is considered overbought
        - Oversold Level: RSI value below which the asset is considered oversold
        """)

    # Action buttons
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Check Current Signal"):
            # Get current signal based on selected strategy
            with st.spinner("Analyzing market data..."):
                hist_data = get_historical_data(apis["history"], instrument_token)

                if hist_data is not None:
                    signal = None

                    if strategy == "MACD Crossover":
                        macd_line, signal_line, _ = calculate_macd(hist_data, fast_period, slow_period, signal_period)
                        signal = check_macd_crossover(macd_line, signal_line)

                    elif strategy == "Bollinger Bands":
                        _, upper_band, lower_band = calculate_bollinger_bands(hist_data, bb_period, num_std)
                        signal = check_bollinger_band_signals(hist_data, upper_band, lower_band)

                    elif strategy == "RSI Oversold/Overbought":
                        rsi = calculate_rsi(hist_data, rsi_period)
                        if rsi.iloc[-1] < oversold:
                            signal = "BUY"
                        elif rsi.iloc[-1] > overbought:
                            signal = "SELL"

                    elif strategy == "Stochastic Oscillator":
                        k, d = calculate_stochastic_oscillator(hist_data, k_period, d_period)
                        signal = check_stochastic_signals(k, d)

                    elif strategy == "Support/Resistance Breakout":
                        signal = check_support_resistance_breakout(hist_data, lookback)

                    if signal:
                        st.success(f"Current Signal: {signal}")
                    else:
                        st.info("No signal detected at the current time.")
                else:
                    st.error("Failed to fetch historical data.")

    with col2:
        if execution_type == "Manual":
            if st.button("Execute Strategy Now"):
                with st.spinner("Executing strategy..."):
                    result = auto_trade(apis["order"], strategy, instrument_token, quantity, stop_loss, take_profit)
                    st.success(result)
        else:
            if st.button("Start Automated Trading"):
                run_hours = [(start_hour, end_hour)]
                result = schedule_strategy_execution(apis["order"], strategy, instrument_token, quantity, interval,
                                                     run_hours)
                st.success(
                    "Automated trading started. The system will check for signals every {} minutes during market hours.".format(
                        interval))
                st.warning(
                    "Warning: Automated trading will continue until the application is closed or you navigate away from this page.")

# Add this new page option
elif page == "Strategy Backtest":
    st.subheader("Strategy Backtesting")

    stock_symbol = st.selectbox("Select Symbol", options=instruments.keys(), key='symbol_backtest')
    instrument_token = instruments.get(stock_symbol)
    timeframe = st.selectbox("Timeframe", ["day", "week", "1minute", "5minute", "30minute"], index=0)

    # Strategy selection
    strategy = st.selectbox("Select Strategy to Backtest", [
        "Short Sell Optimization",
        "MACD Crossover",
        "Bollinger Bands",
        "RSI Strategy",
    ])

    # Strategy-specific parameters
    if strategy == "Short Sell Optimization":
        st.write("Backtest and optimize a short-selling strategy using ATR-based stop-loss and target.")

        stocks = ['GOLDBEES', 'JUNIORBEES', 'ICICIB22', 'CPSEETF', 'ITBEES', 'MID150BEES', 'MON100', 'MAFANG', 'HDFCSML250']
        selected_stocks = st.multiselect("Select Stocks to Backtest", stocks, default=stocks[:2])

        initial_investment = st.number_input("Initial Investment (Rs.)", min_value=1000, value=50000, step=1000)
        stop_loss_atr_mult_range = st.slider("Stop Loss ATR Multiplier", 1.0, 4.0, (1.5, 2.5), step=0.5)
        target_atr_mult_range = st.slider("Target ATR Multiplier", 1.0, 7.0, (4.0, 6.0), step=0.5)

        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", value=pd.to_datetime("2020-01-01"))
        with col2:
            end_date = st.date_input("End Date", value=pd.to_datetime("2026-01-01"))

    elif strategy == "MACD Crossover":
        fast_period = st.number_input("Fast EMA Period", min_value=3, value=12)
        slow_period = st.number_input("Slow EMA Period", min_value=5, value=26)
        signal_period = st.number_input("Signal Period", min_value=3, value=9)
        strategy_params = {"fast_period": fast_period, "slow_period": slow_period, "signal_period": signal_period}
        strategy_func = macd_strategy

    elif strategy == "Bollinger Bands":
        bb_period = st.number_input("Bollinger Band Period", min_value=5, value=20)
        num_std = st.number_input("Number of Standard Deviations", min_value=1.0, value=2.0)
        strategy_params = {"period": bb_period, "num_std": num_std}
        strategy_func = bollinger_band_strategy

    elif strategy == "RSI Strategy":
        rsi_period = st.number_input("RSI Period", min_value=5, value=14)
        overbought = st.number_input("Overbought Level", min_value=50, max_value=100, value=70)
        oversold = st.number_input("Oversold Level", min_value=0, max_value=50, value=30)
        strategy_params = {"period": rsi_period, "overbought": overbought, "oversold": oversold}
        strategy_func = rsi_strategy

    if st.button("Run Backtest"):
        with st.spinner("Running backtest..."):

            if strategy == 'Short Sell Optimization':
                stop_loss_atr_mult_values = [x for x in
                                             np.arange(stop_loss_atr_mult_range[0], stop_loss_atr_mult_range[1] + 0.5,
                                                       0.5)]
                target_atr_mult_values = [x for x in
                                          np.arange(target_atr_mult_range[0], target_atr_mult_range[1] + 0.5, 0.5)]
                initial_investment_range = [initial_investment]

                optimized_results = {}
                date_lists = {}

                for stock in selected_stocks:
                    query = f"Select * from dbo.{stock} where date between '{start_date} 00:00:00.000' and '{end_date} 00:00:00.000' order by Date ASC"
                    data = rd.get_table_data(query=query)
                    if not data.empty:
                        df = pd.DataFrame(data)
                        date_lists[stock] = df['Date']
                        optimized_results[stock] = backtest_etf.optimize_parameters(
                            data, stock, initial_investment_range, stop_loss_atr_mult_values, target_atr_mult_values
                        )
                    else:
                        st.error(f"No data found for {stock}")

                if optimized_results:
                    for stock, result in optimized_results.items():
                        st.write(f"### Optimized Results for {stock}")
                        st.write(f"**Initial Investment:** Rs. {result['Initial Investment']}")
                        st.write(f"**Stop Loss ATR Multiplier:** {result['stop_loss_atr_mult']:.1f}x")
                        st.write(f"**Target ATR Multiplier:** {result['target_atr_mult']:.1f}x")
                        st.write(f"**Final Portfolio Value:** Rs. {result['Final Portfolio Value']:.2f}")
                        st.write(f"**Total Profit:** Rs. {result['Total Profit']:.2f}")
                        st.write(f"**Win Rate:** {result['Win Rate']:.2f}%")
                        st.write(f"**Loss Rate:** {result['Loss Rate']:.2f}%")
                        st.write(f"**Total Trades:** {result['Total Trades']}")
                        st.write(f"**Winning Trades:** {result['Winning Trades']}")
                        st.write(f"**Losing Trades:** {result['Losing Trades']}")
                        st.write("#### Yearly Summary")
                        st.dataframe(result['Yearly Summary'])

                    st.write("### Portfolio Value Over Time")
                    chart_data = pd.DataFrame()
                    for stock, result in optimized_results.items():
                        dates = date_lists[stock]
                        portfolio_values = result['Portfolio Value']
                        if len(dates) != len(portfolio_values):  # Debug check
                            st.warning(
                                f"Length mismatch for {stock}: Dates ({len(dates)}) vs Portfolio ({len(portfolio_values)})")
                        df = pd.DataFrame({
                            'Date': dates,
                            'Portfolio Value': portfolio_values,
                            'Stock': [stock] * len(dates)
                        })
                        chart_data = pd.concat([chart_data, df], ignore_index=True)

                    if not chart_data.empty:
                        chart = alt.Chart(chart_data).mark_line().encode(
                            x='Date:T',
                            y='Portfolio Value:Q',
                            color='Stock:N',
                            tooltip=['Date:T', 'Portfolio Value:Q', 'Stock:N']
                        ).properties(
                            width=800,
                            height=400,
                            title='Portfolio Value Over Time (ATR-Based Optimization)'
                        ).interactive()

                        st.altair_chart(chart, use_container_width=True)

                    for stock, result in optimized_results.items():
                        csv = result['Tradebook'].to_csv(index=False)
                        st.download_button(
                            label=f"Download Tradebook for {stock}",
                            data=csv,
                            file_name=f"{stock}_tradebook.csv",
                            mime="text/csv"
                        )

                    st.dataframe(result['Tradebook'])
                else:
                    st.warning("No results to display. Check data availability.")

            else:
                # Fetch historical data
                hist_data = get_historical_data(apis["history"], instrument_token, timeframe, sort_data=True)

                if hist_data is not None:
                    # Run backtest
                    backtest_results = backtest_strategy(hist_data, strategy_func, **strategy_params)

                    # Display results
                    st.subheader("Backtest Results")

                    # Summary statistics
                    total_trades = backtest_results['signal'].value_counts().sum()
                    profitable_trades = len(backtest_results[backtest_results['pnl'] > 0])
                    win_rate = profitable_trades / total_trades * 100 if total_trades > 0 else 0

                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total Trades", total_trades)
                    col2.metric("Win Rate", f"{win_rate:.2f}%")
                    col3.metric("Total P&L", f"₹{backtest_results['cumulative_pnl'].iloc[-1]:.2f}")

                    # Plot performance
                    st.subheader("Performance Chart")
                    chart_data = pd.DataFrame({
                        'Date': backtest_results['timestamp'],
                        'Close Price': backtest_results['close'],
                        'Cumulative P&L': backtest_results['cumulative_pnl']
                    })

                    # Show signals on the chart
                    buy_signals = backtest_results[backtest_results['signal'] == 'BUY']
                    sell_signals = backtest_results[backtest_results['signal'] == 'SELL']

                    import altair as alt

                    # Price chart with signals
                    price_chart = alt.Chart(chart_data).mark_line().encode(
                        x='Date:T',
                        y=alt.Y('Close Price:Q', scale=alt.Scale(zero=False))
                    )

                    # Add buy signals
                    buy_points = alt.Chart(buy_signals).mark_point(
                        color='green',
                        size=100,
                        shape='triangle-up'
                    ).encode(
                        x='timestamp:T',
                        y='close:Q'
                    )

                    # Add sell signals
                    sell_points = alt.Chart(sell_signals).mark_point(
                        color='red',
                        size=100,
                        shape='triangle-down'
                    ).encode(
                        x='timestamp:T',
                        y='close:Q'
                    )

                    # P&L chart
                    pnl_chart = alt.Chart(chart_data).mark_line(color='purple').encode(
                        x='Date:T',
                        y='Cumulative P&L:Q'
                    )

                    # Display charts
                    st.altair_chart(price_chart + buy_points + sell_points, use_container_width=True)
                    st.altair_chart(pnl_chart, use_container_width=True)

                    # Show detailed trades
                    st.subheader("Trade Details")
                    trades_df = backtest_results[backtest_results['signal'].notnull()].copy()
                    trades_df = trades_df[['timestamp', 'signal', 'close', 'pnl']]
                    trades_df.columns = ['Date', 'Signal', 'Price', 'P&L']
                    st.dataframe(trades_df)
                else:
                    st.error("Failed to fetch historical data for backtesting.")

# Footer
st.markdown(f'<div class="metric-box">Last Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>',
            unsafe_allow_html=True)
