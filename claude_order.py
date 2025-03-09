import streamlit as st
import pandas as pd
import numpy as np
import datetime
import json
import os
import time
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from lightweight_charts import Chart
import matplotlib.pyplot as plt
import yfinance as yf
from pprint import pprint
import logging
import threading
import requests
from io import StringIO
import sys
from pathlib import Path
from matplotlib.figure import Figure
from ta.trend import SMAIndicator, EMAIndicator, MACD
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.volatility import BollingerBands
from ta.volume import VolumeWeightedAveragePrice

# Configure upstox client
import upstox_client
from upstox_client.rest import ApiException

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
CONFIG_FILE = "upstox_config.json"
DEFAULT_TIMEFRAME = "1D"
TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h", "1D", "1W", "1M"]
ORDER_TYPES = ["MARKET", "LIMIT", "SL", "SL-M"]
PRODUCT_TYPES = ["I", "D", "CO", "OCO"]
DURATION_TYPES = ["DAY", "IOC", "GTC"]
EXCHANGE_CODES = ["NSE", "BSE", "NFO", "CDS", "MCX"]
POSITION_TYPES = ["NET", "OPEN", "CLOSED"]
WATCHLIST_FILE = "watchlist.json"
ORDER_HISTORY_FILE = "order_history.json"


class UpstoxTradingApp:
    def __init__(self):
        self.authenticated = False
        self.api_client = None
        self.user_api = None
        self.order_api = None
        self.portfolio_api = None
        self.market_quote_api = None
        self.user_data = None
        self.access_token = None
        self.watchlist = self.load_watchlist()
        self.order_history = self.load_order_history()
        self.current_symbol = None
        self.current_exchange = "NSE"
        self.current_timeframe = DEFAULT_TIMEFRAME
        self.symbols_data = {}
        self.streaming_thread = None
        self.streaming_active = False
        self.current_strategy = None
        self.indicators = {}
        self.alert_levels = {}

    def load_config(self):
        """Load configuration from file"""
        try:
            if not os.path.exists(CONFIG_FILE):
                return None
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            return None

    def save_config(self, config_data):
        """Save configuration to file"""
        try:
            with open(CONFIG_FILE, 'w') as f:
                json.dump(config_data, f, indent=4)
            return True
        except Exception as e:
            logger.error(f"Error saving configuration: {e}")
            return False

    def load_watchlist(self):
        """Load watchlist from file"""
        try:
            if not os.path.exists(WATCHLIST_FILE):
                return []
            with open(WATCHLIST_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading watchlist: {e}")
            return []

    def save_watchlist(self):
        """Save watchlist to file"""
        try:
            with open(WATCHLIST_FILE, 'w') as f:
                json.dump(self.watchlist, f, indent=4)
            return True
        except Exception as e:
            logger.error(f"Error saving watchlist: {e}")
            return False

    def load_order_history(self):
        """Load order history from file"""
        try:
            if not os.path.exists(ORDER_HISTORY_FILE):
                return []
            with open(ORDER_HISTORY_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading order history: {e}")
            return []

    def save_order_history(self):
        """Save order history to file"""
        try:
            with open(ORDER_HISTORY_FILE, 'w') as f:
                json.dump(self.order_history, f, indent=4)
            return True
        except Exception as e:
            logger.error(f"Error saving order history: {e}")
            return False

    def authenticate(self, api_key, api_secret, redirect_uri, api_version="v2"):
        """Authenticate with Upstox API"""
        try:
            auth_url = (f"https://api.upstox.com/v2/login/authorization/dialog?"
                        f"client_id={api_key}&"
                        f"redirect_uri={redirect_uri}&"
                        f"response_type=code")

            self.api_key = api_key
            self.api_secret = api_secret
            self.redirect_uri = redirect_uri
            return auth_url
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return None

    def get_access_token(self, api_key, api_secret, redirect_uri, auth_code):
        """Get access token using authorization code"""
        try:
            token_url = "https://api.upstox.com/v2/login/authorization/token"
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json'
            }
            payload = {
                "client_id": api_key,
                "client_secret": api_secret,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
                "code": auth_code
            }

            response = requests.post(token_url, data=payload, headers=headers)
            response.raise_for_status()

            token_data = response.json()
            self.access_token = token_data.get("access_token")

            if not self.access_token:
                logger.error("No access token in response")
                return False

            config_data = {
                "api_key": api_key,
                "api_secret": api_secret,
                "redirect_uri": redirect_uri,
                "access_token": self.access_token
            }
            self.save_config(config_data)

            self.initialize_api_clients()
            self.authenticated = True
            return True
        except Exception as e:
            logger.error(f"Error obtaining access token: {e}")
            return False

    def initialize_api_clients(self):
        """Initialize API clients after authentication"""
        try:
            if not self.access_token:
                return False

            # Initialize API client without Configuration object
            self.api_client = upstox_client.ApiClient()
            self.api_client.configuration.host = "https://api.upstox.com/v2"
            self.api_client.configuration.access_token = self.access_token

            # Set default headers
            self.api_client.set_default_header('Authorization', f'Bearer {self.access_token}')
            self.api_client.set_default_header('Content-Type', 'application/json')
            self.api_client.set_default_header('Accept', 'application/json')

            # Initialize API endpoints
            self.user_api = upstox_client.UserApi(self.api_client)
            self.order_api = upstox_client.OrderApi(self.api_client)
            self.portfolio_api = upstox_client.PortfolioApi(self.api_client)
            self.market_quote_api = upstox_client.MarketQuoteApi(self.api_client)

            # Verify authentication
            self.user_data = self.user_api.get_profile()
            logger.info("API clients initialized successfully")
            return True
        except ApiException as e:
            logger.error(f"Exception when verifying authentication: {e}")
            return False
        except Exception as e:
            logger.error(f"Error initializing API clients: {e}")
            return False

    def get_holdings(self):
        """Get user holdings"""
        try:
            if not self.authenticated:
                return None
            response = self.portfolio_api.get_holdings()
            return response.to_dict()
        except ApiException as e:
            logger.error(f"Exception when calling PortfolioApi->get_holdings: {e}")
            return None

    def get_positions(self, position_type="NET"):
        """Get user positions"""
        try:
            if not self.authenticated:
                return None
            response = self.portfolio_api.get_positions({"type": position_type})
            return response.to_dict()
        except ApiException as e:
            logger.error(f"Exception when calling PortfolioApi->get_positions: {e}")
            return None

    def get_funds(self):
        """Get user funds"""
        try:
            if not self.authenticated:
                return None
            response = self.user_api.get_funds()
            return response.to_dict()
        except ApiException as e:
            logger.error(f"Exception when calling UserApi->get_funds: {e}")
            return None

    def search_instruments(self, exchange, symbol_name):
        """Search for instruments"""
        try:
            if not self.authenticated:
                return None
            response = self.market_quote_api.search_instruments(exchange, {"instrument_name": symbol_name})
            return response.to_dict()
        except ApiException as e:
            logger.error(f"Exception when calling MarketQuoteApi->search_instruments: {e}")
            return None

    def get_market_quote(self, instrument_tokens, mode="full"):
        """Get market quotes for instruments"""
        try:
            if not self.authenticated:
                return None
            if not isinstance(instrument_tokens, list):
                instrument_tokens = [instrument_tokens]
            request_data = {"instrument_tokens": instrument_tokens}
            if mode == "full":
                response = self.market_quote_api.get_full_quotes(request_data)
            else:
                response = self.market_quote_api.get_ltp_quotes(request_data)
            return response.to_dict()
        except ApiException as e:
            logger.error(f"Exception when calling MarketQuoteApi->get_quotes: {e}")
            return None

    def get_historical_data(self, instrument_token, interval, from_date, to_date):
        """Get historical candle data"""
        try:
            if not self.authenticated:
                return None
            request_data = {
                "instrument_token": instrument_token,
                "interval": interval,
                "to_date": to_date,
                "from_date": from_date
            }
            response = self.market_quote_api.get_historical_candle_data(request_data)
            return response.to_dict()
        except ApiException as e:
            logger.error(f"Exception when calling MarketQuoteApi->get_historical_candle_data: {e}")
            return None

    def get_last_traded_price(self, instrument_token):
        """Get last traded price for an instrument"""
        try:
            if not self.authenticated:
                return None
            request_data = {"instrument_tokens": [instrument_token]}
            response = self.market_quote_api.get_ltp_quotes(request_data)
            return response.to_dict()
        except ApiException as e:
            logger.error(f"Exception when calling MarketQuoteApi->get_ltp_quotes: {e}")
            return None

    def place_order(self, order_data):
        """Place an order"""
        try:
            if not self.authenticated:
                return None
            response = self.order_api.place_order(order_data)
            history_entry = {
                "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": "PLACE",
                "order_data": order_data,
                "response": response.to_dict()
            }
            self.order_history.append(history_entry)
            self.save_order_history()
            return response.to_dict()
        except ApiException as e:
            logger.error(f"Exception when calling OrderApi->place_order: {e}")
            history_entry = {
                "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": "PLACE",
                "order_data": order_data,
                "error": str(e)
            }
            self.order_history.append(history_entry)
            self.save_order_history()
            return None

    def modify_order(self, order_id, order_data):
        """Modify an existing order"""
        try:
            if not self.authenticated:
                return None

            response = self.order_api.modify_order(order_id, order_data)

            # Add to order history
            history_entry = {
                "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": "MODIFY",
                "order_id": order_id,
                "order_data": order_data,
                "response": response.to_dict()
            }
            self.order_history.append(history_entry)
            self.save_order_history()

            return response.to_dict()
        except ApiException as e:
            logger.error(f"Exception when calling OrderApi->modify_order: {e}")

            # Add failed order to history
            history_entry = {
                "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": "MODIFY",
                "order_id": order_id,
                "order_data": order_data,
                "error": str(e)
            }
            self.order_history.append(history_entry)
            self.save_order_history()

            return None

    def cancel_order(self, order_id):
        """Cancel an order"""
        try:
            if not self.authenticated:
                return None

            response = self.order_api.cancel_order(order_id)

            # Add to order history
            history_entry = {
                "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": "CANCEL",
                "order_id": order_id,
                "response": response.to_dict()
            }
            self.order_history.append(history_entry)
            self.save_order_history()

            return response.to_dict()
        except ApiException as e:
            logger.error(f"Exception when calling OrderApi->cancel_order: {e}")

            # Add failed cancellation to history
            history_entry = {
                "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": "CANCEL",
                "order_id": order_id,
                "error": str(e)
            }
            self.order_history.append(history_entry)
            self.save_order_history()

            return None

    def get_order_book(self):
        """Get order book"""
        try:
            if not self.authenticated:
                return None

            response = self.order_api.get_order_book()
            return response.to_dict()
        except ApiException as e:
            logger.error(f"Exception when calling OrderApi->get_order_book: {e}")
            return None

    def get_trade_book(self):
        """Get trade book"""
        try:
            if not self.authenticated:
                return None

            response = self.order_api.get_trade_book()
            return response.to_dict()
        except ApiException as e:
            logger.error(f"Exception when calling OrderApi->get_trade_book: {e}")
            return None

    def add_to_watchlist(self, symbol_data):
        """Add a symbol to watchlist"""
        if symbol_data not in self.watchlist:
            self.watchlist.append(symbol_data)
            self.save_watchlist()
            return True
        return False

    def remove_from_watchlist(self, instrument_token):
        """Remove a symbol from watchlist"""
        for item in self.watchlist:
            if item.get("instrument_token") == instrument_token:
                self.watchlist.remove(item)
                self.save_watchlist()
                return True
        return False

    def calculate_indicators(self, df):
        """Calculate technical indicators for the dataframe"""
        # Ensure dataframe has necessary columns
        if df.empty or not all(col in df.columns for col in ['open', 'high', 'low', 'close', 'volume']):
            return df

        try:
            # Trend indicators
            # SMA
            df['sma_20'] = SMAIndicator(close=df['close'], window=20).sma_indicator()
            df['sma_50'] = SMAIndicator(close=df['close'], window=50).sma_indicator()
            df['sma_200'] = SMAIndicator(close=df['close'], window=200).sma_indicator()

            # EMA
            df['ema_9'] = EMAIndicator(close=df['close'], window=9).ema_indicator()
            df['ema_21'] = EMAIndicator(close=df['close'], window=21).ema_indicator()

            # MACD
            macd = MACD(close=df['close'], window_slow=26, window_fast=12, window_sign=9)
            df['macd_line'] = macd.macd()
            df['macd_signal'] = macd.macd_signal()
            df['macd_histogram'] = macd.macd_diff()

            # Momentum indicators
            # RSI
            df['rsi_14'] = RSIIndicator(close=df['close'], window=14).rsi()

            # Stochastic
            stoch = StochasticOscillator(high=df['high'], low=df['low'], close=df['close'], window=14, smooth_window=3)
            df['stoch_k'] = stoch.stoch()
            df['stoch_d'] = stoch.stoch_signal()

            # Volatility indicators
            # Bollinger Bands
            bollinger = BollingerBands(close=df['close'], window=20, window_dev=2)
            df['bb_upper'] = bollinger.bollinger_hband()
            df['bb_lower'] = bollinger.bollinger_lband()
            df['bb_middle'] = bollinger.bollinger_mavg()
            df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / df['bb_middle']

            # Volume indicators
            # VWAP
            df['vwap'] = VolumeWeightedAveragePrice(high=df['high'], low=df['low'],
                                                    close=df['close'], volume=df['volume'],
                                                    window=14).volume_weighted_average_price()

            return df
        except Exception as e:
            logger.error(f"Error calculating indicators: {e}")
            return df

    def generate_trading_signals(self, df):
        """Generate trading signals based on indicators"""
        try:
            signals = pd.DataFrame(index=df.index)
            signals['signal'] = 0  # 0: no signal, 1: buy, -1: sell

            # Initialize as NaN to avoid forward-looking bias
            signals['sma_cross'] = np.nan
            signals['macd_cross'] = np.nan
            signals['rsi_signal'] = np.nan
            signals['bb_signal'] = np.nan

            # SMA crossover (fast SMA crosses above slow SMA = buy, vice versa = sell)
            signals['sma_cross'] = np.where(df['sma_20'] > df['sma_50'], 1, -1)

            # MACD crossover (MACD line crosses above signal line = buy, vice versa = sell)
            signals['macd_cross'] = np.where(df['macd_line'] > df['macd_signal'], 1, -1)

            # RSI signals (RSI < 30 = buy, RSI > 70 = sell)
            signals['rsi_signal'] = np.where(df['rsi_14'] < 30, 1, np.where(df['rsi_14'] > 70, -1, 0))

            # Bollinger Bands signals (price crosses below lower band = buy, price crosses above upper band = sell)
            signals['bb_signal'] = np.where(df['close'] < df['bb_lower'], 1,
                                            np.where(df['close'] > df['bb_upper'], -1, 0))

            # Generate final signal based on a combination of indicators
            # Simple strategy: Take majority vote from the 4 signals
            for i in range(1, len(signals)):
                signal_vals = [signals.iloc[i]['sma_cross'], signals.iloc[i]['macd_cross'],
                               signals.iloc[i]['rsi_signal'], signals.iloc[i]['bb_signal']]

                # Count buy and sell signals
                buy_count = sum(1 for x in signal_vals if x == 1)
                sell_count = sum(1 for x in signal_vals if x == -1)

                # Generate signal based on majority vote
                if buy_count > sell_count and buy_count >= 2:
                    signals.iloc[i, 0] = 1  # Buy signal
                elif sell_count > buy_count and sell_count >= 2:
                    signals.iloc[i, 0] = -1  # Sell signal

            return signals
        except Exception as e:
            logger.error(f"Error generating trading signals: {e}")
            return pd.DataFrame()

    def backtest_strategy(self, df, signals, initial_capital=100000):
        """Backtest a trading strategy using historical data and signals"""
        try:
            # Create a dataframe to track positions and portfolio value
            positions = pd.DataFrame(index=signals.index).fillna(0.0)
            positions['position'] = signals['signal'].cumsum()

            # Create a dataframe to track portfolio value
            portfolio = pd.DataFrame(index=signals.index)
            portfolio['positions'] = positions['position'] * df['close']
            portfolio['cash'] = initial_capital - (positions['position'].diff().fillna(0) * df['close']).cumsum()
            portfolio['total'] = portfolio['positions'] + portfolio['cash']
            portfolio['returns'] = portfolio['total'].pct_change()

            # Calculate strategy performance metrics
            total_return = (portfolio['total'][-1] - initial_capital) / initial_capital * 100
            annual_return = total_return / (len(portfolio) / 252) if len(portfolio) > 0 else 0
            sharpe_ratio = np.sqrt(252) * (portfolio['returns'].mean() / portfolio['returns'].std()) if portfolio[
                                                                                                            'returns'].std() != 0 else 0
            max_drawdown = (portfolio['total'] / portfolio['total'].cummax() - 1).min() * 100

            # Count number of trades
            trades = positions['position'].diff().fillna(0)
            num_trades = len(trades[trades != 0])

            results = {
                'total_return': total_return,
                'annual_return': annual_return,
                'sharpe_ratio': sharpe_ratio,
                'max_drawdown': max_drawdown,
                'num_trades': num_trades,
                'portfolio': portfolio,
                'positions': positions
            }

            return results
        except Exception as e:
            logger.error(f"Error backtesting strategy: {e}")
            return None

    def set_price_alert(self, instrument_token, price_level, alert_type="above"):
        """Set a price alert for an instrument"""
        if instrument_token not in self.alert_levels:
            self.alert_levels[instrument_token] = []

        alert = {
            "price": price_level,
            "type": alert_type,  # "above" or "below"
            "triggered": False,
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        self.alert_levels[instrument_token].append(alert)
        return True

    def check_alerts(self, instrument_token, current_price):
        """Check if any alerts should be triggered"""
        if instrument_token not in self.alert_levels:
            return None

        triggered_alerts = []

        for i, alert in enumerate(self.alert_levels[instrument_token]):
            if not alert["triggered"]:
                if (alert["type"] == "above" and current_price >= alert["price"]) or \
                        (alert["type"] == "below" and current_price <= alert["price"]):
                    # Mark alert as triggered
                    self.alert_levels[instrument_token][i]["triggered"] = True
                    triggered_alerts.append(alert)

        return triggered_alerts if triggered_alerts else None


def format_currency(value):
    """Format currency values for display"""
    if isinstance(value, (int, float)):
        return f"₹{value:,.2f}"
    return value


def format_percentage(value):
    """Format percentage values for display"""
    if isinstance(value, (int, float)):
        return f"{value:.2f}%"
    return value


def parse_historical_data(candle_data):
    """Parse historical candle data response into pandas DataFrame"""
    try:
        if not candle_data or "data" not in candle_data or not candle_data["data"].get("candles"):
            return pd.DataFrame()

        candles = candle_data["data"]["candles"]
        data = []

        for candle in candles:
            if len(candle) >= 6:
                data.append({
                    "timestamp": candle[0],
                    "open": float(candle[1]),
                    "high": float(candle[2]),
                    "low": float(candle[3]),
                    "close": float(candle[4]),
                    "volume": float(candle[5])
                })

        df = pd.DataFrame(data)
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df.set_index("timestamp", inplace=True)

        return df
    except Exception as e:
        logger.error(f"Error parsing historical data: {e}")
        return pd.DataFrame()