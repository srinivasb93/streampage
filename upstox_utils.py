import streamlit as st
import upstox_client
from upstox_client.rest import ApiException
import pandas as pd
from datetime import datetime, timedelta, time as date_time
import time
import threading
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv
import os
import numpy as np
import logging
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True)
logger = logging.getLogger(__name__)

# Upstox API Initialization
def init_upstox_api():
    config = upstox_client.Configuration()
    config.access_token = ACCESS_TOKEN
    api_client = upstox_client.ApiClient(config)
    return {
        "order": upstox_client.OrderApi(api_client),
        "portfolio": upstox_client.PortfolioApi(api_client),
        "history": upstox_client.HistoryApi(api_client),
        "charges": upstox_client.ChargeApi(api_client),
        "market_data": upstox_client.MarketQuoteApi(api_client),
        "user": upstox_client.UserApi(api_client)
    }


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
        logger.error(f"Failed to send email: {e}")

def notify(subject, body):
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
            tag="AutoOrder",
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
        logger.error(f"Error placing order: {e}")
        return None

def calculate_atr(df, period=14):
    """Calculate Average True Range (ATR) for the DataFrame."""
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = true_range.rolling(window=period).mean()
    return atr

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
        logger.error(f"Error fetching historical data: {e}")
        return None


def fetch_live_data(market_api=None, instrument_token=None, websocket=False):
    if websocket:
        if is_connected() and instrument_token in get_subscribed_instruments():
            return get_live_data(instrument_token)  # Original WebSocket function from websocket_manager
        else:
            logger.warning("WebSocket not connected or instrument not subscribed. Falling back to REST.")
            return {}
    # Fallback to REST API
    try:
        print(f"Inside Get live - {instrument_token}")
        quote = get_market_quote(market_api, instrument_token)
        return quote

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
        logger.error(f"Error calculating brokerage: {e}")
        return 0
