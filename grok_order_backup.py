import pandas as pd
import streamlit as st
import upstox_client
from datetime import time as date_time
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv
from sqlalchemy.sql import text
import os
import json
import requests
from lightweight_charts.widgets import StreamlitChart
from common_utils.read_write_sql_data import get_table_data, load_sql_data, create_connection
from common_utils import *
from python_scripts.backtest_strategy import backtest_etf
import uuid
import altair as alt
from kiteconnect import KiteConnect
import pyotp


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
# Upstox credentials
UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
ZERODHA_ACCESS_TOKEN = os.getenv("ZERODHA_ACCESS_TOKEN")
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
UPSTOX_API_KEY = os.getenv("UPSTOX_API_KEY")
UPSTOX_API_SECRET = os.getenv("UPSTOX_API_SECRET")
# Zerodha credentials
ZERODHA_API_KEY = os.getenv("ZERODHA_API_KEY")
ZERODHA_API_SECRET = os.getenv("ZERODHA_API_SECRET")
ZERODHA_USERNAME = os.getenv("ZERODHA_USERNAME")
ZERODHA_PASSWORD = os.getenv("ZERODHA_PASSWORD")
ZERODHA_TOTP_TOKEN = os.getenv("ZERODHA_TOTP_TOKEN")

# Zerodha API URLs
ZERODHA_LOGIN_URL = "https://kite.zerodha.com/api/login"
ZERODHA_TWOFA_URL = "https://kite.zerodha.com/api/twofa"

DATABASE = "NSEDATA"

st.set_page_config(page_title="Stock Trading Dashboard", layout="wide")
st.markdown("""
    <style>
    .main { background-color: #f5f5f5; padding: 20px; border-radius: 10px; margin-top: -5em;}
    .stButton>button { background-color: #4CAF50; color: white; border-radius: 5px; }
    .stTextInput>input { border-radius: 5px; }
    .sidebar .sidebar-content { background-color: #e0e0e0; padding: 10px; border-radius: 10px; }
    .metric-box { background-color: #336699; padding: 10px; border-radius: 5px; box-shadow: 2px 2px 5px rgba(0,0,0,0.1); }
    </style>
""", unsafe_allow_html=True)


broker = st.sidebar.selectbox("Select Broker", ["Upstox", "Zerodha"], key="select_broker")

# At the top of your Streamlit file, after imports
if "thread_registry" not in st.session_state:
    st.session_state["thread_registry"] = {}  # {thread_name: {"thread": Thread, "stop_event": Event}}

def init_apis():
    apis = {}

    config = upstox_client.Configuration()
    config.access_token = st.session_state.get("access_token", UPSTOX_ACCESS_TOKEN)
    api_client = upstox_client.ApiClient(config)
    upstox_apis = {
        "order": upstox_client.OrderApi(api_client),
        "portfolio": upstox_client.PortfolioApi(api_client),
        "history": upstox_client.HistoryApi(api_client),
        "charges": upstox_client.ChargeApi(api_client),
        "market_data": upstox_client.MarketQuoteApi(api_client),
        "user": upstox_client.UserApi(api_client)
    }

    kite = KiteConnect(api_key=ZERODHA_API_KEY)
    access_token = st.session_state.get("zerodha_access_token", ZERODHA_ACCESS_TOKEN)
    if access_token:
        kite.set_access_token(access_token)
    kite_apis = {"kite": kite}
    return upstox_apis, kite_apis

upstox_apis, kite_apis = init_apis()

# NEW: Zerodha authentication
def fetch_zerodha_access_token():
    try:
        session = requests.Session()
        response = session.post(ZERODHA_LOGIN_URL, data={'user_id': ZERODHA_USERNAME, 'password': ZERODHA_PASSWORD})
        response.raise_for_status()
        response_data = json.loads(response.text)
        if response_data.get('status') != 'success':
            raise ValueError(f"Login failed: {response_data.get('message', 'Unknown error')}")
        request_id = response_data['data']['request_id']

        twofa_pin = pyotp.TOTP(ZERODHA_TOTP_TOKEN).now()
        response = session.post(ZERODHA_TWOFA_URL, data={
            'user_id': ZERODHA_USERNAME,
            'request_id': request_id,
            'twofa_value': twofa_pin,
            'twofa_type': 'totp'
        })
        response.raise_for_status()
        response_data = json.loads(response.text)
        if response_data.get('status') != 'success':
            raise ValueError(f"TOTP authentication failed: {response_data.get('message', 'Unknown error')}")

        kite = KiteConnect(api_key=ZERODHA_API_KEY)
        kite_url = kite.login_url()
        try:
            session.get(kite_url)
        except Exception as e:
            e_msg = str(e)
            request_token = e_msg.split('request_token=')[1].split(' ')[0].split('&action')[0]
            data = kite.generate_session(request_token, ZERODHA_API_SECRET)
            access_token = data['access_token']
            kite.set_access_token(access_token)
            st.session_state["zerodha_access_token"] = access_token
            with open(".env", "r+") as f:
                lines = f.readlines()
                f.seek(0)
                for line in lines:
                    if not line.startswith("ZERODHA_ACCESS_TOKEN"):
                        f.write(line)
                f.write(f"ZERODHA_ACCESS_TOKEN={access_token}\n")
            return access_token
    except Exception as e:
        st.error(f"Zerodha authentication failed: {str(e)}")
        return None

# Modified: Fetch Upstox access token
def fetch_upstox_access_token(api_key, api_secret, redirect_uri, auth_code):
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
        st.error(f"Failed to fetch Upstox token: {response.text}")
        return None

def manage_scheduled_orders():
    scheduled_orders_df = get_table_data(
        selected_database=DATABASE,
        selected_table="ScheduledOrders",
        query=f"SELECT * FROM NSEDATA.dbo.ScheduledOrders WHERE Status = 'PENDING' AND Broker = '{broker}'"
    )
    scheduled_orders = scheduled_orders_df.to_dict('records') if not scheduled_orders_df.empty else []
    for order in scheduled_orders:
        thread_name = f"ScheduledOrder_{order['ScheduledOrderID']}"
        if thread_name not in st.session_state["thread_registry"]:
            stop_event = threading.Event()
            thread = threading.Thread(
                target=execute_scheduled_order,
                args=(order,),
                daemon=True,
                name=thread_name
            )
            thread.start()
            st.session_state["thread_registry"][thread_name] = {"thread": thread, "stop_event": stop_event}
            logger.info(f"Started new thread for scheduled order {order['ScheduledOrderID']}")
    return scheduled_orders

def execute_scheduled_order(order):
    stop_event = threading.Event()
    thread_name = f"ScheduledOrder_{order['order_id']}"
    logger.info(f"Thread {thread_name} started for order {order['order_id']}")
    while not stop_event.is_set():
        time_to_wait = (order["schedule_datetime"] - datetime.now()).total_seconds()
        if time_to_wait > 0:
            stop_event.wait(min(time_to_wait, 1))
            if stop_event.is_set():
                logger.info(f"Thread {thread_name} stopped before execution")
                break
        else:
            logger.info(f"Thread {thread_name} executing order {order['order_id']} at {datetime.now()}")
            api = upstox_apis["order"] if order["broker"] == "Upstox" else kite_apis["kite"]
            result = place_order(
                api, order["instrument_token"], order["transaction_type"],
                order["quantity"], order["price"], order["order_type"],
                order["trigger_price"], False, order["product"],
                "DAY", order["stop_loss"], order["target"], broker=order["broker"]
            )
            if result:
                update_scheduled_order(order["order_id"], status="EXECUTED", broker=order["broker"])
                logger.info(f"Thread {thread_name} successfully executed order {order['order_id']}")
            else:
                logger.error(f"Thread {thread_name} failed to execute order {order['order_id']}")
            st.session_state["thread_registry"].pop(thread_name, None)
            break
    if stop_event.is_set():
        logger.info(f"Thread {thread_name} stopped")

def recover_session_state():
    scheduled_orders = manage_scheduled_orders()
    if scheduled_orders:
        logger.info(f"Recovered {len(scheduled_orders)} pending scheduled orders for {broker}")
    else:
        logger.info(f"No pending scheduled orders to recover for {broker}")
    auto_orders_df = get_table_data(
        selected_database=DATABASE,
        selected_table="AutoOrders",
        query=f"SELECT * FROM NSEDATA.dbo.AutoOrders WHERE Broker = '{broker}'"
    )
    if not auto_orders_df.empty:
        st.session_state["auto_orders"] = auto_orders_df.to_dict('records')
        logger.info(f"Recovered {len(st.session_state['auto_orders'])} auto orders for {broker}")
    else:
        st.session_state["auto_orders"] = []
        logger.info(f"No auto orders to recover for {broker}")


if "initialized" not in st.session_state:
    recover_session_state()
    st.session_state["initialized"] = True


def send_email(subject, body):
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_SENDER
    msg["To"] = email_address
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
    except Exception as e:
        st.error(f"Failed to send email: {e}")


def notify(title, message, type='success'):
    if notify_channel == "Streamlit Toast":
        st.toast(f"{title}: {message}", icon="ℹ️" if type == "info" else "✅" if type == "success" else "⚠️")
    elif notify_channel == "Email" and email_address:
        send_email(title, message)


def sync_order_statuses(api, broker):
    stop_event = threading.Event()
    while not stop_event.is_set():
        orders_df = get_table_data(
            selected_database=DATABASE,
            selected_table="Orders",
            query=f"SELECT OrderID, Status FROM NSEDATA.dbo.Orders WHERE Status NOT IN "
                  f"('success', 'complete', 'rejected', 'cancelled', 'cancelled after market order') AND Broker = '{broker}'"
        )
        for _, row in orders_df.iterrows():
            try:
                if broker == "Upstox":
                    status = api.get_order_status(order_id=row["OrderID"]).data.status
                elif broker == "Zerodha":
                    status = api.order_history(order_id=row["OrderID"])[-1]["status"].lower()

                logger.info(f"Syncing order {row['OrderID']} for {broker}")
                update_order_status(row["OrderID"], status, broker)
            except Exception as e:
                logger.error(f"Error syncing order {row['OrderID']} for {broker}: {e}")
        stop_event.wait(300)
    logger.info(f"Order sync thread stopped for {broker}")


if "order_sync_thread" not in st.session_state:
    thread_upstox = threading.Thread(
        target=sync_order_statuses,
        args=(upstox_apis["order"], "Upstox"),
        daemon=True,
        name="OrderSyncThread_Upstox"
    )

    thread_zerodha = threading.Thread(
        target=sync_order_statuses,
        args=(kite_apis["kite"], "Zerodha"),
        daemon=True,
        name="OrderSyncThread_Zerodha"
    )
    stop_event = threading.Event()
    thread_upstox.start()
    thread_zerodha.start()
    st.session_state["thread_registry"]["OrderSyncThread_Upstox"] = {"thread": thread_upstox, "stop_event": stop_event}
    st.session_state["thread_registry"]["OrderSyncThread_Zerodha"] = {"thread": thread_zerodha, "stop_event": stop_event}
    st.session_state["order_sync_thread"] = True


def monitor_limit_order(order_id, instrument_token, transaction_type, quantity, product_type,
                        stop_loss_price, target_price):
    stop_event = threading.Event()
    thread_name = f"LimitOrderMonitor_{order_id}"
    while not stop_event.is_set():
        order_status = api.get_order_status(order_id=order_id).data.status if broker == "Upstox" else \
            api.order_history(order_id=order_id)[-1]["status"].lower()
        update_order_status(order_id, order_status, broker)
        if order_status.lower() == "complete":
            place_order(
                api, instrument_token, "BUY" if transaction_type == "SELL" else "SELL",
                quantity, 0, "SL-M", stop_loss_price, product_type=product_type, broker=broker
            )
            place_order(
                api, instrument_token, "BUY" if transaction_type == "SELL" else "SELL",
                quantity, target_price, "LIMIT", product_type=product_type, broker=broker
            )
            break
        elif order_status.lower() in ["rejected", "cancelled"]:
            break
        stop_event.wait(1)
    if stop_event.is_set():
        logger.info(f"Limit order monitor thread {thread_name} stopped")
    st.session_state["thread_registry"].pop(thread_name, None)
    logger.info(f"Limit order monitor thread {thread_name} started for order {order_id}")


# Place Order (Upstox or Zerodha)
def place_order(api, instrument_token, transaction_type, quantity, price=0, order_type="MARKET",
                trigger_price=0, is_amo=False, product_type="D", validity='DAY', stop_loss=None, target=None,
                remarks='Primary', primary_order='', broker="Upstox"):
    try:
        if broker == "Upstox":
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
        elif broker == "Zerodha":
            # Map Upstox parameters to Zerodha
            # zerodha_product = {"I": "MIS", "D": "CNC"}.get(product_type, "CNC")
            zerodha_validity = "DAY" if validity == "DAY" else "IOC"
            order_params = {
                "tradingsymbol": get_symbol_for_instrument(instrument_token=instrument_token),
                "exchange": instrument_token.split("|")[0].replace("_EQ", ""),
                "transaction_type": transaction_type,
                "order_type": order_type,
                "quantity": quantity,
                "product": product_type,
                "validity": zerodha_validity,
                "price": price if order_type in ["LIMIT", "SL"] else 0,
                "trigger_price": trigger_price if order_type in ["SL", "SL-M"] else 0,
                "tag": "StreamlitOrder"
            }
            print(order_params)
            response = api.place_order(
                variety=api.VARIETY_REGULAR if not is_amo else api.VARIETY_AMO,
                **order_params
            )
            primary_order_id = response

        notify(f"Order Placed: {transaction_type}", f"Order ID: {response}")

        # Save to SQL
        order_data = pd.DataFrame([{
            "OrderID": primary_order_id,
            "Broker": broker,
            "PrimaryOrderID": primary_order,
            "InstrumentToken": instrument_token,
            "TransactionType": transaction_type,
            "Quantity": quantity,
            "OrderType": order_type,
            "Price": price,
            "TriggerPrice": trigger_price,
            "ProductType": product_type,
            "API_Status": "success" if broker == "Zerodha" else response.status,
            "Status": "PENDING",
            "Remarks": f"{remarks} Order placed via Streamlit"
        }])
        load_sql_data(order_data, "Orders", load_type="append", index_required=False, database=DATABASE)

        # Monitor and log trade
        if stop_loss or target:
            def monitor_and_log_trade(primary_id):
                entry_price = None
                stop_event = threading.Event()
                thread_name = f"OrderMonitor_{primary_id}"
                while not stop_event.is_set():
                    if broker == "Upstox":
                        order_details = api.get_order_status(order_id=primary_id).data
                        order_status = order_details.status
                        update_order_status(primary_id, order_status, broker)
                        if order_status == "complete" and not entry_price:
                            entry_price = order_details.avg_price
                    elif broker == "Zerodha":
                        order_details = api.order_history(order_id=primary_id)[-1]
                        order_status = order_details["status"]
                        update_order_status(primary_id, order_status, broker)
                        if order_status == "COMPLETE" and not entry_price:
                            entry_price = order_details["average_price"]

                    if order_status.lower() in ["complete", "rejected", "cancelled"]:
                        break
                    time.sleep(1)

                    if order_status.lower() == "complete" and (stop_loss or target):
                        notify("Order Execution", f"Order {primary_id} executed")

                        sl_order_id = None
                        target_order_id = None

                        if stop_loss:
                            sl_transaction = "SELL" if transaction_type == "BUY" else "BUY"
                            sl_response = place_order(
                                api, instrument_token, sl_transaction, quantity, order_type="SL",
                                trigger_price=stop_loss, price=stop_loss, product_type=product_type,
                                remarks='SL Order', primary_order=primary_order_id, broker=broker
                            )
                            if sl_response:
                                sl_order_id = sl_response.data.order_id if broker == "Upstox" else sl_response[
                                    "order_id"]
                                notify("Stop-Loss Order Placed", f"SL Order ID: {sl_order_id}")

                        if target:
                            target_transaction = "SELL" if transaction_type == "BUY" else "BUY"
                            target_response = place_order(
                                api, instrument_token, target_transaction, quantity, order_type="LIMIT",
                                price=target, product_type=product_type, remarks='Target Order',
                                primary_order=primary_order_id, broker=broker
                            )
                            if target_response:
                                target_order_id = target_response.data.order_id if broker == "Upstox" else \
                                target_response["order_id"]
                                notify("Target Order Placed", f"Target Order ID: {target_order_id}")

                        sl_status = None
                        target_status = None
                        if sl_order_id:
                            sl_status = api.get_order_status(order_id=sl_order_id).data.status if broker == "Upstox" else \
                                api.order_history(order_id=sl_order_id)[-1]["status"]
                        if target_order_id:
                            target_status = api.get_order_status(
                                order_id=target_order_id).data.status if broker == "Upstox" else \
                                api.order_history(order_id=target_order_id)[-1]["status"]
                        exit_price = None
                        exit_time = None
                        if sl_status == ("complete" if broker == "Upstox" else "COMPLETE"):
                            exit_price = api.get_order_details(order_id=sl_order_id,
                                                               api_version="v2").data.avg_price if broker == "Upstox" else \
                                api.order_history(order_id=sl_order_id)[-1]["average_price"]
                            exit_time = api.get_order_details(order_id=sl_order_id,
                                                              api_version="v2").data.order_timestamp if broker == "Upstox" else \
                                api.order_history(order_id=sl_order_id)[-1]["order_timestamp"]
                            notify("SL Hit", f"Stop-loss triggered for Order ID {sl_order_id}")
                        elif target_status == ("complete" if broker == "Upstox" else "COMPLETE"):
                            exit_price = api.get_order_details(order_id=target_order_id,
                                                               api_version="v2").data.avg_price if broker == "Upstox" else \
                                api.order_history(order_id=target_order_id)[-1]["average_price"]
                            exit_time = api.get_order_details(order_id=target_order_id,
                                                              api_version="v2").data.order_timestamp if broker == "Upstox" else \
                                api.order_history(order_id=target_order_id)[-1]["order_timestamp"]
                            notify("Target Hit", f"Target triggered for Order ID {target_order_id}")
                        if exit_price:
                            trade_data = pd.DataFrame([{
                                "OrderID": primary_id,
                                "Broker": broker,
                                "InstrumentToken": instrument_token,
                                "EntryPrice": entry_price,
                                "ExitPrice": exit_price,
                                "Quantity": quantity,
                                "Pnl": (exit_price - entry_price) * quantity if transaction_type == "BUY" else
                                (entry_price - exit_price) * quantity,
                                "EntryTime": order_details.order_timestamp if broker == "Upstox" else order_details[
                                    "order_timestamp"],
                                "ExitTime": exit_time
                            }])
                            load_sql_data(trade_data, "TradeHistory", load_type="append", index_required=False,
                                          database=DATABASE)
                            break

                    stop_event.wait(1)
                if stop_event.is_set():
                    logger.info(f"Order monitor thread {thread_name} stopped")
                st.session_state["thread_registry"].pop(thread_name, None)

            thread = threading.Thread(
                target=monitor_and_log_trade,
                args=(primary_order_id,),
                daemon=True,
                name=f"OrderMonitor_{primary_order_id}"
            )
            stop_event = threading.Event()
            thread.start()
            st.session_state["thread_registry"][thread.name] = {"thread": thread, "stop_event": stop_event}

        return response
    except Exception as e:
        st.error(f"Error placing {broker} order: {e}")
        return None


def place_oco_orders(api, parent_order_id, instrument_token, transaction_type, quantity, product_type="D",
                     stop_loss=None, target=None, broker="Upstox"):
    """
    Place OCO (One Cancels the Other) orders after the parent limit order is executed.

    Parameters:
        api: Broker API instance (Upstox or Zerodha).
        instrument_token (str): Instrument token for the order.
        quantity (int): Quantity of the order.
        stop_loss (float): Stop-loss price.
        target (float): Target price.
        transaction_type (str): BUY or SELL.
        product_type (str): Product type (e.g., MIS, CNC).
        broker (str): Broker name (Upstox or Zerodha).
    """
    def monitor_parent_order(parent_order_id):
        while True:
            try:
                # Check the status of the parent order
                if broker == "Upstox":
                    order_status = api.get_order_status(order_id=parent_order_id).data.status
                elif broker == "Zerodha":
                    order_status = api.order_history(order_id=parent_order_id)[-1]["status"].lower()

                if order_status in ("complete", 'COMPLETE'):
                    # Place stop-loss and target orders
                    sl_transaction = "SELL" if transaction_type == "BUY" else "BUY"
                    target_transaction = "SELL" if transaction_type == "BUY" else "BUY"

                    sl_order = api.place_order(
                        instrument_token=instrument_token,
                        quantity=quantity,
                        price=stop_loss,
                        order_type="SL",
                        transaction_type=sl_transaction,
                        product=product_type
                    )
                    target_order = api.place_order(
                        instrument_token=instrument_token,
                        quantity=quantity,
                        price=target,
                        order_type="LIMIT",
                        transaction_type=target_transaction,
                        product=product_type
                    )

                    # Monitor stop-loss and target orders
                    if broker == 'Upstox':
                        monitor_oco_orders(api, sl_order["order_id"], target_order["order_id"], broker)
                    elif broker == 'Zerodha':
                        monitor_oco_orders(api, sl_order, target_order, broker)
                    break
            except Exception as e:
                print(f"Error monitoring parent order: {e}")
            time.sleep(1)

    def monitor_oco_orders(api, sl_order_id, target_order_id, broker):
        while True:
            try:
                # Check the status of stop-loss and target orders
                if broker == "Upstox":
                    sl_status = api.get_order_status(order_id=sl_order_id).data.status
                    target_status = api.get_order_status(order_id=target_order_id).data.status
                elif broker == "Zerodha":
                    sl_status = api.order_history(order_id=sl_order_id)[-1]["status"].lower()
                    target_status = api.order_history(order_id=target_order_id)[-1]["status"].lower()

                # Cancel the other order if one is executed
                if sl_status in ("complete", "COMPLETE"):
                    api.cancel_order(order_id=target_order_id)
                    break
                elif target_status in ("complete", "COMPLETE"):
                    api.cancel_order(order_id=sl_order_id)
                    break
            except Exception as e:
                print(f"Error monitoring OCO orders: {e}")
            time.sleep(300)  # Check every 5 minutes
    threading.Thread(target=monitor_parent_order, args=(parent_order_id,), daemon=True).start()


# Fetch Unified Order Book
def get_order_book(upstox_api, zerodha_api):
    orders = []
    try:
        # Upstox orders
        upstox_orders = upstox_api.get_order_book(api_version="v2").data
        for order in upstox_orders:
            order_dict = order.to_dict()
            orders.append({
                "Broker": "Upstox",
                "Order ID": order_dict.get("order_id", ""),
                "Symbol": order_dict.get("trading_symbol", ""),
                "Exchange": order_dict.get("exchange", ""),
                "Trans. Type": order_dict.get("transaction_type", ""),
                "Order Type": order_dict.get("order_type", ""),
                "Product": order_dict.get("product", ""),
                "Quantity": order_dict.get("quantity", 0),
                "Status": order_dict.get("status", ""),
                "Price": order_dict.get("price", 0),
                "Trigger Price": order_dict.get("trigger_price", 0),
                "Avg. Price": order_dict.get("average_price", 0),
                "Filled Qty": order_dict.get("filled_quantity", 0),
                "Order Time": order_dict.get("order_timestamp", ""),
                "Remarks": order_dict.get("status_message", "")
            })
    except Exception as e:
        st.error(f"Failed to fetch Upstox orders: {e}")

    try:
        # Zerodha orders
        zerodha_orders = zerodha_api.orders()
        for order in zerodha_orders:
            orders.append({
                "Broker": "Zerodha",
                "Order ID": order.get("order_id", ""),
                "Symbol": order.get("tradingsymbol", ""),
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
            })
    except Exception as e:
        st.error(f"Failed to fetch Zerodha orders: {e}")

    return pd.DataFrame(orders)


# Fetch Unified Positions
def get_positions(upstox_api, zerodha_api):
    positions = []
    try:
        # Upstox positions
        upstox_positions = upstox_api.get_positions(api_version="v2").data
        for pos in upstox_positions:
            pos_dict = pos.to_dict()
            positions.append({
                "Broker": "Upstox",
                "Symbol": pos_dict.get("trading_symbol", ""),
                "Exchange": pos_dict.get("exchange", ""),
                "Product": pos_dict.get("product", ""),
                "Quantity": pos_dict.get("quantity", 0),
                "Avg. Price": pos_dict.get("average_price", 0),
                "Last Price": pos_dict.get("last_price", 0),
                "P&L": pos_dict.get("pnl", 0),
                "Instrument Token": pos_dict.get("instrument_token", "")
            })
    except Exception as e:
        st.error(f"Failed to fetch Upstox positions: {e}")

    try:
        # Zerodha positions
        zerodha_positions = zerodha_api.positions().get("net", [])
        for pos in zerodha_positions:
            positions.append({
                "Broker": "Zerodha",
                "Symbol": pos.get("tradingsymbol", ""),
                "Exchange": pos.get("exchange", ""),
                "Product": pos.get("product", ""),
                "Quantity": pos.get("net_quantity", 0),
                "Avg. Price": pos.get("average_price", 0),
                "Last Price": pos.get("last_price", 0),
                "P&L": pos.get("pnl", 0),
                "Instrument Token": f"{pos.get('exchange')}|{pos.get('tradingsymbol')}"
            })
    except Exception as e:
        st.error(f"Failed to fetch Zerodha positions: {e}")

    return pd.DataFrame(positions)


# Fetch Unified Portfolio
def get_portfolio(upstox_api, zerodha_api):
    holdings = []
    try:
        # Upstox portfolio
        upstox_holdings = upstox_api.get_holdings(api_version="v2").data
        for holding in upstox_holdings:
            holding_dict = holding.to_dict()
            holdings.append({
                "Broker": "Upstox",
                "Symbol": holding_dict.get("trading_symbol", ""),
                "Exchange": holding_dict.get("exchange", ""),
                "Quantity": holding_dict.get("quantity", 0),
                "Last Price": holding_dict.get("last_price", 0),
                "Avg. Price": holding_dict.get("average_price", 0),
                "P&L": holding_dict.get("pnl", 0),
                "Day Change": holding_dict.get("day_change", 0) * holding_dict.get("quantity", 0),
                "Day Change %": holding_dict.get("day_change_percentage", 0),
            })
    except Exception as e:
        st.error(f"Failed to fetch Upstox portfolio: {e}")

    try:
        # Zerodha portfolio
        zerodha_holdings = zerodha_api.holdings()
        for holding in zerodha_holdings:
            holdings.append({
                "Broker": "Zerodha",
                "Symbol": holding.get("tradingsymbol", ""),
                "Exchange": holding.get("exchange", ""),
                "Quantity": holding.get("quantity", 0),
                "Last Price": holding.get("last_price", 0),
                "Avg. Price": holding.get("average_price", 0),
                "P&L": holding.get("pnl", 0),
                "Day Change": holding.get("day_change", 0)*holding.get("quantity", 0),
                "Day Change %": holding.get("day_change_percentage", 0),
            })
    except Exception as e:
        st.error(f"Failed to fetch Zerodha portfolio: {e}")

    return pd.DataFrame(holdings)


def update_order_status(order_id, status, broker):
    query = f"UPDATE NSEDATA.dbo.Orders SET Status = '{status}' WHERE OrderID = '{order_id}' AND Broker = '{broker}'"
    with create_connection(DATABASE).connect() as conn:
        conn.execute(text(query))
        conn.commit()


def update_scheduled_order(order_id, new_quantity=None, new_price=None, new_trigger_price=None,
                          new_schedule_time=None, new_stop_loss=None, new_target=None, status=None, broker="Upstox"):
    updates = []
    params = {"order_id": order_id, "broker": broker}
    if new_quantity is not None:
        updates.append("Quantity = :new_quantity")
        params["new_quantity"] = new_quantity
    if new_price is not None:
        updates.append("Price = :new_price")
        params["new_price"] = new_price
    if new_trigger_price is not None:
        updates.append("TriggerPrice = :new_trigger_price")
        params["new_trigger_price"] = new_trigger_price
    if new_schedule_time is not None:
        updates.append("ScheduleDateTime = :new_schedule_time")
        params["new_schedule_time"] = new_schedule_time
    if new_stop_loss is not None:
        updates.append("StopLoss = :new_stop_loss")
        params["new_stop_loss"] = new_stop_loss
    if new_target is not None:
        updates.append("Target = :new_target")
        params["new_target"] = new_target
    if status is not None:
        updates.append("Status = :status")
        params["status"] = status
    if updates:
        query = text(f"UPDATE NSEDATA.dbo.ScheduledOrders SET {', '.join(updates)} WHERE ScheduledOrderID = :order_id AND Broker = :broker")
        try:
            with create_connection(DATABASE).connect() as conn:
                conn.execute(query, params)
                conn.commit()
                logger.info(f"Scheduled order {order_id} updated for {broker}: {updates}")
        except Exception as e:
            logger.error(f"Failed to update scheduled order {order_id} for {broker}: {e}")


def run_auto_orders(api, broker):
    if "auto_orders" not in st.session_state or not st.session_state["auto_orders"]:
        logger.info(f"No auto orders defined for {broker}")
        return "No auto orders to execute."
    now = datetime.now()
    market_open_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if not (market_open_time <= now <= market_close_time):
        logger.info(f"Current time {now} is outside market hours (9:15 - 15:30)")
        return "Market is closed. Auto orders will run at next market open."
    if "last_auto_order_run" in st.session_state and st.session_state["last_auto_order_run"].date() == now.date():
        logger.info("Auto orders already executed today")
        return "Auto orders already executed today."
    results = []
    for order in st.session_state["auto_orders"]:
        if order["Broker"] != broker:
            continue
        instrument_token = order["InstrumentToken"]
        risk_per_trade = order["RiskPerTrade"]
        stop_loss_type = order["StopLossType"]
        stop_loss_value = order["StopLossValue"]
        target_value = order["TargetValue"]
        atr_period = order["ATRPeriod"]
        transaction_type = order["TransactionType"]
        product_type = order["ProductType"]
        order_type = order["OrderType"]
        limit_price = order["LimitPrice"]
        live_data = get_market_quote(upstox_apis['market_data'], instrument_token)
        if not live_data or "ltp" not in live_data:
            logger.error(f"Failed to fetch live data for {instrument_token}")
            results.append(f"Failed to fetch live data for {instrument_token}")
            continue
        current_price = live_data["ltp"]
        hist_data = get_historical_data(instrument_token)
        if hist_data is None:
            logger.error(f"Failed to fetch historical data for {instrument_token}")
            results.append(f"Failed to fetch historical data for {instrument_token}")
            continue
        df = pd.DataFrame(hist_data)
        funds_data = get_user_profile_and_funds(upstox_apis["user"], user_profile=False)
        available_margin = funds_data["data"]["equity"]["available_margin"] if broker == "Upstox" else \
                          funds_data["equity"]["available_margin"] if funds_data else 0
        risk_amount = available_margin * (risk_per_trade / 100)
        if stop_loss_type == "Fixed Amount":
            stop_loss_price = current_price + stop_loss_value if transaction_type == "SELL" else current_price - stop_loss_value
            target_price = current_price - target_value if transaction_type == "SELL" else current_price + target_value
            quantity = int(risk_amount / stop_loss_value)
        elif stop_loss_type == "Percentage of Entry":
            stop_loss_price = current_price * (1 + stop_loss_value / 100) if transaction_type == "SELL" else \
                              current_price * (1 - stop_loss_value / 100)
            target_price = current_price * (1 - target_value / 100) if transaction_type == "SELL" else \
                           current_price * (1 + target_value / 100)
            quantity = int(risk_amount / (current_price * (stop_loss_value / 100)))
        else:
            atr = calculate_atr(df, atr_period).iloc[-1] if not df.empty else 0
            stop_loss_price = current_price + (atr * stop_loss_value) if transaction_type == "SELL" else \
                              current_price - (atr * stop_loss_value)
            target_price = current_price - (atr * target_value) if transaction_type == "SELL" else \
                           current_price + (atr * target_value)
            quantity = int(risk_amount / (atr * stop_loss_value)) if atr > 0 else 1
        quantity = max(1, quantity)
        try:
            order_response = place_order(
                api, instrument_token, transaction_type, quantity,
                price=limit_price if order_type == "LIMIT" else 0,
                order_type=order_type, product_type=product_type, is_amo=False, broker=broker
            )
            if order_response:
                order_id = order_response.data.order_id if broker == "Upstox" else order_response
                logger.info(f"Auto order placed for {instrument_token}: Order ID {order_id}")
                results.append(f"Auto order placed for {instrument_token}: Order ID {order_id}")
                if order_type == "LIMIT":
                    thread = threading.Thread(
                        target=monitor_limit_order,
                        args=(order_id, instrument_token, transaction_type, quantity, product_type, stop_loss_price, target_price),
                        daemon=True,
                        name=f"LimitOrderMonitor_{order_id}"
                    )
                    stop_event = threading.Event()
                    thread.start()
                    st.session_state["thread_registry"][thread.name] = {"thread": thread, "stop_event": stop_event}
                else:
                    place_order(
                        api, instrument_token, "BUY" if transaction_type == "SELL" else "SELL",
                        quantity, 0, "SL-M", stop_loss_price, product_type=product_type, broker=broker
                    )
                    place_order(
                        api, instrument_token, "BUY" if transaction_type == "SELL" else "SELL",
                        quantity, target_price, "LIMIT", product_type=product_type, broker=broker
                    )
            else:
                logger.error(f"Failed to place auto order for {instrument_token}")
                results.append(f"Failed to place auto order for {instrument_token}")
        except Exception as e:
            logger.error(f"Error placing auto order for {instrument_token}: {str(e)}")
            results.append(f"Error placing auto order for {instrument_token}: {str(e)}")
    st.session_state["last_auto_order_run"] = now
    return "\n".join(results)


def get_running_threads():
    """Return a list of dictionaries with details of all active threads from the registry."""
    threads = []
    for name, info in st.session_state["thread_registry"].items():
        thread = info["thread"]
        if thread.is_alive():  # Only include alive threads
            threads.append({
                "Name": name,
                "ID": thread.ident,
                "Daemon": thread.daemon,
                "Alive": thread.is_alive()
            })
    return threads


def restart_thread(thread_name):
    """Restart a specific thread if supported."""
    if thread_name == "RiskCheckThread":
        if "RiskCheckThread" not in st.session_state["thread_registry"]:
            thread = threading.Thread(target=periodic_risk_check, daemon=True, name="RiskCheckThread")
            stop_event = threading.Event()
            thread.start()
            st.session_state["thread_registry"]["RiskCheckThread"] = {"thread": thread, "stop_event": stop_event}
            st.session_state["risk_check_thread"] = True
            st.success("RiskCheckThread restarted")
    # Add similar logic for other threads as needed


def log_thread_activity():
    """Log details of running threads."""
    threads = get_running_threads()
    for t in threads:
        logger.info(f"Thread: {t['Name']}, ID: {t['ID']}, Daemon: {t['Daemon']}, Alive: {t['Alive']}")


def check_risk_exposure(api, auto_orders, positions_df=None):
    funds_data = get_user_profile_and_funds(api, user_profile=False)
    available_margin = funds_data["data"]["equity"]["available_margin"] if broker == "Upstox" else \
                      funds_data["equity"]["available_margin"] if funds_data else 0
    total_risk = sum(order["RiskPerTrade"] for order in auto_orders if order["Broker"] == broker) + (
        sum(abs(row["UnrealizedPnL"]) / available_margin * 100 for _, row in positions_df.iterrows())
        if positions_df is not None else 0
    )
    max_risk_threshold = 5.0
    if total_risk > max_risk_threshold:
        notify("Risk Alert", f"Total risk ({total_risk:.2f}%) exceeds threshold ({max_risk_threshold}%)", "warning")


def periodic_risk_check(api, auto_orders, positions_df=None):
    stop_event = threading.Event()
    while not stop_event.is_set():
        check_risk_exposure(api, auto_orders, positions_df)
        stop_event.wait(60)
    logger.info("Risk check thread stopped")


if "risk_check_thread" not in st.session_state:
    auto_orders = st.session_state.get("auto_orders", [])
    positions_df = None
    thread = threading.Thread(
        target=periodic_risk_check,
        args=(upstox_apis["user"] if broker == "Upstox" else kite_apis["kite"], auto_orders, positions_df),
        daemon=True,
        name="RiskCheckThread"
    )
    stop_event = threading.Event()
    thread.start()
    st.session_state["thread_registry"]["RiskCheckThread"] = {"thread": thread, "stop_event": stop_event}
    st.session_state["risk_check_thread"] = True

# Sidebar
with st.sidebar:
    st.subheader("Navigation")
    page = st.radio("Go to",
                    ["Order Management", "Order Book", "Positions", "Trade Dashboard", "Portfolio",
                     "Analytics", "Algo Trading", "Strategy Backtest", "Get Token"])
    st.subheader("Notification Settings")
    st.session_state["email_notifications"] = st.checkbox("Enable Email Notifications", value=False)

    notify_triggers = st.sidebar.multiselect("Notify On", ["Order Execution", "SL Hit", "Target Hit"])
    notify_channel = st.sidebar.selectbox("Channel", ["Streamlit Toast", "Email"])
    if notify_channel == "Email":
        email_address = st.sidebar.text_input("Email Address")

    st.subheader("Risk Management Settings")
    max_risk_threshold = st.sidebar.number_input("Max Risk Threshold (% of Capital)", min_value=1.0, value=5.0,
                                                 step=0.5)
    if st.button("Refresh Dashboard", key="refresh"):
        st.rerun()

    enable_debug = st.checkbox("View Session State")


if 'instruments_data' not in st.session_state:
    st.session_state['instruments_data'] = fetch_instruments()
instruments = st.session_state['instruments_data'] if 'instruments_data' in st.session_state else {}

if page == "Get Token":
    st.subheader(f"Fetch {broker} Access Token")
    if broker == "Upstox":
        api_key = st.text_input("API Key", value=UPSTOX_API_KEY)
        api_secret = st.text_input("API Secret", value=UPSTOX_API_SECRET)
        redirect_uri = st.text_input("Redirect URI", "https://api.upstox.com/v2/login")
        auth_url = f"https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={api_key}&redirect_uri={redirect_uri}"
        st.markdown(f"[Click here to authorize]({auth_url})")
        auth_code = st.text_input("Authorization Code (from redirect URL)")
        if st.button("Fetch Access Token") and auth_code:
            token = fetch_upstox_access_token(api_key, api_secret, redirect_uri, auth_code)
            if token:
                st.success(f"Successfully fetched Upstox Access Token: {token}")
                st.session_state["access_token"] = token
    elif broker == "Zerodha":
        if st.button("Fetch Access Token"):
            token = fetch_zerodha_access_token()
            if token:
                st.success(f"Successfully fetched Zerodha Access Token: {token}")
                st.session_state["zerodha_access_token"] = token

elif page == "Order Management":
    if 'funds_data' not in st.session_state:
        st.session_state['funds_data'] = get_user_profile_and_funds(upstox_apis['user'])
    st.subheader(f"Order Management - {broker}")
    funds_cols = st.columns([2, 1, 2], gap="small", vertical_alignment='center')
    with funds_cols[0]:
        try:
            if broker == 'Upstox':
                st.markdown(
                    f'<div class="metric-box">Upstox Funds Available: ₹{st.session_state["funds_data"]["data"]["equity"]["available_margin"]:.2f}</div>',
                    unsafe_allow_html=True)
            elif broker == 'Zerodha':
                zerodha_funds = kite_apis["kite"].margins()["equity"]["available"]["live_balance"]
                st.markdown(f'<div class="metric-box">Zerodha Funds Available: ₹{zerodha_funds:.2f}</div>',
                            unsafe_allow_html=True)
        except:
            st.markdown(f'<div class="metric-box">Unable to fetch {broker} funds data</div>', unsafe_allow_html=True)

    num_orders = funds_cols[2].number_input("Number of Orders", min_value=1, max_value=10, value=1, step=1)
    orders = []
    for i in range(num_orders):
        st.markdown(f":rainbow[Order {i + 1}]")
        multi_order_cols = st.columns(9)
        stock_symbol = multi_order_cols[0].selectbox("Select Symbol", options=instruments.keys(), key=f"multi_order_{i}")
        instrument_token = instruments.get(stock_symbol)
        with st.expander("Calculate Position Size", expanded=False):
            capital = st.number_input("Total Capital (Rs.)", min_value=1000, value=50000, step=1000, key=f"multi_capital_{i}")
            risk_per_trade = st.number_input("Risk per Trade (%)", min_value=0.1, value=1.0, step=0.1, key=f"multi_risk_{i}")
            product_type = st.selectbox("Product Type", ['I', 'D'] if broker == "Upstox" else ['MIS', 'CNC'], key=f"multi_product_calc_{i}")
            if product_type in ['I', 'MIS']:
                transaction_type = st.radio("Transaction Type", ["BUY", "SELL"], horizontal=True, key=f"multi_trans_calc_{i}")
            else:
                transaction_type = "BUY"
            stop_loss_type = st.selectbox("Stop Loss Type", ["Fixed Amount", "Percentage of Entry", "ATR Based"], key=f"multi_sl_type_{i}")
            live_data = get_market_quote(upstox_apis['market_data'], instrument_token)
            entry_price = live_data.get('ltp', 0) if live_data else 0
            if stop_loss_type == "Fixed Amount":
                stop_loss_value = st.number_input("Stop Loss Value (Rs.)", min_value=1.0, value=100.0, step=1.0, key=f"multi_sl_fixed_{i}")
                target_value = st.number_input("Target Value (Rs.)", min_value=1.0, value=250.0, step=1.0, key=f"multi_target_fixed_{i}")
            elif stop_loss_type == "Percentage of Entry":
                stop_loss_percent = st.number_input("Stop Loss (%)", min_value=0.1, value=1.0, step=0.1, key=f"multi_sl_pct_{i}")
                target_percent = st.number_input("Target (%)", min_value=0.1, value=2.5, step=0.1, key=f"multi_target_pct_{i}")
            else:
                atr_period = st.number_input("ATR Period", min_value=5, value=14, step=1, key=f"multi_atr_period_{i}")
                stop_loss_atr_mult = st.number_input("Stop Loss ATR Multiplier", min_value=0.5, value=2.0, step=0.5, key=f"multi_sl_atr_{i}")
                target_atr_mult = st.number_input("Target ATR Multiplier", min_value=0.5, value=5.0, step=0.5, key=f"multi_target_atr_{i}")
                hist_data = get_historical_data(instrument_token)
                if hist_data is not None:
                    df = pd.DataFrame(hist_data)
                    df['ATR'] = calculate_atr(df, atr_period)
                    atr = df['ATR'].iloc[-1] if not df['ATR'].empty else 0
                else:
                    atr = 0
            if st.button("Calculate", key=f"multi_calc_{i}"):
                if entry_price == 0:
                    st.error("No live price available")
                else:
                    risk_amount = capital * (risk_per_trade / 100)
                    if stop_loss_type == "Fixed Amount":
                        if transaction_type == "BUY":
                            stop_loss_price = entry_price - stop_loss_value
                            target_price = entry_price + target_value
                        else:
                            stop_loss_price = entry_price + stop_loss_value
                            target_price = entry_price - target_value
                        quantity = int(risk_amount / stop_loss_value)
                    elif stop_loss_type == "Percentage of Entry":
                        if transaction_type == "BUY":
                            stop_loss_price = entry_price * (1 - stop_loss_percent / 100)
                            target_price = entry_price * (1 + target_percent / 100)
                        else:
                            stop_loss_price = entry_price * (1 + stop_loss_percent / 100)
                            target_price = entry_price * (1 - target_percent / 100)
                        quantity = int(risk_amount / (entry_price * (stop_loss_percent / 100)))
                    else:
                        if transaction_type == "BUY":
                            stop_loss_price = entry_price - (atr * stop_loss_atr_mult)
                            target_price = entry_price + (atr * target_atr_mult)
                        else:
                            stop_loss_price = entry_price + (atr * stop_loss_atr_mult)
                            target_price = entry_price - (atr * target_atr_mult)
                        quantity = int(risk_amount / (atr * stop_loss_atr_mult)) if atr > 0 else 0
                    quantity = max(1, quantity)
                    trade_value = entry_price * quantity
                    entry_brokerage = calculate_brokerage(
                        upstox_apis["charges"], instrument_token, quantity,
                        entry_price, transaction_type, product_type
                    )
                    if product_type in ['I', 'MIS']:
                        exit_brokerage = calculate_brokerage(
                            upstox_apis["charges"], instrument_token, quantity,
                            target_price if transaction_type == "BUY" else stop_loss_price,
                            "SELL" if transaction_type == "BUY" else "BUY", product_type
                        )
                        total_brokerage = entry_brokerage + exit_brokerage
                        entry_charges = trade_value * 0.001
                        exit_charges = (target_price if transaction_type == "BUY" else stop_loss_price) * quantity * 0.001
                        total_charges = entry_charges + exit_charges
                        st.write(f"**Brokerage (Both Legs):** Rs. {total_brokerage:.2f}")
                        st.write(f"**Other Charges (Both Legs):** Rs. {total_charges:.2f}")
                        total_cost = trade_value + total_brokerage + total_charges
                    else:
                        total_brokerage = entry_brokerage
                        total_charges = trade_value * 0.001
                        st.write(f"**Brokerage (Entry):** Rs. {total_brokerage:.2f}")
                        st.write(f"**Other Charges (Entry):** Rs. {total_charges:.2f}")
                        total_cost = trade_value + total_brokerage + total_charges
                    st.write(f"**Quantity:** {quantity}")
                    st.write(f"**Stop Loss Price:** Rs. {stop_loss_price:.2f}")
                    st.write(f"**Target Price:** Rs. {target_price:.2f}")
                    st.write(f"**Total Cost:** Rs. {total_cost:.2f}")
                    if "calc_result" not in st.session_state:
                        st.session_state["calc_result"] = {}
                    st.session_state["calc_result"][stock_symbol] = {
                        "quantity": quantity,
                        "stop_loss": stop_loss_price,
                        "target": target_price,
                        "entry_price": entry_price,
                        "transaction_type": transaction_type
                    }
        live_data = get_market_quote(upstox_apis['market_data'], instrument_token)
        try:
            st.markdown(f":rainbow[Last Traded Price] - {live_data.get('ltp', 0)}")
        except:
            st.markdown(f":rainbow[Last Traded Price] - N/A")
        order_types_upstox = ["MARKET", "LIMIT", "SL", "SL-M"]
        zerodha_order_types = order_types_upstox + ["COVER"]
        order_types = zerodha_order_types if broker == "Zerodha" else order_types_upstox
        calc_result = st.session_state.get("calc_result", {}).get(stock_symbol, {})
        quantity = multi_order_cols[1].number_input(f"Quantity {i + 1}", min_value=1,
                                                    value=calc_result.get("quantity", 1), key=f"multi_qty_{i}")
        order_type = multi_order_cols[2].selectbox(f"Order Type {i + 1}", order_types, key=f"multi_order_type_{i}")
        transaction_type = multi_order_cols[3].radio(
            f"Transaction Type {i + 1}", ["BUY", "SELL"], key=f"multi_trans_{i}", horizontal=True,
            index=0 if calc_result.get("transaction_type") == 'BUY' else 1)
        product_type = multi_order_cols[4].radio(
            "Product Type", ['I', 'D'] if broker == "Upstox" else ['MIS', 'CNC'],
            horizontal=True, key=f"multi_prod_type_{i}")
        amo_order = multi_order_cols[5].checkbox("AMO Order", key=f"multi_amo_{i}")

        schedule_order = multi_order_cols[8].checkbox("Schedule", key=f"multi_schedule_short_{i}")
        if order_type == "LIMIT":
            price = multi_order_cols[6].number_input(
                f"Limit Price {i + 1}", min_value=0.0, value=0.0, key=f"multi_price_{i}")
            trigger_price = 0
        elif order_type == "SL":
            price = multi_order_cols[6].number_input(
                f"Limit Price {i + 1}", min_value=0.0, value=live_data.get('ltp', 0), key=f"multi_price_sl_{i}")
            trigger_price = multi_order_cols[7].number_input(
                f"Trigger Price {i + 1}", min_value=0.0, value=live_data.get('ltp', 0), key=f"multi_trigger_{i}")
        elif order_type == "SL-M":
            price = 0
            trigger_price = multi_order_cols[6].number_input(
                f"Stoploss Trigger {i + 1}", min_value=0.0, value=live_data.get('ltp', 0), key=f"multi_trigger_slm_{i}")
        elif order_type == "COVER":
            price = multi_order_cols[6].number_input(
                f"Limit Price {i + 1}", min_value=0.0, value=live_data.get('ltp', 0), key=f"multi_price_cover_{i}")
            trigger_price = multi_order_cols[7].number_input(
                f"Trigger Price {i + 1}", min_value=0.0, value=live_data.get('ltp', 0), key=f"multi_trigger_cover_{i}")
        else:
            price, trigger_price = 0, 0

        stop_loss = multi_order_cols[6].number_input(
            f"Stop-Loss {i + 1}", min_value=0.0, value=calc_result.get("stop_loss", 0.0), key=f"multi_sl_{i}")
        target = multi_order_cols[7].number_input(
            f"Target {i + 1}", min_value=0.0, value=calc_result.get("target", 0.0), key=f"multi_target_{i}")

        if schedule_order:
            schedule_cols = st.columns(4)
            schedule_time = schedule_cols[0].time_input(
                f"Schedule Time {i + 1}", value=date_time(9, 15), step=60, key=f"multi_time_{i}")
            schedule_date = schedule_cols[1].date_input(
                f"Schedule Date {i + 1}", value=datetime.now(), min_value=datetime.now(), key=f"multi_date_{i}")
            schedule_datetime = datetime.combine(schedule_date, schedule_time)
        else:
            schedule_datetime = None
        order_data = {
            "order_id": f"multi_scheduled_{i}_{uuid.uuid4().hex.upper()[0:6]}",
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
            "schedule_datetime": schedule_datetime + timedelta(seconds=1) if schedule_datetime else schedule_datetime,
            "stop_loss": stop_loss,
            "target": target,
            "strategy": "short_sell_open" if schedule_order else None,
            "broker": broker
        }
        orders.append(order_data)
    if st.button("Place Order(s)"):
        with st.spinner("Placing order(s)..."):
            for order in orders:
                if order["schedule_datetime"] and order["schedule_datetime"] > datetime.now():
                    scheduled_order_data = pd.DataFrame([{
                        "ScheduledOrderID": order["order_id"],
                        "Broker": order["broker"],
                        "InstrumentToken": order["instrument_token"],
                        "TransactionType": order["transaction_type"],
                        "Quantity": order["quantity"],
                        "OrderType": order["order_type"],
                        "Price": order["price"],
                        "TriggerPrice": order["trigger_price"],
                        "ProductType": order["product"],
                        "ScheduleDateTime": order["schedule_datetime"],
                        "StopLoss": order["stop_loss"],
                        "Target": order["target"]
                    }])
                    load_sql_data(scheduled_order_data, "ScheduledOrders", load_type="append", index_required=False, database=DATABASE)
                    thread = threading.Thread(
                        target=execute_scheduled_order,
                        args=(order,),
                        daemon=True,
                        name=f"ScheduledOrder_{order['order_id']}"
                    )
                    stop_event = threading.Event()
                    thread.start()
                    st.session_state["thread_registry"][thread.name] = {"thread": thread, "stop_event": stop_event}
                    st.write(f"Order {order['tag']} scheduled for {order['schedule_datetime']}")
                else:
                    api = upstox_apis["order"] if broker == "Upstox" else kite_apis["kite"]
                    result = place_order(
                        api, order["instrument_token"], order["transaction_type"],
                        order["quantity"], order["price"], order["order_type"],
                        order["trigger_price"], order["is_amo"], order["product"],
                        order["validity"], order["stop_loss"], order["target"], broker=broker
                    )
                    if result:
                        order_id = result.data.order_id if broker == "Upstox" else result
                        st.success(f"Order {order['tag']} placed: Order ID {order_id}")

elif page == "Order Book":
    st.subheader("Order Book")
    tabs = st.tabs(["Orders", "Scheduled Orders", "Auto Orders", "Threads"])
    db_orders = get_table_data(selected_database=DATABASE, selected_table="Orders")
    with tabs[0]:
        orders_df = get_order_book(upstox_apis["order"], kite_apis['kite'])

        if not orders_df.empty:
            orders_df['Order Time'] = pd.to_datetime(orders_df['Order Time'])
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
                broker_filter = st.multiselect(
                    "Filter by Broker",
                    options=["Zerodha", "Upstox"],
                    default=broker
                )
            # with col3:
            #     date_range = st.date_input(
            #         "Date Range",
            #         value=(datetime.now() - timedelta(days=7), datetime.now())
            #     )

            filtered_df = orders_df
            if status_filter:
                filtered_df = filtered_df[filtered_df["Status"].isin(status_filter)]
            if transaction_filter:
                filtered_df = filtered_df[filtered_df["Trans. Type"].isin(transaction_filter)]
            if broker_filter:
                filtered_df = filtered_df[filtered_df["Broker"].isin(broker_filter)]
            # if date_range:
            #     filtered_df = filtered_df[
            #         (pd.to_datetime(filtered_df["Order Time"]) >= pd.to_datetime(date_range[0])) &
            #         (pd.to_datetime(filtered_df["Order Time"]) <= pd.to_datetime(date_range[1]))
            #         ]

            st.dataframe(filtered_df.sort_values(by="Order Time", ascending=False))
            filter_cols = ['complete', 'rejected', 'cancelled', 'cancelled after market order',
                           'CANCELLED', 'CANCELLED AMO', 'REJECTED', 'COMPLETE']
            col1, col2, _, col3 = st.columns([1, .75, .75, .36], vertical_alignment='bottom')
            selected_order_id = col1.selectbox(
                "Select Order to Modify/Cancel",
                options=filtered_df["Order ID"][~filtered_df["Status"].isin(filter_cols)].tolist()
            )

            if selected_order_id:
                order_data = filtered_df[filtered_df["Order ID"] == selected_order_id].iloc[0]
                broker = order_data["Broker"]

                with col2:
                    if order_data["Status"] not in filter_cols:
                        if st.button("Cancel Order"):
                            try:
                                if broker == "Upstox":
                                    response = upstox_apis["order"].cancel_order(selected_order_id,
                                                                                 api_version="2.0").data
                                else:
                                    response = kite_apis["kite"].cancel_order(variety=kite_apis["kite"].VARIETY_AMO,
                                                                        order_id=selected_order_id)
                                if response:
                                    st.success(f"Order cancelled successfully! {response}")
                                else:
                                    st.error("Failed to cancel order.")
                            except Exception as e:
                                st.error(f"Failed to cancel {broker} order: {e}")

                with col3:
                    if st.button("Cancel All Orders", type='primary'):
                        try:
                            if broker == "Upstox":
                                response = upstox_apis["order"].cancel_multi_order().data
                            else:
                                # Zerodha doesn't have a direct cancel all API; cancel each open order
                                open_orders = [o for o in kite_apis["kite"].orders() if
                                               o["status"] not in ["COMPLETE", "REJECTED", "CANCELLED",
                                                                   'CANCELLED AMO']]
                                for order in open_orders:
                                    kite_apis["kite"].cancel_order(
                                        variety=kite_apis["kite"].VARIETY_AMO if 'AMO' in order['status']
                                        else kite_apis["kite"].VARIETY_REGULAR,
                                        order_id=order["order_id"])
                                response = "All open orders cancelled"
                            if response:
                                st.toast(f"Orders cancelled successfully! {response}")
                            else:
                                st.toast("Failed to cancel orders.")
                        except Exception as e:
                            st.error(f"Failed to cancel all {broker} orders: {e}")

                if not order_data.empty:
                    modify_cols = st.columns(2)
                    with modify_cols[1]:
                        with st.expander("Order Details"):
                            row_cols = st.columns(2)
                            with row_cols[0]:
                                st.write(f"Broker: {order_data['Broker']}")
                                st.write(f"Symbol: {order_data['Symbol']}")
                                st.write(f"Exchange: {order_data['Exchange']}")
                                st.write(f"Transaction Type: {order_data['Trans. Type']}")
                                st.write(f"Order Type: {order_data['Order Type']}")
                                st.write(f"Product: {order_data['Product']}")
                            with row_cols[1]:
                                st.write(f"Quantity: {order_data['Quantity']}")
                                st.write(f"Status: {order_data['Status']}")
                                st.write(f"Price: {order_data['Price']}")
                                st.write(f"Trigger Price: {order_data['Trigger Price']}")

                    with modify_cols[0]:
                        if order_data["Status"] not in filter_cols:
                            st.subheader("Modify Order")
                            modify_order_cols = st.columns(3)
                            new_quantity = modify_order_cols[0].number_input(
                                "New Quantity",
                                min_value=1,
                                value=int(order_data["Quantity"])
                            )
                            if order_data["Order Type"] in ["LIMIT", "SL"]:
                                new_price = st.number_input(
                                    "New Price",
                                    min_value=0.05,
                                    step=0.05,
                                    format="%.2f",
                                    value=float(order_data["Price"])
                                )
                            if order_data["Order Type"] in ["SL", "SL-M"]:
                                new_trigger_price = st.number_input(
                                    "New Trigger Price",
                                    min_value=0.05,
                                    step=0.05,
                                    format="%.2f",
                                    value=float(order_data["Trigger Price"])
                                )
                            new_disclosed_qty = modify_order_cols[1].number_input(
                                "New Disclosed Quantity",
                                min_value=0,
                                value=int(order_data.get("Filled Qty", 0))
                            )
                            order_type = modify_order_cols[2].selectbox(
                                "Order Type",
                                options=['LIMIT', 'MARKET', 'SL', 'SL-M'],
                                index=['LIMIT', 'MARKET', 'SL', 'SL-M'].index(order_data["Order Type"])
                            )

                            if st.button("Modify Order"):
                                try:
                                    if broker == "Upstox":
                                        modify_data = {
                                            "order_id": selected_order_id,
                                            "quantity": new_quantity,
                                            "disclosed_quantity": new_disclosed_qty if new_disclosed_qty > 0 else None,
                                            "validity": 'DAY',
                                            "order_type": order_type,
                                            "trigger_price": new_trigger_price if order_type in ["SL", "SL-M"] else 0,
                                            "price": new_price if order_type in ["LIMIT", "SL"] else 0
                                        }
                                        response = upstox_apis['order'].modify_order(modify_data,
                                                                                     api_version="2.0").data
                                    else:
                                        response = kite_apis["kite"].modify_order(
                                            variety=kite_apis["kite"].VARIETY_AMO if 'AMO' in order_data["Status"]
                                            else kite_apis["kite"].VARIETY_REGULAR,
                                            order_id=selected_order_id,
                                            quantity=new_quantity,
                                            price=new_price if order_type in ["LIMIT", "SL"] else 0,
                                            trigger_price=new_trigger_price if order_type in ["SL", "SL-M"] else 0,
                                            order_type=order_type,
                                            validity="DAY"
                                        )
                                    if response:
                                        st.success(f"Order modified successfully! {response}")
                                    else:
                                        st.error("Failed to modify order.")
                                except Exception as e:
                                    st.error(f"Failed to modify {broker} order: {e}")

        else:
            st.info("No orders found")

        if not db_orders.empty:
            st.write('Database Orders')
            st.dataframe(db_orders.sort_values(by="OrderTimestamp", ascending=False))
        else:
            st.info("No orders found in database")

    with tabs[1]:
        st.subheader("Scheduled Orders")
        scheduled_orders = manage_scheduled_orders()
        if scheduled_orders:
            scheduled_df = pd.DataFrame(scheduled_orders)
            st.dataframe(scheduled_df)
            selected_scheduled_order = st.selectbox("Select Scheduled Order to Modify/Cancel", options=scheduled_df["ScheduledOrderID"])
            if selected_scheduled_order:
                order = next(o for o in scheduled_orders if o["ScheduledOrderID"] == selected_scheduled_order)
                new_quantity = st.number_input("New Quantity", min_value=1, value=order["Quantity"], key="new_qty")
                new_price = st.number_input("New Price", min_value=0.0, value=float(order["Price"]))
                new_trigger_price = st.number_input("New Trigger Price", min_value=0.0, value=float(order["TriggerPrice"]))
                new_schedule_time = st.date_input("New Schedule Time", value=order["ScheduleDateTime"], min_value=datetime.now())
                new_stop_loss = st.number_input("New Stop-Loss", min_value=0.0, value=order["StopLoss"])
                new_target = st.number_input("New Target", min_value=0.0, value=order["Target"])
                col_mod, col_can = st.columns(2)
                with col_mod:
                    if st.button("Modify Scheduled Order"):
                        update_scheduled_order(selected_scheduled_order, new_quantity, new_price, new_trigger_price,
                                              new_schedule_time, new_stop_loss, new_target)
                        st.success("Scheduled order updated")
                with col_can:
                    if st.button("Cancel Scheduled Order"):
                        query = f"UPDATE NSEDATA.dbo.ScheduledOrders SET Status = 'cancelled' WHERE ScheduledOrderID = '{selected_scheduled_order}'"
                        with create_connection(DATABASE).connect() as conn:
                            conn.execute(text(query))
                            conn.commit()
                        st.success("Scheduled order cancelled")
        else:
            st.info("No scheduled orders")

    with tabs[2]:
        st.write("##### Add New Auto Order")
        auto_cols = st.columns(6)
        auto_stock_symbol = auto_cols[0].selectbox("Select Symbol", options=instruments.keys(), key="auto_symbol")
        auto_instrument_token = instruments.get(auto_stock_symbol)
        auto_transaction_type = auto_cols[1].radio("Transaction Type", ["BUY", "SELL"], horizontal=True,
                                                  key="auto_trans_type")
        auto_order_type = auto_cols[2].selectbox("Order Type", ["MARKET", "LIMIT"], key="auto_order_type")
        auto_product_type = auto_cols[3].selectbox("Product Type", ["I", "D"], key="auto_product_type")

        if auto_order_type == "LIMIT":
            live_data = get_market_quote(upstox_apis['market_data'], auto_instrument_token)
            current_ltp = live_data.get("ltp", 0) if live_data else 0
            auto_limit_price = auto_cols[5].number_input("Limit Price", min_value=0.05, value=current_ltp,
                                                        step=0.05, key="auto_limit_price")
        else:
            auto_limit_price = 0.0

        auto_risk_per_trade = auto_cols[4].number_input("Risk per Trade (%)", min_value=0.1, value=1.0,
                                                       step=0.1, key="auto_risk")
        auto_stop_loss_type = auto_cols[0].selectbox("Stop Loss Type",
                                                    ["Fixed Amount", "Percentage of Entry", "ATR Based"],
                                                    key="auto_sl_type")
        if auto_stop_loss_type == "Fixed Amount":
            auto_stop_loss_value = auto_cols[1].number_input("Stop Loss Value (Rs.)", min_value=1.0, value=100.0,
                                                            step=1.0, key="auto_sl_fixed")
            auto_target_value = auto_cols[2].number_input("Target Value (Rs.)", min_value=1.0, value=250.0, step=1.0,
                                                key="auto_target_fixed")
        elif auto_stop_loss_type == "Percentage of Entry":
            auto_stop_loss_percent = auto_cols[1].number_input("Stop Loss (%)", min_value=0.1, value=1.0, step=0.1,
                                                     key="auto_sl_pct")
            auto_target_percent = auto_cols[2].number_input("Target (%)", min_value=0.1, value=2.5, step=0.1,
                                                  key="auto_target_pct")
        else:
            auto_atr_period = auto_cols[1].number_input("ATR Period", min_value=5, value=14,
                                                       step=1, key="auto_atr_period")
            auto_stop_loss_atr_mult = auto_cols[2].number_input("Stop Loss ATR Multiplier", min_value=0.5,
                                                               value=2.0, step=0.5, key="auto_sl_atr")
            auto_target_atr_mult = auto_cols[3].number_input("Target ATR Multiplier", min_value=0.5, value=5.0,
                                                            step=0.5, key="auto_target_atr")

        backtest_data = get_table_data(selected_database=DATABASE, selected_table="BacktestResults",
                                      query=f"SELECT * FROM NSEDATA.dbo.BacktestResults WHERE "
                                            f"InstrumentToken = '{auto_instrument_token}' ORDER BY BacktestDate DESC")
        if not backtest_data.empty:
            latest_backtest = backtest_data.iloc[0]
            st.write(
                f"Backtest Suggestion for {auto_stock_symbol}: "
                f"SL={latest_backtest['OptimalStopLoss']}, Target={latest_backtest['OptimalTarget']},"
                f" WinRate={latest_backtest['WinRate']}%")

        if st.button("Add Auto Order"):
            auto_order = {
                "InstrumentToken": auto_instrument_token,
                "TransactionType": auto_transaction_type,
                "RiskPerTrade": auto_risk_per_trade,
                "StopLossType": auto_stop_loss_type,
                "StopLossValue": auto_stop_loss_value if auto_stop_loss_type == "Fixed Amount" else (
                    auto_stop_loss_percent if auto_stop_loss_type == "Percentage of Entry" else auto_stop_loss_atr_mult),
                "TargetValue": auto_target_value if auto_stop_loss_type == "Fixed Amount" else (
                    auto_target_percent if auto_stop_loss_type == "Percentage of Entry" else auto_target_atr_mult),
                "ATRPeriod": auto_atr_period if auto_stop_loss_type == "ATR Based" else 14,
                "ProductType": auto_product_type,
                "OrderType": auto_order_type,
                "LimitPrice": auto_limit_price if auto_order_type == "LIMIT" else 0.0
            }
            auto_order_df = pd.DataFrame([auto_order])
            load_sql_data(auto_order_df, "AutoOrders", load_type="append", index_required=False, database=DATABASE)
            st.success(f"Auto order added for {auto_stock_symbol}")

        st.write("##### Manage Existing Auto Orders")
        auto_orders_df = get_table_data(selected_database=DATABASE, selected_table="AutoOrders")
        if not auto_orders_df.empty:
            st.session_state["auto_orders"] = auto_orders_df.to_dict('records')
            auto_orders_df_display = auto_orders_df.copy()
            auto_orders_df_display["Symbol"] = auto_orders_df_display["InstrumentToken"].map(
                lambda token: next((k for k, v in instruments.items() if v == token), "Unknown")
            )
            st.dataframe(auto_orders_df_display)

            selected_auto_order_id = st.selectbox("Select Auto Order to Modify/Delete",
                                                 options=auto_orders_df["AutoOrderID"])
            if selected_auto_order_id:
                selected_order = auto_orders_df[auto_orders_df["AutoOrderID"] == selected_auto_order_id].iloc[0]

                st.write("##### Modify Auto Order")
                mod_transaction_type = st.radio("Transaction Type", ["BUY", "SELL"],
                                               index=0 if selected_order["TransactionType"] == "BUY" else 1,
                                               key="mod_auto_trans_type")
                mod_order_type = st.selectbox("Order Type", ["MARKET", "LIMIT"],
                                             index=0 if selected_order["OrderType"] == "MARKET" else 1,
                                             key="mod_auto_order_type")
                mod_product_type = st.selectbox("Product Type", ["I", "D"],
                                               index=0 if selected_order["ProductType"] == "I" else 1,
                                               key="mod_auto_product_type")
                if mod_order_type == "LIMIT":
                    mod_limit_price = st.number_input("Limit Price",
                                                      min_value=0.05,
                                                      value=float(selected_order["LimitPrice"]) or
                                                            get_market_quote(
                                                                upstox_apis['market_data'],
                                                                selected_order["InstrumentToken"]).get("ltp", 0),
                                                      step=0.05,
                                                      key="mod_auto_limit_price")
                else:
                    mod_limit_price = 0.0
                mod_risk_per_trade = st.number_input("Risk per Trade (%)", min_value=0.1,
                                                    value=float(selected_order["RiskPerTrade"]), step=0.1,
                                                    key="mod_auto_risk")
                mod_stop_loss_type = st.selectbox("Stop Loss Type",
                                                 ["Fixed Amount", "Percentage of Entry", "ATR Based"],
                                                 index=["Fixed Amount", "Percentage of Entry", "ATR Based"].index(
                                                     selected_order["StopLossType"]), key="mod_auto_sl_type")
                if mod_stop_loss_type == "Fixed Amount":
                    mod_stop_loss_value = st.number_input("Stop Loss Value (Rs.)", min_value=1.0,
                                                         value=float(selected_order["StopLossValue"]), step=1.0,
                                                         key="mod_auto_sl_fixed")
                    mod_target_value = st.number_input("Target Value (Rs.)", min_value=1.0,
                                                      value=float(selected_order["TargetValue"]), step=1.0,
                                                      key="mod_auto_target_fixed")
                elif mod_stop_loss_type == "Percentage of Entry":
                    mod_stop_loss_percent = st.number_input("Stop Loss (%)", min_value=0.1,
                                                           value=float(selected_order["StopLossValue"]), step=0.1,
                                                           key="mod_auto_sl_pct")
                    mod_target_percent = st.number_input("Target (%)", min_value=0.1,
                                                        value=float(selected_order["TargetValue"]), step=0.1,
                                                        key="mod_auto_target_pct")
                else:
                    mod_atr_period = st.number_input("ATR Period", min_value=5, value=int(selected_order["ATRPeriod"]),
                                                    step=1, key="mod_auto_atr_period")
                    mod_stop_loss_atr_mult = st.number_input("Stop Loss ATR Multiplier", min_value=0.5,
                                                            value=float(selected_order["StopLossValue"]), step=0.5,
                                                            key="mod_auto_sl_atr")
                    mod_target_atr_mult = st.number_input("Target ATR Multiplier", min_value=0.5,
                                                         value=float(selected_order["TargetValue"]), step=0.5,
                                                         key="mod_auto_target_atr")

                col_mod, col_del = st.columns(2)
                with col_mod:
                    if st.button("Modify Auto Order"):
                        update_query = f"""
                            UPDATE NSEDATA.dbo.AutoOrders
                            SET TransactionType = '{mod_transaction_type}',
                                OrderType = '{mod_order_type}',
                                ProductType = '{mod_product_type}',
                                LimitPrice = {mod_limit_price},
                                RiskPerTrade = {mod_risk_per_trade},
                                StopLossType = '{mod_stop_loss_type}',
                                StopLossValue = {mod_stop_loss_value if mod_stop_loss_type == "Fixed Amount" else (
                            mod_stop_loss_percent if mod_stop_loss_type == "Percentage of Entry" 
                            else mod_stop_loss_atr_mult)},
                                TargetValue = {mod_target_value if mod_stop_loss_type == "Fixed Amount" 
                        else (mod_target_percent if mod_stop_loss_type == "Percentage of Entry"
                              else mod_target_atr_mult)},
                                ATRPeriod = {mod_atr_period if mod_stop_loss_type == "ATR Based" else 14}
                            WHERE AutoOrderID = {selected_auto_order_id}
                        """
                        with create_connection(DATABASE).connect() as conn:
                            conn.execute(text(update_query))
                            conn.commit()
                        st.success(f"Auto order {selected_auto_order_id} modified")

                with col_del:
                    if st.button("Delete Auto Order"):
                        delete_query = text("DELETE FROM NSEDATA.dbo.AutoOrders WHERE AutoOrderID = :id")
                        with create_connection(DATABASE).connect() as conn:
                            conn.execute(delete_query, {"id": selected_auto_order_id})
                            conn.commit()
                        st.success(f"Auto order {selected_auto_order_id} deleted")

        else:
            st.session_state["auto_orders"] = []
            st.info("No auto orders found")

        auto_orders = st.session_state.get("auto_orders", [])
        if auto_orders:
            total_risk = sum(order["RiskPerTrade"] for order in auto_orders)
            st.write(f"Total Risk Exposure: {total_risk:.2f}%")
            if st.button("Run Auto Orders Now"):
                with st.spinner("Calculating risk..."):
                    api = upstox_apis["order"] if broker == "Upstox" else kite_apis['kite']
                    result = run_auto_orders(api, broker)
                    st.write(result)

    with tabs[3]:
        st.subheader("Running Threads")
        threads = get_running_threads()
        if threads:
            threads_df = pd.DataFrame(threads)
            st.dataframe(threads_df)
            st.write(f"Total Active Threads: {len(threads)}")
            selected_thread_name = st.selectbox("Select Thread to Stop", options=[t["Name"] for t in threads])
            if st.button("Stop Selected Thread"):
                if selected_thread_name in st.session_state["thread_registry"]:
                    stop_event = st.session_state["thread_registry"][selected_thread_name]["stop_event"]
                    stop_event.set()
                    st.session_state["thread_registry"][selected_thread_name]["thread"].join(timeout=2)
                    if not st.session_state["thread_registry"][selected_thread_name]["thread"].is_alive():
                        st.session_state["thread_registry"].pop(selected_thread_name)
                    st.success(f"Thread {selected_thread_name} stopped or signaled to stop")
                else:
                    st.error("Thread not found in registry")
        else:
            st.info("No active threads found")

elif page == "Positions":
    st.subheader("Current Positions")
    positions_df = get_positions(upstox_apis["portfolio"], kite_apis['kite'])
    if not positions_df.empty:
        total_investment = positions_df['Avg. Price'] * positions_df['Quantity']
        total_pnl = positions_df['P&L'].sum()
        total_value = positions_df['Last Price'] * positions_df['Quantity']

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Investment", f"₹{total_investment.sum():.2f}")
        col2.metric("Total P&L", f"₹{total_pnl:.2f}",
                    f"{(total_pnl / total_investment.sum() * 100 if total_investment.sum() else 0):.2f}%")
        col3.metric("Current Value", f"₹{total_value.sum():.2f}")

        st.dataframe(positions_df, use_container_width=True)

        st.subheader("Position Actions")
        for idx, row in positions_df.iterrows():
            col1, col2 = st.columns([3, 1])
            col1.write(
                f"{row['Broker']} - {row['Symbol']} - {row['Quantity']} @ {row['Avg. Price']} ({row['Product']})")
            if col2.button("Square Off", key=f"squareoff_{idx}"):
                try:
                    api = upstox_apis['order'] if row['Broker'] == "Upstox" else kite_apis['kite']
                    order_response = place_order(
                        row['Broker'],
                        api,
                        row['Instrument Token'],
                        quantity=abs(row['Quantity']),
                        price=0,
                        order_type="MARKET",
                        transaction_type="SELL" if row['Quantity'] > 0 else "BUY",
                        product_type=row['Product']
                    )
                    if order_response:
                        order_id = order_response.data.order_id if row['Broker'] == "Upstox" else order_response
                        st.success(f"Square off order placed! Order ID: {order_id}")
                    else:
                        st.error("Failed to place square off order.")
                except Exception as e:
                    st.error(f"Error placing {row['Broker']} square off order: {str(e)}")
    else:
        st.info("No positions found")

elif page == "Trade Dashboard":
    st.subheader("Trade Dashboard")

    # Open Positions
    st.write("##### Open Positions")
    positions_df = get_positions(upstox_apis["portfolio"], kite_apis['kite'])
    if not positions_df.empty:
        positions_df["UnrealizedPnL"] = positions_df.apply(
            lambda row: (get_market_quote(upstox_apis['market_data'], row["instrument_token"])["ltp"] - row["average_price"]) * row["quantity"]
            if row["quantity"] != 0 else 0, axis=1
        )
        st.dataframe(positions_df[["instrument_token", "quantity", "average_price", "UnrealizedPnL"]])
        total_unrealized_pnl = positions_df["UnrealizedPnL"].sum()
        st.metric("Total Unrealized P&L", f"₹{total_unrealized_pnl:.2f}")
    else:
        st.info("No open positions")

    # Recent Trades
    st.write("##### Recent Trades")
    recent_trades = get_table_data(selected_database=DATABASE, selected_table="TradeHistory",
                                   query="SELECT TOP 10 * FROM NSEDATA.dbo.TradeHistory ORDER BY ExitTime DESC")
    if not recent_trades.empty:
        st.dataframe(recent_trades)
        total_realized_pnl = recent_trades["Pnl"].sum()
        win_rate = len(recent_trades[recent_trades["Pnl"] > 0]) / len(recent_trades) * 100
        avg_trade_duration = (recent_trades["ExitTime"] - recent_trades["EntryTime"]).mean().total_seconds() / 60
        st.metric("Total Realized P&L", f"₹{total_realized_pnl:.2f}")
        st.metric("Win Rate", f"{win_rate:.2f}%")
        st.metric("Avg Trade Duration (min)", f"{avg_trade_duration:.2f}")
    else:
        st.info("No recent trades")

    # P&L Chart
    st.write("##### P&L Trend")
    all_trades = get_table_data(selected_database=DATABASE, selected_table="TradeHistory",
                                query="SELECT ExitTime, Pnl FROM NSEDATA.dbo.TradeHistory ORDER BY ExitTime")
    if not all_trades.empty:
        chart = alt.Chart(all_trades).mark_line().encode(
            x="ExitTime:T",
            y="Pnl:Q",
            tooltip=["ExitTime", "Pnl"]
        ).interactive()
        st.altair_chart(chart, use_container_width=True)

    st.write("##### Portfolio Risk Distribution")
    funds_data = get_user_profile_and_funds(upstox_apis["user"], user_profile=False)
    available_margin = funds_data["data"]["equity"]["available_margin"] if funds_data else 1
    if not positions_df.empty:
        positions_df["Risk"] = positions_df["UnrealizedPnL"].abs() / available_margin * 100
    else:
        positions_df = pd.DataFrame()
    auto_risk_df = pd.DataFrame(st.session_state.get("auto_orders", []))
    if not positions_df.empty or not auto_risk_df.empty:

        if not auto_risk_df.empty:
            risk_df = pd.concat([
                positions_df[["instrument_token", "Risk"]].rename(columns={"instrument_token": "Instrument"}),
                auto_risk_df[["InstrumentToken", "RiskPerTrade"]].rename(
                    columns={"InstrumentToken": "Instrument", "RiskPerTrade": "Risk"})
            ])
        else:
            risk_df = pd.concat([
                positions_df[["instrument_token", "Risk"]].rename(columns={"instrument_token": "Instrument"})
            ])
        chart = alt.Chart(risk_df).mark_bar().encode(
            x="Instrument:N",
            y="Risk:Q",
            tooltip=["Instrument", "Risk"]
        ).interactive()
        st.altair_chart(chart, use_container_width=True)

elif page == "Portfolio":
    st.subheader("Portfolio Overview")
    portfolio_df = get_portfolio(upstox_apis["portfolio"], kite_apis['kite'])
    if not portfolio_df.empty:
        total_value = (portfolio_df["Last Price"] * portfolio_df["Quantity"]).sum()
        total_buy_value = (portfolio_df["Avg. Price"] * portfolio_df["Quantity"]).sum()
        total_pnl = (portfolio_df["Last Price"] - portfolio_df["Avg. Price"]) * portfolio_df["Quantity"]
        today_pnl = portfolio_df["Day Change"].sum()
        portfolio_df['Buy Value'] = portfolio_df['Avg. Price'] * portfolio_df['Quantity']
        portfolio_df['Market Value'] = portfolio_df['Last Price'] * portfolio_df['Quantity']
        st.dataframe(
            portfolio_df[["Broker", "Symbol", "Exchange", "Quantity", "Last Price", "Avg. Price", "Buy Value",
                          "Market Value", "P&L", 'Day Change', 'Day Change %']]
            .style
            .highlight_max(subset=["Buy Value", "Market Value", "P&L"],
                            color='#3ee27a')
            .highlight_min(subset=["Buy Value", "Market Value", "P&L"],
                            color="#ea3c34")
            .format(precision=2, thousands=",")
            .background_gradient(cmap='RdYlGn', subset=['Day Change', 'Day Change %']),
            hide_index=True,
            use_container_width=True,)
        columns = st.columns(4)
        columns[0].metric('Total Buy Value', f'₹ {total_buy_value:.2f}')
        columns[1].metric('Total Portfolio Value', f'₹ {total_value:.2f}')
        columns[2].metric('Total P&L', f'₹ {total_pnl.sum():.2f}', f'{(total_pnl.sum() / total_buy_value * 100):.2f}%')
        columns[3].metric('Day Change', f'₹ {today_pnl:.2f}', f'{(today_pnl / total_value * 100):.2f}%')
    else:
        st.info("No holdings found")

    # Fetch and display zerodha mutual funds portfolio
    st.subheader("Mutual Funds Portfolio - Zerodha")
    mf_portfolio = pd.DataFrame(kite_apis['kite'].mf_holdings())
    mf_portfolio['current_value'] = mf_portfolio['quantity'] * mf_portfolio['last_price']
    mf_portfolio["pnl"] = (mf_portfolio["last_price"] - mf_portfolio["average_price"]) * mf_portfolio["quantity"]
    mf_portfolio["pnl %"] = (mf_portfolio["pnl"] / (mf_portfolio["average_price"] * mf_portfolio["quantity"])) * 100
    if not mf_portfolio.empty:
        st.dataframe(mf_portfolio[["fund", "quantity", "average_price", "last_price", "current_value", "pnl", "pnl %"]]
                     .style
                     .highlight_max(subset=["current_value", "pnl"],
                                    color='#3ee27a')
                     .highlight_min(subset=["current_value", "pnl"],
                                    color="#ea3c34")
                     .format(precision=2, thousands=",")
                     .background_gradient(cmap='RdYlGn', subset=["pnl %"]),
                     hide_index=True,
                     use_container_width=True,
                     )
        total_mf_buy_value = (mf_portfolio["average_price"] * mf_portfolio["quantity"]).sum()
        total_mf_value = (mf_portfolio["current_value"]).sum()
        total_mf_pnl = (mf_portfolio["pnl"]).sum()
        mf_cols = st.columns(3)
        mf_cols[0].metric("Total Mutual Funds Buy Value", f"₹{total_mf_buy_value:.2f}")
        mf_cols[1].metric("Total Mutual Funds Value", f"₹{total_mf_value:.2f}")
        mf_cols[2].metric("Total Mutual Funds P&L", f"₹{total_mf_pnl:.2f}", f"{(total_mf_pnl / total_mf_buy_value * 100):.2f}%")
    else:
        st.info("No mutual funds found")

elif page == "Analytics":
    st.write("Trade Analytics & Live Feed")
    analytics_cols = st.columns(5)
    stock_symbol = analytics_cols[0].selectbox("Select Symbol", options=instruments.keys(), key='symbol_analysis')
    instrument_token = instruments.get(stock_symbol)
    timeframe = analytics_cols[1].selectbox("Timeframe", ["1minute", "day", "week", "month", "30minute"], index=1)
    ema_period = analytics_cols[2].number_input("EMA Period", min_value=5, value=20, max_value=200)
    lr_period = analytics_cols[3].number_input("LR Period", min_value=5, value=20, max_value=200)
    rsi_period = analytics_cols[4].number_input("RSI Period", min_value=5, value=14, max_value=50)
    show_columns = st.columns(5)
    show_sr = show_columns[0].checkbox("Show Support & Resistance")
    show_trend = show_columns[1].checkbox("Show Trend Lines")
    show_ema = show_columns[2].checkbox("Show EMA")
    show_lr = show_columns[3].checkbox("Show Linear Regression")
    show_rsi = show_columns[4].checkbox("Show RSI")
    timeframe_mapping = {
        "1minute": "minutes",
        "day": "days",
        "week": "weeks",
        "month": "months",
        "30minute": "minutes"
    }
    api_timeframe = timeframe_mapping.get(timeframe, "minutes")
    data = get_historical_data(instrument_token, api_timeframe)
    if data is not None:
        chart = StreamlitChart(height=600, toolbox=True, scale_candles_only=True)
        chart_data = data.rename(columns={"timestamp": "time", "open": "open", "high": "high", "low": "low", "close": "close"})
        chart_data.sort_values(by='time', inplace=True)
        chart.set(chart_data)
        chart.legend(True, color_based_on_candle=True, font_size=22, font_family='sans-serif')
        chart.watermark(stock_symbol)
        if show_sr:
            support = data["low"].min()
            resistance = data["high"].max()
            chart.horizontal_line(support, color="green", style='dashed', text="Support")
            chart.horizontal_line(resistance, color="red", style='dashed', text="Resistance")
        if show_trend:
            trend_start = {"time": data["timestamp"].iloc[0], "value": data["close"].iloc[0]}
            trend_end = {"time": data["timestamp"].iloc[60], "value": data["close"].iloc[60]}
            chart.trend_line(trend_start['time'], trend_start['value'], trend_end['time'], trend_end['value'], line_color="blue")
        if show_ema:
            ema = calculate_ema(data, ema_period)
            latest_ema = ema.iloc[-1]
            chart.marker(data["timestamp"].iloc[-1], color="orange", text="EMA")
            st.write(f"Latest EMA ({ema_period}): {latest_ema:.2f}")
        if show_lr:
            lr = calculate_linear_regression(data, lr_period)
            latest_lr = lr.iloc[-1]
            chart.marker(data["timestamp"].iloc[-1], color="purple", text='LR')
            st.write(f"Latest Linear Regression ({lr_period}): {latest_lr:.2f}")
        if show_rsi:
            rsi = calculate_rsi(data, rsi_period)
            rsi_data = pd.DataFrame({"time": data["timestamp"], "rsi": rsi}).dropna()
            rsi_line = chart.create_line(name="RSI", color="blue")
            rsi_line.set(rsi_data)
            latest_rsi = rsi.iloc[-1]
            st.write(f"Latest RSI ({rsi_period}): {latest_rsi:.2f}")
        chart.load()

elif page == "Algo Trading":
    st.subheader("Algorithmic Trading")
    col1, col2 = st.columns(2)
    with col1:
        strategy = st.selectbox("Select Strategy", ["MACD Crossover", "Bollinger Bands", "RSI Oversold/Overbought", "Stochastic Oscillator", "Support/Resistance Breakout"])
        stock_symbol = st.selectbox("Select Symbol", options=instruments.keys(), key='symbol_algo')
        instrument_token = instruments.get(stock_symbol)
        quantity = st.number_input("Quantity", min_value=1, value=1)
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
    st.subheader("Strategy Description")
    if strategy == "MACD Crossover":
        st.markdown("""**MACD Crossover Strategy**...""")  # Truncated for brevity
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Check Current Signal"):
            with st.spinner("Analyzing market data..."):
                hist_data = get_historical_data(instrument_token)
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
                    result = auto_trade(upstox_apis["order"], strategy, instrument_token, quantity, stop_loss, take_profit)
                    st.success(result)
        else:
            if st.button("Start Automated Trading"):
                run_hours = [(start_hour, end_hour)]
                result = schedule_strategy_execution(upstox_apis["order"], strategy, instrument_token, quantity, interval, run_hours)
                st.success(f"Automated trading started. Checking every {interval} minutes during market hours.")
                st.warning("Warning: Automated trading continues until the app is closed or you navigate away.")

elif page == "Strategy Backtest":
    st.subheader("Strategy Backtesting")
    stock_symbol = st.selectbox("Select Symbol", options=instruments.keys(), key='symbol_backtest')
    instrument_token = instruments.get(stock_symbol)
    timeframe = st.selectbox("Timeframe", ["day", "week", "1minute", "5minute", "30minute"], index=0)
    strategy = st.selectbox("Select Strategy to Backtest",
                            ["Short Sell Optimization", "MACD Crossover", "Bollinger Bands", "RSI Strategy"])

    if strategy == "Short Sell Optimization":
        st.write("Backtest and optimize a short-selling strategy using ATR-based stop-loss and target.")
        stocks = ['GOLDBEES', 'JUNIORBEES', 'ICICIB22', 'CPSEETF', 'ITBEES', 'MID150BEES', 'MON100', 'MAFANG',
                  'HDFCSML250']
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
                    data = get_table_data(query=query)
                    if not data.empty:
                        df = pd.DataFrame(data)
                        date_lists[stock] = df['Date']
                        optimized_results[stock] = backtest_etf.optimize_parameters(data, stock,
                                                                                    initial_investment_range,
                                                                                    stop_loss_atr_mult_values,
                                                                                    target_atr_mult_values)
                    else:
                        st.error(f"No data found for {stock}")
                if optimized_results:
                    for stock, result in optimized_results.items():
                        st.write(f"##### Optimized Results for {stock}")
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
                        st.write("##### Yearly Summary")
                        st.dataframe(result['Yearly Summary'])
                    st.write("##### Portfolio Value Over Time")
                    chart_data = pd.DataFrame()
                    for stock, result in optimized_results.items():
                        dates = date_lists[stock]
                        portfolio_values = result['Portfolio Value']
                        df = pd.DataFrame(
                            {'Date': dates, 'Portfolio Value': portfolio_values, 'Stock': [stock] * len(dates)})
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
                hist_data = get_historical_data(instrument_token, timeframe)
                if hist_data is not None:
                    backtest_results = backtest_strategy(hist_data, strategy_func, **strategy_params)
                    st.subheader("Backtest Results")
                    total_trades = backtest_results['signal'].value_counts().sum()
                    profitable_trades = len(backtest_results[backtest_results['pnl'] > 0])
                    win_rate = profitable_trades / total_trades * 100 if total_trades > 0 else 0
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total Trades", total_trades)
                    col2.metric("Win Rate", f"{win_rate:.2f}%")
                    col3.metric("Total P&L", f"₹{backtest_results['cumulative_pnl'].iloc[-1]:.2f}")

                    st.subheader("Performance Chart")
                    chart_data = pd.DataFrame({
                        'Date': backtest_results['timestamp'],
                        'Close Price': backtest_results['close'],
                        'Cumulative P&L': backtest_results['cumulative_pnl']
                    })
                    buy_signals = backtest_results[backtest_results['signal'] == 'BUY']
                    sell_signals = backtest_results[backtest_results['signal'] == 'SELL']

                    # Price Chart with Buy/Sell Signals
                    price_chart = alt.Chart(chart_data).mark_line().encode(
                        x='Date:T',
                        y=alt.Y('Close Price:Q', scale=alt.Scale(zero=False)),
                        color=alt.value('#336699'),
                        tooltip=['Date:T', 'Close Price:Q']
                    ).properties(
                        width=800,
                        height=300,
                        title=f'{strategy} - Price and Signals'
                    )

                    buy_points = alt.Chart(buy_signals).mark_point(
                        color='green',
                        size=100,
                        shape='triangle-up'
                    ).encode(
                        x='timestamp:T',
                        y='close:Q'
                    )

                    sell_points = alt.Chart(sell_signals).mark_point(
                        color='red',
                        size=100,
                        shape='triangle-down'
                    ).encode(
                        x='timestamp:T',
                        y='close:Q'
                    )

                    # Cumulative P&L Chart
                    pnl_chart = alt.Chart(chart_data).mark_line().encode(
                        x='Date:T',
                        y=alt.Y('Cumulative P&L:Q', scale=alt.Scale(zero=False)),
                        color=alt.value('#4CAF50'),
                        tooltip=['Date:T', 'Cumulative P&L:Q']
                    ).properties(
                        width=800,
                        height=200,
                        title='Cumulative Profit & Loss'
                    )

                    # Combine charts
                    combined_chart = alt.layer(price_chart, buy_points, sell_points) & pnl_chart
                    st.altair_chart(combined_chart, use_container_width=True)

                    # Trade Log
                    st.subheader("Trade Log")
                    trade_log = backtest_results[['timestamp', 'close', 'signal', 'pnl', 'cumulative_pnl']].dropna(
                        subset=['signal'])
                    trade_log.columns = ['Date', 'Price', 'Signal', 'P&L', 'Cumulative P&L']
                    st.dataframe(trade_log)

                    # Download results
                    csv = backtest_results.to_csv(index=False)
                    st.download_button(
                        label="Download Backtest Results",
                        data=csv,
                        file_name=f"{stock_symbol}_{strategy}_backtest.csv",
                        mime="text/csv"
                    )
                else:
                    st.error("Failed to fetch historical data.")

if enable_debug:
    st.json(st.session_state)