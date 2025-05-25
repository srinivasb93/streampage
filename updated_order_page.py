import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import threading
import schedule
import time
import logging
from queue import Queue
import uuid
from upstox_client.rest import ApiException
import upstox_client
from dotenv import load_dotenv
import os
from lightweight_charts.widgets import StreamlitChart
import altair as alt
from websocket_manager import initialize_websocket, subscribe_to_instrument, get_live_data, close_websocket
from common_utils.read_write_sql_data import get_table_data, load_sql_data
import requests

# Load environment variables
load_dotenv()
ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Custom CSS
st.set_page_config(page_title="Upstox Trading Dashboard", layout="wide")

# Validate access token
if not ACCESS_TOKEN:
    logger.error("Access token is missing in .env file.")
    st.error("Access token is missing. Please set UPSTOX_ACCESS_TOKEN in your .env file or use the 'Get Token' page to fetch a new token.")
    st.stop()

# Initialize session state
if "access_token" not in st.session_state:
    st.session_state.access_token = ACCESS_TOKEN
if "apis" not in st.session_state:
    config = upstox_client.Configuration()
    config.access_token = st.session_state.access_token
    api_client = upstox_client.ApiClient(config)
    st.session_state.apis = {
        "order": upstox_client.OrderApi(api_client),
        "portfolio": upstox_client.PortfolioApi(api_client),
        "history": upstox_client.HistoryApi(api_client),
        "user": upstox_client.UserApi(api_client)
    }
if "websocket_initialized" not in st.session_state:
    st.session_state.websocket_initialized = False
    st.session_state.websocket_error = None

    # Pass the access token directly to avoid accessing st.session_state in the thread
    access_token = st.session_state.access_token

    def start_websocket(token):
        try:
            logger.info("Starting WebSocket connection...")
            success = initialize_websocket(token)
            if success:
                logger.info("WebSocket initialized successfully.")
                # Subscribe to default instruments after connection
                default_instruments = ["NSE_EQ|INE002A01018"]  # Example: Reliance Industries
                for instrument in default_instruments:
                    subscribe_to_instrument(instrument)
                    logger.info(f"Subscribed to instrument: {instrument}")
                st.session_state.websocket_initialized = True
            else:
                logger.error("WebSocket initialization failed.")
                st.session_state.websocket_error = "Failed to initialize WebSocket."
        except Exception as e:
            logger.error(f"WebSocket initialization error: {e}")
            st.session_state.websocket_error = str(e)

    # Start WebSocket in a background thread with a timeout
    websocket_thread = threading.Thread(target=start_websocket, args=(access_token,), daemon=True)
    websocket_thread.start()
    websocket_thread.join(timeout=5)  # Wait for up to 5 seconds
    if not st.session_state.websocket_initialized and not st.session_state.websocket_error:
        st.session_state.websocket_error = "WebSocket initialization timed out."

if "instruments_data" not in st.session_state:
    try:
        path = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
        instruments_df = pd.read_json(path)
        st.session_state.instruments_data = dict(zip(
            instruments_df[(instruments_df['segment'] == 'NSE_EQ') & (instruments_df['instrument_type'] == 'EQ')]
            ['trading_symbol'].values.tolist(),
            instruments_df['instrument_key'].values.tolist()
        ))
    except Exception as e:
        logger.error(f"Failed to fetch instruments: {e}")
        st.session_state.instruments_data = {}
if "funds_data" not in st.session_state:
    try:
        st.session_state.funds_data = st.session_state.apis["user"].get_user_fund_margin(api_version="v2").to_dict()
    except ApiException as e:
        st.error(f"Failed to fetch funds data: {e}")
        st.session_state.funds_data = {}
if "auto_orders" not in st.session_state:
    st.session_state.auto_orders = []
if "scheduled_orders" not in st.session_state:
    st.session_state.scheduled_orders = Queue()


st.markdown("""
    <style>
    .main { background-color: #f5f5f5; padding: 20px; border-radius: 10px; margin-top: -5em; }
    .stButton>button { background-color: #4CAF50; color: white; border-radius: 5px; }
    .stTextInput>input { border-radius: 5px; }
    .sidebar .sidebar-content { background-color: #e0e0e0; padding: 10px; border-radius: 10px; }
    .metric-box { background-color: #336699; padding: 10px; border-radius: 5px; box-shadow: 2px 2px 5px rgba(0,0,0,0.1); }
    </style>
""", unsafe_allow_html=True)

# Helper Functions
def fetch_access_token(api_key, api_secret, redirect_uri, auth_code):
    url = "https://api.upstox.com/v2/login/authorization/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "authorization_code", "code": auth_code, "client_id": api_key, "client_secret": api_secret, "redirect_uri": redirect_uri}
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        token = response.json()["access_token"]
        st.session_state.access_token = token
        with open(".env", "a") as f:
            f.write(f"UPSTOX_ACCESS_TOKEN={token}\n")
        return token
    st.error(f"Failed to fetch token: {response.text}")
    return None

def place_order(instrument_token, transaction_type, quantity, price=0, order_type="MARKET", trigger_price=0, is_amo=False, product_type="D", validity="DAY", stop_loss=None, target=None):
    try:
        order = upstox_client.PlaceOrderRequest(
            quantity=quantity, product=product_type, validity=validity, price=price, tag="StreamlitOrder",
            instrument_token=instrument_token, order_type=order_type, transaction_type=transaction_type,
            disclosed_quantity=0, trigger_price=trigger_price, is_amo=is_amo
        )
        response = st.session_state.apis["order"].place_order(order, api_version="v2")
        order_id = response.data.order_id
        if stop_loss or target:
            monitor_order(order_id, stop_loss, target, instrument_token, quantity, transaction_type, product_type)
        return order_id
    except ApiException as e:
        st.error(f"Error placing order: {e}")
        return None

def monitor_order(order_id, stop_loss, target, instrument_token, quantity, transaction_type, product_type):
    def monitor():
        while True:
            status = st.session_state.apis["order"].get_order_details(order_id, api_version="v2").data.status
            if status in ["complete", "rejected", "cancelled"]:
                break
            time.sleep(1)
        if status == "complete" and (stop_loss or target):
            if stop_loss:
                sl_price = stop_loss if transaction_type == "BUY" else stop_loss * 1.002
                place_order(instrument_token, "SELL" if transaction_type == "BUY" else "BUY", quantity, order_type="SL-M", trigger_price=sl_price, product_type=product_type)
            if target:
                tp_price = target if transaction_type == "BUY" else target * 0.998
                place_order(instrument_token, "SELL" if transaction_type == "BUY" else "BUY", quantity, order_type="LIMIT", price=tp_price, product_type=product_type)
    threading.Thread(target=monitor, daemon=True).start()

def get_historical_data(instrument_token, interval="day", days=365):
    from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    to_date = datetime.now().strftime("%Y-%m-%d")
    try:
        response = st.session_state.apis["history"].get_historical_candle_data1(
            instrument_key=instrument_token, interval=interval, to_date=to_date, from_date=from_date, api_version="v2"
        )
        df = pd.DataFrame(response.data.candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        return df
    except ApiException as e:
        st.error(f"Error fetching historical data: {e}")
        return None

# Check for WebSocket errors
if st.session_state.websocket_error:
    st.error(f"WebSocket Error: {st.session_state.websocket_error}")
    st.warning("The app will continue without live data. You can still place orders and view historical data.")

# Sidebar
with st.sidebar:
    st.subheader("Navigation")
    page = st.radio("Go to", ["Order Management", "Order Book", "Positions", "Portfolio", "Analytics", "Algo Trading", "Strategy Backtest", "Get Token"])
    st.subheader("Settings")
    st.session_state.email_notifications = st.checkbox("Enable Email Notifications", value=False)
    if st.button("Refresh Dashboard"):
        st.rerun()

# Pages
if page == "Get Token":
    st.subheader("Fetch Upstox Access Token")
    auth_url = f"https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={API_KEY}&redirect_uri=https://api.upstox.com/v2/login"
    st.markdown(f"[Click here to authorize]({auth_url})")
    auth_code = st.text_input("Authorization Code")
    if st.button("Fetch Token") and auth_code:
        token = fetch_access_token(API_KEY, API_SECRET, "https://api.upstox.com/v2/login", auth_code)
        if token:
            st.success(f"Token fetched: {token}")

elif page == "Order Management":
    st.subheader("Order Management")
    funds_cols = st.columns([2, 1, 2])
    with funds_cols[0]:
        try:
            st.markdown(f'<div class="metric-box">Funds: ₹{st.session_state.funds_data["data"]["equity"]["available_margin"]:.2f}</div>', unsafe_allow_html=True)
        except:
            st.markdown('<div class="metric-box">Funds data unavailable</div>', unsafe_allow_html=True)

    num_orders = funds_cols[2].number_input("Number of Orders", min_value=1, max_value=10, value=1)
    orders = []

    for i in range(num_orders):
        st.markdown(f":rainbow[Order {i + 1}]")
        cols = st.columns(9)
        symbol = cols[0].selectbox(f"Symbol {i + 1}", options=st.session_state.instruments_data.keys(), key=f"sym_{i}")
        instrument_token = st.session_state.instruments_data[symbol]
        subscribe_to_instrument(instrument_token)

        with st.expander("Position Sizing", expanded=False):
            capital = st.number_input("Capital", min_value=1000, value=50000, key=f"cap_{i}")
            risk = st.number_input("Risk %", min_value=0.1, value=1.0, key=f"risk_{i}")
            stop_loss_type = st.selectbox("Stop Loss Type", ["Fixed", "Percent", "ATR"], key=f"sl_type_{i}")
            live_data = get_live_data(instrument_token) if st.session_state.websocket_initialized else {"ltp": 0}
            entry_price = live_data.get("ltp", 0)
            if stop_loss_type == "Fixed":
                sl = st.number_input("Stop Loss", min_value=1.0, value=100.0, key=f"sl_fixed_{i}")
                target = st.number_input("Target", min_value=1.0, value=250.0, key=f"target_fixed_{i}")
                quantity = int((capital * risk / 100) / sl)
            elif stop_loss_type == "Percent":
                sl_pct = st.number_input("Stop Loss %", min_value=0.1, value=1.0, key=f"sl_pct_{i}")
                target_pct = st.number_input("Target %", min_value=0.1, value=2.5, key=f"target_pct_{i}")
                sl = entry_price * (sl_pct / 100)
                target = entry_price * (target_pct / 100)
                quantity = int((capital * risk / 100) / sl)
            else:  # ATR
                atr_period = st.number_input("ATR Period", min_value=5, value=14, key=f"atr_period_{i}")
                sl_mult = st.number_input("SL Multiplier", min_value=0.5, value=2.0, key=f"sl_mult_{i}")
                target_mult = st.number_input("Target Multiplier", min_value=0.5, value=5.0, key=f"target_mult_{i}")
                hist_data = get_historical_data(instrument_token)
                atr = hist_data["close"].pct_change().rolling(atr_period).std().iloc[-1] if hist_data is not None else 0
                sl = atr * sl_mult
                target = atr * target_mult
                quantity = int((capital * risk / 100) / sl) if atr > 0 else 1
            quantity = max(1, quantity)
            st.write(f"Quantity: {quantity}, SL: {entry_price - sl:.2f}, Target: {entry_price + target:.2f}")

        ltp = live_data.get("ltp", 0) if st.session_state.websocket_initialized else "N/A"
        cols[1].markdown(f":rainbow[LTP: {ltp}]")
        quantity_input = cols[2].number_input(f"Quantity {i + 1}", min_value=1, value=quantity, key=f"qty_{i}")
        order_type = cols[3].selectbox(f"Order Type {i + 1}", ["MARKET", "LIMIT", "SL", "SL-M"], key=f"type_{i}")
        transaction_type = cols[4].radio(f"Trans. Type {i + 1}", ["BUY", "SELL"], key=f"trans_{i}")
        product_type = cols[5].radio("Product", ["I", "D"], key=f"prod_{i}")
        amo = cols[6].checkbox("AMO", key=f"amo_{i}")
        stop_loss = cols[7].number_input(f"Stop Loss {i + 1}", min_value=0.0, value=sl, key=f"sl_{i}")
        target = cols[8].number_input(f"Target {i + 1}", min_value=0.0, value=target, key=f"target_{i}")

        price = trigger_price = 0
        if order_type == "LIMIT":
            price = cols[7].number_input(f"Price {i + 1}", min_value=0.0, value=float(ltp) if ltp != "N/A" else 0, key=f"price_{i}")
        elif order_type == "SL":
            price = cols[7].number_input(f"Price {i + 1}", min_value=0.0, value=float(ltp) if ltp != "N/A" else 0, key=f"price_sl_{i}")
            trigger_price = cols[8].number_input(f"Trigger {i + 1}", min_value=0.0, value=float(ltp) if ltp != "N/A" else 0, key=f"trigger_{i}")
        elif order_type == "SL-M":
            trigger_price = cols[7].number_input(f"Trigger {i + 1}", min_value=0.0, value=float(ltp) if ltp != "N/A" else 0, key=f"trigger_slm_{i}")

        schedule_short = cols[8].checkbox("Schedule at Open", key=f"schedule_{i}")
        schedule_datetime = datetime.now() if schedule_short else None
        if schedule_short:
            cols_sched = st.columns(4)
            schedule_time = cols_sched[0].time_input(f"Time {i + 1}", value=datetime(2025, 3, 13, 9, 15).time(), key=f"time_{i}")
            schedule_date = cols_sched[1].date_input(f"Date {i + 1}", value=datetime(2025, 3, 13), key=f"date_{i}")
            schedule_datetime = datetime.combine(schedule_date, schedule_time)

        order_data = {
            "order_id": f"order_{uuid.uuid4()}",
            "instrument_token": instrument_token,
            "quantity": quantity_input,
            "order_type": order_type,
            "transaction_type": "SELL" if schedule_short else transaction_type,
            "product_type": product_type,
            "is_amo": amo,
            "price": price,
            "trigger_price": trigger_price,
            "validity": "DAY",
            "stop_loss": stop_loss,
            "target": target,
            "schedule_datetime": schedule_datetime,
            "status": "PENDING"
        }
        orders.append(order_data)

    if st.button("Place Order(s)"):
        with st.spinner("Placing orders..."):
            for order in orders:
                order_params = {k: v for k, v in order.items() if k not in ["order_id", "status", "schedule_datetime"]}
                if order["schedule_datetime"] and order["schedule_datetime"] > datetime.now():
                    st.session_state.scheduled_orders.put(order)
                    def execute_scheduled():
                        time.sleep((order["schedule_datetime"] - datetime.now()).total_seconds())
                        order_id = place_order(**order_params)
                        if order_id:
                            order["status"] = "PLACED"
                            load_sql_data(pd.DataFrame([order]), "Orders", load_type="append")
                            st.success(f"Scheduled order executed: {order_id}")
                    threading.Thread(target=execute_scheduled, daemon=True).start()
                else:
                    order_id = place_order(**order_params)
                    if order_id:
                        order["status"] = "PLACED"
                        load_sql_data(pd.DataFrame([order]), "Orders", load_type="append")
                        st.success(f"Order placed: {order_id}")

    # Scheduled Orders Section
    st.subheader("Scheduled Orders")
    scheduled_orders = list(st.session_state.scheduled_orders.queue)
    if scheduled_orders:
        df_scheduled = pd.DataFrame(scheduled_orders)
        st.dataframe(df_scheduled[["order_id", "instrument_token", "quantity", "transaction_type", "schedule_datetime"]])
        selected_order = st.selectbox("Select Scheduled Order", options=df_scheduled["order_id"])
        if selected_order:
            order = next(o for o in scheduled_orders if o["order_id"] == selected_order)
            cols_mod = st.columns(2)
            with cols_mod[0]:
                if st.button("Cancel", key=f"cancel_{selected_order}"):
                    st.session_state.scheduled_orders.queue = Queue()
                    for o in [o for o in scheduled_orders if o["order_id"] != selected_order]:
                        st.session_state.scheduled_orders.put(o)
                    st.success("Order cancelled")
            with cols_mod[1]:
                new_time = st.time_input("New Time", value=order["schedule_datetime"].time(), key=f"new_time_{selected_order}")
                new_date = st.date_input("New Date", value=order["schedule_datetime"].date(), key=f"new_date_{selected_order}")
                if st.button("Modify", key=f"modify_{selected_order}"):
                    order["schedule_datetime"] = datetime.combine(new_date, new_time)
                    st.session_state.scheduled_orders.queue = Queue()
                    for o in scheduled_orders:
                        st.session_state.scheduled_orders.put(o)
                    st.success("Order modified")
    else:
        st.info("No scheduled orders")

    # Auto Orders Section
    st.subheader("Auto Orders")
    auto_symbol = st.selectbox("Auto Order Symbol", options=st.session_state.instruments_data.keys(), key="auto_sym")
    auto_instrument = st.session_state.instruments_data[auto_symbol]
    auto_risk = st.number_input("Risk %", min_value=0.1, value=1.0, key="auto_risk")
    auto_sl_type = st.selectbox("Stop Loss Type", ["Fixed", "Percent", "ATR"], key="auto_sl")
    auto_sl_value = st.number_input("Stop Loss Value", min_value=1.0, value=100.0, key="auto_sl_val") if auto_sl_type == "Fixed" else st.number_input("Stop Loss %", min_value=0.1, value=1.0, key="auto_sl_pct") if auto_sl_type == "Percent" else st.number_input("SL Multiplier", min_value=0.5, value=2.0, key="auto_sl_atr")
    auto_target_value = st.number_input("Target Value", min_value=1.0, value=250.0, key="auto_target_val") if auto_sl_type == "Fixed" else st.number_input("Target %", min_value=0.1, value=2.5, key="auto_target_pct") if auto_sl_type == "Percent" else st.number_input("Target Multiplier", min_value=0.5, value=5.0, key="auto_target_atr")
    if st.button("Add Auto Order"):
        live_data = get_live_data(auto_instrument) if st.session_state.websocket_initialized else {"ltp": 0}
        entry_price = live_data.get("ltp", 0)
        capital = st.session_state.funds_data.get("data", {}).get("equity", {}).get("available_margin", 50000)
        risk_amount = capital * (auto_risk / 100)
        if auto_sl_type == "Fixed":
            sl = auto_sl_value
            target = auto_target_value
            quantity = int(risk_amount / sl)
        elif auto_sl_type == "Percent":
            sl = entry_price * (auto_sl_value / 100)
            target = entry_price * (auto_target_value / 100)
            quantity = int(risk_amount / sl)
        else:
            hist_data = get_historical_data(auto_instrument)
            atr = hist_data["close"].pct_change().rolling(14).std().iloc[-1] if hist_data is not None else 0
            sl = atr * auto_sl_value
            target = atr * auto_target_value
            quantity = int(risk_amount / sl) if atr > 0 else 1
        quantity = max(1, quantity)
        auto_order = {
            "order_id": f"auto_{uuid.uuid4()}",
            "instrument_token": auto_instrument,
            "quantity": quantity,
            "transaction_type": "SELL",
            "order_type": "MARKET",
            "product_type": "I",
            "is_amo": False,
            "price": 0,
            "trigger_price": 0,
            "validity": "DAY",
            "stop_loss": entry_price - sl if entry_price > 0 else 0,
            "target": entry_price + target if entry_price > 0 else 0,
            "status": "PENDING"
        }
        st.session_state.auto_orders.append(auto_order)
        load_sql_data(pd.DataFrame([auto_order]), "AutoOrders", load_type="append")
        st.success(f"Auto order added for {auto_symbol}")

    if st.session_state.auto_orders:
        st.subheader("Auto Orders List")
        df_auto = pd.DataFrame(st.session_state.auto_orders)
        st.dataframe(df_auto[["order_id", "instrument_token", "quantity", "stop_loss", "target"]])
        selected_auto = st.selectbox("Select Auto Order", options=df_auto["order_id"])
        if selected_auto:
            order = next(o for o in st.session_state.auto_orders if o["order_id"] == selected_auto)
            cols_auto = st.columns(2)
            with cols_auto[0]:
                if st.button("Cancel Auto", key=f"cancel_auto_{selected_auto}"):
                    st.session_state.auto_orders = [o for o in st.session_state.auto_orders if o["order_id"] != selected_auto]
                    load_sql_data(pd.DataFrame(st.session_state.auto_orders), "AutoOrders", load_type="replace")
                    st.success("Auto order cancelled")
            with cols_auto[1]:
                new_qty = st.number_input("New Quantity", min_value=1, value=order["quantity"], key=f"new_qty_{selected_auto}")
                new_sl = st.number_input("New Stop Loss", min_value=0.0, value=order["stop_loss"], key=f"new_sl_{selected_auto}")
                new_target = st.number_input("New Target", min_value=0.0, value=order["target"], key=f"new_target_{selected_auto}")
                if st.button("Modify Auto", key=f"modify_auto_{selected_auto}"):
                    order.update({"quantity": new_qty, "stop_loss": new_sl, "target": new_target})
                    load_sql_data(pd.DataFrame(st.session_state.auto_orders), "AutoOrders", load_type="replace")
                    st.success("Auto order modified")

    # Auto Order Scheduler
    def execute_auto_orders():
        if st.session_state.auto_orders:
            for order in st.session_state.auto_orders:
                if order["status"] == "PENDING":
                    order_id = place_order(**{k: v for k, v in order.items() if k not in ["order_id", "status", "schedule_datetime"]})
                    if order_id:
                        order["status"] = "PLACED"
                        load_sql_data(pd.DataFrame(st.session_state.auto_orders), "AutoOrders", load_type="replace")
                        st.success(f"Auto order executed: {order_id}")

    if "scheduler_running" not in st.session_state:
        st.session_state.scheduler_running = True
        schedule.every().day.at("09:15").do(execute_auto_orders)
        def run_scheduler():
            while True:
                schedule.run_pending()
                time.sleep(60)
        threading.Thread(target=run_scheduler, daemon=True).start()

# Add other pages (Order Book, Positions, etc.) as needed - simplified for brevity
elif page in ["Order Book", "Positions", "Portfolio", "Analytics", "Algo Trading", "Strategy Backtest"]:
    st.write(f"{page} content to be implemented")

# Footer
st.markdown(f'<div class="metric-box">Last Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>', unsafe_allow_html=True)
