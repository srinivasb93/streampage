import streamlit as st
import upstox_client
from upstox_client.rest import ApiException
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv
import os
import requests
from lightweight_charts.widgets import StreamlitChart
from kiteconnect import KiteConnect
import pyotp
import pandas as pd
import logging
import json
import threading
import time
from datetime import datetime, timedelta
import urllib.parse
from common_utils import *

# Load environment variables
load_dotenv()
# Upstox credentials
ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
SANDBOX_ACCESS_TOKEN = os.getenv("SANDBOX_ACCESS_TOKEN")
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

st.set_page_config(page_title="Upstox & Zerodha Trading Dashboard", layout="wide")
# Custom CSS for UI Enhancement
st.markdown("""
    <style>
    .main { background-color: #f5f5f5; padding: 20px; border-radius: 10px; }
    .stButton>button { background-color: #4CAF50; color: white; border-radius: 5px; }
    .stTextInput>input { border-radius: 5px; }
    .sidebar .sidebar-content { background-color: #e0e0e0; padding: 10px; border-radius: 10px; }
    .metric-box { background-color: #336699; padding: 10px; border-radius: 5px; box-shadow: 2px 2px 5px rgba(0,0,0,0.1); }
    </style>
""", unsafe_allow_html=True)


# Upstox API Initialization
def init_upstox_api():
    config = upstox_client.Configuration()
    config.access_token = st.session_state.get("upstox_access_token", ACCESS_TOKEN)
    api_client = upstox_client.ApiClient(config)
    return {
        "order": upstox_client.OrderApi(api_client),
        "portfolio": upstox_client.PortfolioApi(api_client),
        "history": upstox_client.HistoryApi(api_client),
        "charges": upstox_client.ChargeApi(api_client),
        "market_data": upstox_client.MarketQuoteApi(api_client),
        "user": upstox_client.UserApi(api_client)
    }


# Zerodha API Initialization
def init_zerodha_api():
    kite = KiteConnect(api_key=ZERODHA_API_KEY)
    access_token = st.session_state.get("zerodha_access_token")
    if access_token:
        kite.set_access_token(access_token)
    return kite


# Zerodha Authentication
def authenticate_zerodha():
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
            return kite, access_token
    except Exception as e:
        st.error(f"Zerodha authentication failed: {str(e)}")
        return None, None


# Fetch Upstox Access Token
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
        st.session_state["upstox_access_token"] = token
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


# Place Order (Upstox or Zerodha)
def place_order(broker, api, instrument_token, transaction_type, quantity, price=0, order_type="MARKET",
                trigger_price=0, is_amo=False, product_type="D", validity='DAY'):
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
            order_id = response.data.order_id
            notify(f"Upstox Order Placed: {transaction_type}", f"Order ID: {order_id}")
            return response
        else:  # Zerodha
            zerodha_product = "MIS" if product_type == "I" else "CNC"
            zerodha_exchange = instrument_token.split("|")[0].replace("_EQ", "")
            zerodha_symbol = get_symbol_for_instrument(instrument_token=instrument_token)
            order_id = api.place_order(
                variety=api.VARIETY_REGULAR if not is_amo else api.VARIETY_AMO,
                tradingsymbol=zerodha_symbol,
                exchange=zerodha_exchange,
                transaction_type=transaction_type,
                quantity=quantity,
                order_type=order_type,
                product=zerodha_product,
                price=price if order_type in ["LIMIT", "SL"] else None,
                trigger_price=trigger_price if order_type in ["SL", "SL-M"] else None,
                validity=validity
            )
            notify(f"Zerodha Order Placed: {transaction_type}", f"Order ID: {order_id}")
            return order_id
    except Exception as e:
        st.error(f"Error placing {broker} order: {e}")
        return None


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
                "Day Change": holding_dict.get("day_change_percentage", 0),
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
                "Day Change": holding.get("day_change_percentage", 0),
            })
    except Exception as e:
        st.error(f"Failed to fetch Zerodha portfolio: {e}")

    return pd.DataFrame(holdings)


# Force logging to console
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True)
logger = logging.getLogger(__name__)

# Streamlit App
# Sidebar
with st.sidebar:
    st.subheader("Navigation")
    page = st.radio("Go to",
                    ["Order Management", "Order Book", "Positions", "Portfolio", "Analytics", "Algo Trading",
                     "Strategy Backtest", "Get Token"])
    st.subheader("Settings")
    st.session_state["email_notifications"] = st.checkbox("Enable Email Notifications", value=False)
    st.session_state["broker"] = st.selectbox("Select Broker", ["Upstox", "Zerodha"], key="broker_select")
    if st.button("Refresh Dashboard", key="refresh"):
        st.rerun()

# Initialize APIs
upstox_apis = init_upstox_api()
zerodha_api = init_zerodha_api()

if 'instruments_data' not in st.session_state:
    st.session_state['instruments_data'] = fetch_instruments()
instruments = st.session_state['instruments_data'] if 'instruments_data' in st.session_state else {}

# Get Token Page
if page == "Get Token":
    st.subheader("Fetch Access Token")
    broker = st.session_state.get("broker", "Upstox")
    st.write(f"Selected Broker: {broker}")

    if broker == "Upstox":
        st.subheader("Upstox Authentication")
        api_key = st.text_input("API Key", value=UPSTOX_API_KEY)
        api_secret = st.text_input("API Secret", value=UPSTOX_API_SECRET)
        redirect_uri = st.text_input("Redirect URI", "https://api.upstox.com/v2/login")
        auth_url = f"https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={api_key}&redirect_uri={redirect_uri}"
        st.markdown(f"[Click here to authorize]({auth_url})")
        auth_code = st.text_input("Authorization Code (from redirect URL)")
        if st.button("Fetch Upstox Access Token") and auth_code:
            token = fetch_access_token(api_key, api_secret, redirect_uri, auth_code)
            if token:
                st.success(f"Successfully fetched Upstox Access Token: {token}")
                st.session_state["upstox_access_token"] = token
    else:
        st.subheader("Zerodha Authentication")
        if st.button("Authenticate Zerodha"):
            kite, access_token = authenticate_zerodha()
            if kite and access_token:
                st.session_state["zerodha_api"] = kite
                st.session_state["zerodha_access_token"] = access_token
                st.success(f"Successfully authenticated Zerodha. Access Token: {access_token}")

# Combined Order & Risk Management Page for Order Placement
elif page == "Order Management":
    if 'funds_data' not in st.session_state:
        st.session_state['funds_data'] = get_user_profile_and_funds(upstox_apis['user'])
    st.subheader("Order Management")
    broker = st.session_state.get("broker", "Upstox")
    st.write(f"Selected Broker: {broker}")

    col1, col2 = st.columns(2)

    with col1:
        funds_cols = st.columns(2)
        with funds_cols[0]:
            try:
                if broker == "Upstox":
                    st.markdown(
                        f'<div class="metric-box">Funds Available:'
                        f' ₹{st.session_state["funds_data"]["data"]["equity"]["available_margin"]:.2f}</div>',
                        unsafe_allow_html=True)
                else:
                    funds = zerodha_api.margins().get("equity", {}).get("available", {}).get("cash", 0)
                    st.markdown(
                        f'<div class="metric-box">Funds Available: ₹{funds:.2f}</div>',
                        unsafe_allow_html=True)
            except:
                st.markdown(
                    f'<div class="metric-box">Unable to fetch Funds data</div>',
                    unsafe_allow_html=True)

        single_multi_order = st.radio("Order Type", ["Single Order", "Multiple Orders"], horizontal=True,
                                      label_visibility='collapsed')

        if single_multi_order == 'Single Order':
            order_cols = st.columns(3)
            stock_symbol = order_cols[0].selectbox("Select Symbol", options=instruments.keys())
            instrument_token = instruments.get(stock_symbol)

            # Store selected symbol in session state for polling
            st.session_state["selected_symbol"] = stock_symbol
            # Live data polling outside forms for real-time updates
            ltp_placeholder = st.empty()
            depth_placeholder = st.empty()
            if "last_update" not in st.session_state:
                st.session_state["last_update"] = time.time()

            current_time = time.time()
            if current_time - st.session_state["last_update"] >= 1:
                selected_symbol = st.session_state.get("selected_symbol", None)
                if selected_symbol:
                    instrument_token = instruments.get(selected_symbol)
                    live_data = get_market_quote(upstox_apis['market_data'], instrument_token)
                    with ltp_placeholder.container():
                        st.metric("Last Traded Price", f"₹{live_data.get('ltp', 0):.2f}")
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
            live_data = get_market_quote(upstox_apis['market_data'], instrument_token)

            ltp = live_data.get("ltp", 0)
            if order_type == "LIMIT":
                price = order_type_cols[0].number_input("Limit Price", min_value=0.0,
                                                        value=float(ltp) if ltp else 100.0)
                trigger_price = 0
            elif order_type == "SL":
                price = order_type_cols[0].number_input("Limit Price", min_value=0.0,
                                                        value=float(ltp) if ltp else 100.0)
                trigger_price = order_type_cols[1].number_input("Trigger Price", min_value=0.0, value=95.0)
            elif order_type == "SL-M":
                price = 0
                trigger_price = order_type_cols[0].number_input("Stoploss Trigger", min_value=0.0, value=95.0)
            else:
                price, trigger_price = 0, 0

            other_orders = st.selectbox('Other Orders', ["Auto-sell if Open > Previous Close", "Schedule Order"],
                                        index=None)

            if other_orders == "Schedule Order":
                schedule_cols = st.columns(2)
                schedule_time = schedule_cols[0].time_input("Schedule Time", value="now", step=60)
                schedule_date = schedule_cols[1].date_input("Schedule Date", value=datetime.now(),
                                                            min_value=datetime.now())

            if st.button("Place Order"):
                with st.spinner("Processing order..."):
                    if other_orders == "Auto-sell if Open > Previous Close":
                        hist_data = get_historical_data(upstox_apis["history"], instrument_token)
                        if hist_data is not None:
                            prev_close = hist_data["close"].iloc[-2]
                            current_open = hist_data["close"].iloc[-1]
                            if current_open > prev_close:
                                api = upstox_apis["order"] if broker == "Upstox" else zerodha_api
                                result = place_order(broker, api, instrument_token, "SELL", quantity)
                                if result:
                                    order_id = result.data.order_id if broker == "Upstox" else result
                                    st.success(f"Auto-sell order placed: Order ID {order_id}")
                            else:
                                st.info("Condition not met: Open price not greater than previous close")
                    elif other_orders == "Schedule Order":
                        schedule_datetime = datetime.combine(schedule_date, schedule_time)
                        if schedule_datetime > datetime.now():
                            def execute_scheduled_order(broker, api, symbol_token, trans_type, qty, price_val,
                                                        order_type_val,
                                                        trigger_val, amo, prod_type):
                                time.sleep((schedule_datetime - datetime.now()).total_seconds())
                                print(f"Executing scheduled order for {symbol_token}")
                                result = place_order(broker, api, symbol_token, trans_type, qty, price_val,
                                                     order_type_val, trigger_val, amo, prod_type)
                                if result:
                                    order_id = result.data.order_id if broker == "Upstox" else result
                                    st.success(f"Scheduled order executed: Order ID {order_id}")


                            api = upstox_apis["order"] if broker == "Upstox" else zerodha_api
                            thread = threading.Thread(
                                target=execute_scheduled_order,
                                args=(broker, api, instrument_token, transaction_type, quantity, price, order_type,
                                      trigger_price, amo_order, product_type)
                            )
                            thread.start()
                            st.write(f"Order scheduled for {schedule_datetime}")
                        else:
                            st.error("Schedule time must be in the future.")
                    else:
                        api = upstox_apis["order"] if broker == "Upstox" else zerodha_api
                        result = place_order(broker, api, instrument_token, transaction_type, quantity, price,
                                             order_type, trigger_price, amo_order, product_type)
                        if result:
                            order_id = result.data.order_id if broker == "Upstox" else result
                            st.success(f"Order placed: Order ID {order_id}")

        if single_multi_order == 'Multiple Orders':
            num_orders = col2.number_input("Number of Orders", min_value=1, max_value=10, value=1, step=1)
            orders = []

            for i in range(num_orders):
                st.write(f"Order {i + 1}")
                with st.expander(f"Details for Order {i + 1}", expanded=True):
                    multi_order_cols = st.columns(8)
                    stock_symbol = multi_order_cols[0].selectbox("Select Symbol", options=instruments.keys(),
                                                                 key=f"multi_order_{i}")
                    instrument_token = instruments.get(stock_symbol)
                    quantity = multi_order_cols[1].number_input(
                        f"Quantity {i + 1}", min_value=1, value=1, key=f"multi_qty_{i}")
                    order_type = multi_order_cols[2].selectbox(
                        f"Order Type {i + 1}", ["MARKET", "LIMIT", "SL", "SL-M"], key=f"multi_order_type_{i}")
                    transaction_type = multi_order_cols[3].radio(
                        f"Transaction Type {i + 1}", ["BUY", "SELL"], key=f"multi_trans_{i}", horizontal=True)
                    product_type = multi_order_cols[4].radio(
                        "Product Type", ['I', 'D'], horizontal=True, key=f"multi_prod_type_{i}")
                    amo_order = multi_order_cols[5].checkbox("AMO Order", key=f"multi_amo_{i}")

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

                    if broker == "Upstox":
                        orders.append(upstox_client.MultiOrderRequest(
                            quantity=quantity,
                            product=product_type,
                            validity="DAY",
                            price=price,
                            tag=f"MultiOrder_{i + 1}",
                            instrument_token=instrument_token,
                            order_type=order_type,
                            transaction_type=transaction_type,
                            disclosed_quantity=0,
                            trigger_price=trigger_price,
                            is_amo=amo_order,
                            correlation_id=f"order_{i}",
                            slice=True
                        ))

            if st.button("Place Multiple Orders"):
                with st.spinner("Placing multiple orders..."):
                    try:
                        if broker == "Upstox":
                            response = upstox_apis["order"].place_multi_order(orders)
                            st.success("Multiple orders placed successfully!")
                            st.json(response.data)
                        else:
                            for order in orders:
                                result = place_order(
                                    "Zerodha",
                                    zerodha_api,
                                    order.instrument_token,
                                    order.transaction_type,
                                    order.quantity,
                                    order.price,
                                    order.order_type,
                                    order.trigger_price,
                                    order.is_amo,
                                    order.product
                                )
                                if result:
                                    st.success(f"Zerodha order placed: Order ID {result}")
                                else:
                                    st.error("Failed to place Zerodha order.")
                    except Exception as e:
                        st.error(f"Error placing multiple orders: {e}")

        ltp_placeholder = st.empty()
        depth_placeholder = st.empty()

        if "last_update" not in st.session_state:
            st.session_state["last_update"] = time.time()

        current_time = time.time()
        if current_time - st.session_state["last_update"] >= 1:
            live_data = get_market_quote(upstox_apis['market_data'], instrument_token)
            with ltp_placeholder.container():
                st.metric("Last Traded Price", f"₹{live_data.get('ltp', 0):.2f}")
            if "depth" in live_data and live_data["depth"]:
                with depth_placeholder.container():
                    st.dataframe(pd.DataFrame(live_data["depth"]))
            st.session_state["last_update"] = current_time

        with col2:
            st.subheader("Risk Calculator")
            risk_cols = st.columns(3)
            account_size = risk_cols[0].number_input("Account Size (₹)", min_value=1000.0, value=100000.0)
            stock_symbol = risk_cols[1].selectbox("Select Symbol", options=instruments.keys(), key=f"risk_symbol")
            instrument_token = instruments.get(stock_symbol)
            entry_price = risk_cols[2].number_input("Entry Price (₹)", min_value=0.0, value=100.0)
            trade_size = int((account_size / 4) / entry_price)
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
                target_price = risk_cols[1].number_input("Target Price (₹)", min_value=entry_price,
                                                         value=entry_price + 5.0)
                profit_percent = ((target_price - entry_price) / entry_price) * 100

            product_type = st.selectbox("Product Type", options=['D', 'I'])

            price_per_share_risk = risk_amount / trade_size
            stop_loss = entry_price - price_per_share_risk if transaction_type == "BUY" else entry_price + price_per_share_risk

            if st.button("Calculate Risk & Profit"):
                brokerage = calculate_brokerage(upstox_apis["charges"], instrument_token, trade_size,
                                                entry_price, "BUY", product_type)
                profit_amount = (target_price - entry_price) * trade_size - brokerage

                st.markdown(f'<div class="metric-box">Risk Amount: ₹{risk_amount:.2f} ({risk_percent:.2f}%)</div>',
                            unsafe_allow_html=True)
                st.markdown(f'<div class="metric-box">Stop Loss Price: ₹{stop_loss:.2f}</div>', unsafe_allow_html=True)
                st.markdown(f'<div class="metric-box">Target Price: ₹{target_price:.2f}</div>', unsafe_allow_html=True)
                st.markdown(f'<div class="metric-box">Position Size: {trade_size} shares</div>', unsafe_allow_html=True)
                st.markdown(
                    f'<div class="metric-box">Expected Profit: ₹{profit_amount:.2f} ({profit_percent:.2f}%)</div>',
                    unsafe_allow_html=True)
                st.markdown(f'<div class="metric-box">Brokerage: ₹{brokerage:.2f}</div>', unsafe_allow_html=True)

# Order History Page
elif page == "Order Book":
    st.subheader("Order History")
    orders_df = get_order_book(upstox_apis["order"], zerodha_api)

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
                default=[]
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
                                response = upstox_apis["order"].cancel_order(selected_order_id, api_version="2.0").data
                            else:
                                response = zerodha_api.cancel_order(variety=zerodha_api.VARIETY_AMO, order_id=selected_order_id)
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
                            open_orders = [o for o in zerodha_api.orders() if
                                           o["status"] not in ["COMPLETE", "REJECTED", "CANCELLED", 'CANCELLED AMO']]
                            for order in open_orders:
                                zerodha_api.cancel_order(
                                    variety=zerodha_api.VARIETY_AMO if 'AMO' in order['status']
                                    else zerodha_api.VARIETY_REGULAR,
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
                                    response = upstox_apis['order'].modify_order(modify_data, api_version="2.0").data
                                else:
                                    response = zerodha_api.modify_order(
                                        variety=zerodha_api.VARIETY_AMO if 'AMO' in order_data["Status"]
                                        else zerodha_api.VARIETY_REGULAR,
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

# Positions Page
elif page == "Positions":
    st.subheader("Current Positions")
    positions_df = get_positions(upstox_apis["portfolio"], zerodha_api)
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
                    api = upstox_apis['order'] if row['Broker'] == "Upstox" else zerodha_api
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

# Portfolio Page
elif page == "Portfolio":
    st.subheader("Portfolio Overview")
    portfolio_df = get_portfolio(upstox_apis["portfolio"], zerodha_api)
    if not portfolio_df.empty:
        total_value = (portfolio_df["Last Price"] * portfolio_df["Quantity"]).sum()
        total_buy_value = (portfolio_df["Avg. Price"] * portfolio_df["Quantity"]).sum()
        total_pnl = (portfolio_df["Last Price"] - portfolio_df["Avg. Price"]) * portfolio_df["Quantity"]
        portfolio_df['Buy Value'] = portfolio_df['Avg. Price'] * portfolio_df['Quantity']
        portfolio_df['Market Value'] = portfolio_df['Last Price'] * portfolio_df['Quantity']
        st.dataframe(
            portfolio_df[["Broker", "Symbol", "Exchange", "Quantity", "Last Price", "Avg. Price", "Buy Value",
                          "Market Value", "P&L", 'Day Change']]
            .style
            .highlight_max(subset=["Buy Value", "Market Value", "P&L", "Day Change"],
                            color='#3ee27a')
            .highlight_min(subset=["Buy Value", "Market Value", "P&L", "Day Change"],
                            color="#ea3c34")
            .format(precision=2, thousands=",")
            .background_gradient(cmap='RdYlGn', subset=['P&L', 'Day Change']),
            hide_index=True,
            use_container_width=True,)
        columns = st.columns(3)
        columns[0].metric('Total Buy Value', f'₹ {total_buy_value:.2f}')
        columns[1].metric('Total Portfolio Value', f'₹ {total_value:.2f}')
        columns[2].metric('Total P&L', f'₹ {total_pnl.sum():.2f}', f'{(total_pnl.sum() / total_buy_value * 100):.2f}%')
    else:
        st.info("No holdings found")

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

    period_cols = analytics_cols[2].columns(3)
    show_columns = st.columns(5)
    show_sr = show_columns[0].checkbox("Show Support & Resistance")
    show_trend = show_columns[1].checkbox("Show Trend Lines")
    show_ema = show_columns[2].checkbox("Show EMA")
    show_lr = show_columns[3].checkbox("Show Linear Regression")
    show_rsi = show_columns[4].checkbox("Show RSI")

    data = get_historical_data(upstox_apis["history"], instrument_token, timeframe)
    if data is not None:
        chart = StreamlitChart(height=600, toolbox=True)
        chart_data = data.rename(
            columns={"timestamp": "time", "open": "open", "high": "high", "low": "low", "close": "close"})
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
            chart.trend_line(trend_start['time'], trend_start['value'], trend_end['time'], trend_end['value'],
                             line_color="blue")

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
            rsi_data = pd.DataFrame({
                "time": data["timestamp"],
                "rsi": rsi
            }).dropna()
            rsi_line = chart.create_line(name="RSI", color="blue")
            rsi_line.set(rsi_data)
            latest_rsi = rsi.iloc[-1]
            st.write(f"Latest RSI ({rsi_period}): {latest_rsi:.2f}")

        chart.load()

# Algo Trading Page
elif page == "Algo Trading":
    st.subheader("Algorithmic Trading")
    broker = st.session_state.get("broker", "Upstox")
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
        st.markdown("""
        **MACD Crossover Strategy**
        The MACD strategy generates signals when the MACD line crosses over the signal line.
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
        Parameters by:
        - RSI Period: Lookback period for calculating RSI
        - Overbought Level: RSI value above which the asset is considered overbought
        - Oversold Level: RSI value below which the asset is considered oversold
        """)

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Check Current Signal"):
            with st.spinner("Analyzing market data..."):
                hist_data = get_historical_data(upstox_apis["history"], instrument_token)
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
                    api = upstox_apis["order"] if broker == "Upstox" else zerodha_api
                    result = auto_trade(api, strategy, instrument_token, quantity, stop_loss, take_profit)
                    st.success(result)
        else:
            if st.button("Start Automated Trading"):
                run_hours = [(start_hour, end_hour)]
                api = upstox_apis["order"] if broker == "Upstox" else zerodha_api
                result = schedule_strategy_execution(api, strategy, instrument_token, quantity, interval, run_hours)
                st.success(
                    "Automated trading started. The system will check for signals every {} minutes during market hours.".format(
                        interval))
                st.warning(
                    "Warning: Automated trading will continue until the application is closed or you navigate away from this page.")

# Strategy Backtest Page
elif page == "Strategy Backtest":
    st.subheader("Strategy Backtesting")
    stock_symbol = st.selectbox("Select Symbol", options=instruments.keys(), key='symbol_backtest')
    instrument_token = instruments.get(stock_symbol)
    timeframe = st.selectbox("Timeframe", ["day", "week", "1minute", "5minute", "30minute"], index=0)
    strategy = st.selectbox("Select Strategy to Backtest", [
        "MACD Crossover",
        "Bollinger Bands",
        "RSI Strategy",
    ])

    if strategy == "MACD Crossover":
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
            hist_data = get_historical_data(upstox_apis["history"], instrument_token, timeframe, sort_data=True)
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

                import altair as alt

                price_chart = alt.Chart(chart_data).mark_line().encode(
                    x='Date:T',
                    y=alt.Y('Close Price:Q', scale=alt.Scale(zero=False))
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
                pnl_chart = alt.Chart(chart_data).mark_line(color='purple').encode(
                    x='Date:T',
                    y='Cumulative P&L:Q'
                )
                st.altair_chart(price_chart + buy_points + sell_points, use_container_width=True)
                st.altair_chart(pnl_chart, use_container_width=True)

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