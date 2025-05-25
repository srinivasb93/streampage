import streamlit as st
import pandas as pd
from kiteconnect import KiteConnect
import upstox_client
from upstox_client.rest import ApiException
import requests
import pyotp
import logging
import json
from datetime import datetime, timedelta
import uuid
import urllib.parse
import os
from dotenv import load_dotenv
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)

# Streamlit page configuration
st.set_page_config(page_title="Zerodha & Upstox Algo Trading", layout="wide")

# Initialize session state
if 'broker' not in st.session_state:
    st.session_state.broker = None
if 'kite' not in st.session_state:
    st.session_state.kite = None
if 'upstox_client' not in st.session_state:
    st.session_state.upstox_client = None
if 'access_token' not in st.session_state:
    st.session_state.access_token = None

# Constants (Replace with your credentials)
# Zerodha
ZERODHA_API_KEY = os.getenv("ZERODHA_API_KEY")
ZERODHA_API_SECRET = os.getenv("ZERODHA_API_SECRET")
ZERODHA_TOTP_TOKEN = os.getenv("ZERODHA_TOTP_TOKEN")
ZERODHA_USERNAME = os.getenv("ZERODHA_USERNAME")
ZERODHA_PASSWORD = os.getenv("ZERODHA_PASSWORD")
# Upstox
UPSTOX_API_KEY = os.getenv("UPSTOX_API_KEY")
UPSTOX_API_SECRET = os.getenv("UPSTOX_API_SECRET")
UPSTOX_REDIRECT_URI = "https://api.upstox.com/v2/login"  # Replace with your registered redirect URI
UPSTOX_AUTH_URL = auth_url = f"https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={UPSTOX_API_KEY}&redirect_uri={UPSTOX_REDIRECT_URI}"
UPSTOX_TOKEN_URL = "https://api.upstox.com/v2/login/authorization/token"

# API URLs
ZERODHA_BASE_URL = "https://kite.zerodha.com"
ZERODHA_LOGIN_URL = f"{ZERODHA_BASE_URL}/api/login"
ZERODHA_TWOFA_URL = f"{ZERODHA_BASE_URL}/api/twofa"



def authenticate_zerodha():
    """Authenticate with Zerodha and return KiteConnect object and access token"""
    try:
        session = requests.Session()
        response = session.post(ZERODHA_LOGIN_URL, data={'user_id': ZERODHA_USERNAME, 'password': ZERODHA_PASSWORD})
        response.raise_for_status()  # Ensure login request succeeded
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
        response.raise_for_status()  # Ensure TOTP request succeeded
        response_data = json.loads(response.text)
        if response_data.get('status') != 'success':
            raise ValueError(
                f"TOTP authentication failed jusqu'à ce point: {response_data.get('message', 'Unknown error')}")

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
            return kite, access_token
    except Exception as e:
        st.error(f"Zerodha authentication failed: {str(e)}")
        return None, None


def get_upstox_auth_url():
    """Generate Upstox authorization URL"""
    params = {
        "client_id": UPSTOX_API_KEY,
        "redirect_uri": UPSTOX_REDIRECT_URI,
        "response_type": "code",
        "state": str(uuid.uuid4())
    }
    # return f"{UPSTOX_AUTH_URL}?{urllib.parse.urlencode(params)}"
    return UPSTOX_AUTH_URL


def authenticate_upstox(auth_code):
    """Authenticate with Upstox using auth code and return Upstox client"""
    try:
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "code": auth_code,
            "client_id": UPSTOX_API_KEY,
            "client_secret": UPSTOX_API_SECRET,
            "redirect_uri": UPSTOX_REDIRECT_URI,
            "grant_type": "authorization_code"
        }
        response = requests.post(UPSTOX_TOKEN_URL, headers=headers, data=data)
        response.raise_for_status()
        access_token = response.json().get("access_token")

        configuration = upstox_client.Configuration()
        configuration.access_token = access_token
        client = upstox_client.ApiClient(configuration)
        return client, access_token
    except Exception as e:
        st.error(f"Upstox authentication failed: {str(e)}")
        return None, None


def place_zerodha_order(kite, tradingsymbol, exchange, transaction_type, quantity, order_type, product, price=None,
                        trigger_price=None):
    """Place an order using Zerodha KiteConnect"""
    try:
        order_id = kite.place_order(
            tradingsymbol=tradingsymbol,
            exchange=exchange,
            transaction_type=transaction_type,
            quantity=quantity,
            variety=kite.VARIETY_REGULAR,
            order_type=order_type,
            product=product,
            price=price if order_type in [kite.ORDER_TYPE_LIMIT, kite.ORDER_TYPE_SL] else None,
            trigger_price=trigger_price if order_type in [kite.ORDER_TYPE_SL, kite.ORDER_TYPE_SLM] else None,
            validity=kite.VALIDITY_DAY
        )
        return order_id
    except Exception as e:
        st.error(f"Zerodha order placement failed: {str(e)}")
        return None


def place_upstox_order(client, tradingsymbol, exchange, transaction_type, quantity, order_type, product, price=None,
                       trigger_price=None):
    """Place an order using Upstox API"""
    try:
        api_instance = upstox_client.OrderApi(client)
        body = upstox_client.PlaceOrderRequest(
            quantity=quantity,
            product=product,
            validity="DAY",
            price=price if order_type in ["LIMIT", "SL"] else 0.0,
            instrument_token=f"{exchange}|{tradingsymbol}",
            order_type=order_type,
            transaction_type=transaction_type,
            disclosed_quantity=0,
            trigger_price=trigger_price if order_type in ["SL", "SL-M"] else 0.0,
            is_amo=False
        )
        api_response = api_instance.place_order(body, api_version="2.0")
        return api_response.data.order_id
    except ApiException as e:
        st.error(f"Upstox order placement failed: {str(e)}")
        return None


def get_upstox_historical_data(client, instrument_token, interval, from_date, to_date):
    """Fetch historical OHLC data using Upstox API"""
    try:
        api_instance = upstox_client.HistoryApi(client)
        api_response = api_instance.get_historical_candle_data1(
            instrument_key=instrument_token,
            interval=interval,
            to_date=to_date,
            from_date=from_date,
            api_version="2.0"
        )
        data = api_response.data.candles
        df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except ApiException as e:
        st.error(f"Failed to fetch historical data: {str(e)}")
        return pd.DataFrame()


def get_upstox_live_quotes(client, instrument_tokens):
    """Fetch live market quotes using Upstox REST API"""
    try:
        api_instance = upstox_client.MarketQuoteApi(client)
        api_response = api_instance.get_full_market_quote(",".join(instrument_tokens), api_version="2.0")
        quotes = []
        for token, quote in api_response.data.items():
            quotes.append({
                "instrument_token": token,
                "ltp": quote.last_price,
                "volume": quote.volume,
                "timestamp": quote.timestamp
            })
        return pd.DataFrame(quotes)
    except ApiException as e:
        st.error(f"Failed to fetch live quotes: {str(e)}")
        return pd.DataFrame()


def get_zerodha_portfolio(kite):
    """Get portfolio holdings from Zerodha"""
    try:
        holdings = kite.holdings()
        return pd.DataFrame(holdings)
    except Exception as e:
        st.error(f"Failed to fetch Zerodha portfolio: {str(e)}")
        return pd.DataFrame()


def get_upstox_portfolio(client):
    """Get portfolio holdings from Upstox"""
    try:
        api_instance = upstox_client.PortfolioApi(client)
        api_response = api_instance.get_holdings(api_version="2.0")
        return pd.DataFrame([item.to_dict() for item in api_response.data])
    except ApiException as e:
        st.error(f"Failed to fetch Upstox portfolio: {str(e)}")
        return pd.DataFrame()


def get_zerodha_orders(kite):
    """Get order history from Zerodha"""
    try:
        orders = kite.orders()
        return pd.DataFrame(orders)
    except Exception as e:
        st.error(f"Failed to fetch Zerodha orders: {str(e)}")
        return pd.DataFrame()


def get_upstox_orders(client):
    """Get order history from Upstox"""
    try:
        api_instance = upstox_client.OrderApi(client)
        api_response = api_instance.get_trade_history(api_version="2.0")
        return pd.DataFrame(api_response.data)
    except ApiException as e:
        st.error(f"Failed to fetch Upstox orders: {str(e)}")
        return pd.DataFrame()


def get_zerodha_positions(kite):
    """Get current positions from Zerodha"""
    try:
        positions = kite.positions()
        return pd.DataFrame(positions['net'])
    except Exception as e:
        st.error(f"Failed to fetch Zerodha positions: {str(e)}")
        return pd.DataFrame()


def get_upstox_positions(client):
    """Get current positions from Upstox"""
    try:
        api_instance = upstox_client.PortfolioApi(client)
        api_response = api_instance.get_positions(api_version="2.0")
        return pd.DataFrame(api_response.data)
    except ApiException as e:
        st.error(f"Failed to fetch Upstox positions: {str(e)}")
        return pd.DataFrame()


def main():
    st.title("Zerodha & Upstox Algo Trading Dashboard")

    # Sidebar for broker selection and navigation
    broker = st.sidebar.selectbox("Select Broker", ["Zerodha", "Upstox"])
    st.session_state.broker = broker
    menu = ["Authenticate", "Place Order", "View Orders", "View Positions", "View Portfolio", "Historical Data",
            "Live Quotes"]
    choice = st.sidebar.selectbox("Menu", menu)

    # Authentication
    if choice == "Authenticate":
        st.header(f"Authenticate with {broker}")
        if broker == "Zerodha":
            if st.button("Login to Zerodha"):
                kite, access_token = authenticate_zerodha()
                if kite and access_token:
                    st.session_state.kite = kite
                    st.session_state.access_token = access_token
                    st.success("Zerodha authentication successful!")
        else:
            st.subheader("Upstox Login Flow")
            if st.button("Generate Upstox Login URL"):
                auth_url = get_upstox_auth_url()
                st.write(f"[Click here to login to Upstox]({auth_url})")
                st.info("After logging in, copy the 'code' from the redirect URL and paste it below.")

            auth_code = st.text_input("Enter Upstox Authorization Code")
            if st.button("Authenticate Upstox") and auth_code:
                upstox_client, access_token = authenticate_upstox(auth_code)
                if upstox_client and access_token:
                    st.session_state.upstox_client = upstox_client
                    st.session_state.access_token = access_token
                    st.success("Upstox authentication successful!")

    # Ensure authentication for other sections
    if (broker == "Zerodha" and st.session_state.kite is None) or \
            (broker == "Upstox" and st.session_state.upstox_client is None) and choice != "Authenticate":
        st.warning(f"Please authenticate with {broker} first!")
        return

    kite = st.session_state.kite if broker == "Zerodha" else None
    upstox_client = st.session_state.upstox_client if broker == "Upstox" else None

    # Place Order
    if choice == "Place Order":
        st.header("Place New Order")
        with st.form("order_form"):
            tradingsymbol = st.text_input("Trading Symbol (e.g., INFY)")
            exchange = st.selectbox("Exchange", ["NSE", "BSE"] if broker == "Zerodha" else ["NSE_EQ", "BSE_EQ"])
            transaction_type = st.selectbox("Transaction Type", ["BUY", "SELL"])
            quantity = st.number_input("Quantity", min_value=1, value=1)
            order_type = st.selectbox("Order Type", ["MARKET", "LIMIT", "SL", "SL-M"])
            product = st.selectbox("Product", ["CNC", "MIS"] if broker == "Zerodha" else ["D", "I"])
            price = st.number_input("Price (for Limit/SL Orders)", min_value=0.0, value=0.0) if order_type in ["LIMIT",
                                                                                                               "SL"] else None
            trigger_price = st.number_input("Trigger Price (for SL/SL-M Orders)", min_value=0.0,
                                            value=0.0) if order_type in ["SL", "SL-M"] else None
            submitted = st.form_submit_button("Place Order")

            if submitted:
                if broker == "Zerodha":
                    order_id = place_zerodha_order(kite, tradingsymbol, exchange, transaction_type, quantity,
                                                   order_type, product, price, trigger_price)
                else:
                    order_id = place_upstox_order(upstox_client, tradingsymbol, exchange, transaction_type, quantity,
                                                  order_type, product, price, trigger_price)
                if order_id:
                    st.success(f"Order placed successfully! Order ID: {order_id}")

    # View Orders
    if choice == "View Orders":
        st.header("Order History")
        if broker == "Zerodha":
            orders_df = get_zerodha_orders(kite)
        else:
            orders_df = get_upstox_orders(upstox_client)
        if not orders_df.empty:
            st.dataframe(orders_df)
        else:
            st.info("No orders found.")

    # View Positions
    if choice == "View Positions":
        st.header("Current Positions")
        if broker == "Zerodha":
            positions_df = get_zerodha_positions(kite)
        else:
            positions_df = get_upstox_positions(upstox_client)
        if not positions_df.empty:
            st.dataframe(positions_df)
        else:
            st.info("No positions found.")

    # View Portfolio
    if choice == "View Portfolio":
        st.header("Portfolio Holdings")
        if broker == "Zerodha":
            portfolio_df = get_zerodha_portfolio(kite)
        else:
            portfolio_df = get_upstox_portfolio(upstox_client)
        if not portfolio_df.empty:
            st.dataframe(portfolio_df)
        else:
            st.info("No holdings found.")

    # Historical Data
    if choice == "Historical Data":
        st.header("Fetch Historical Data (Upstox)")
        instrument_token = st.text_input("Instrument Token (e.g., NSE_EQ|INE528G01035)")
        interval = st.selectbox("Interval", ["1minute", "5minute", "day", "week", "month"])
        from_date = st.date_input("From Date", value=datetime.now() - timedelta(days=30))
        to_date = st.date_input("To Date", value=datetime.now())
        submitted = st.button("Fetch Historical Data")

        if submitted and upstox_client:
            historical_df = get_upstox_historical_data(
                upstox_client,
                instrument_token,
                interval,
                from_date.strftime("%Y-%m-%d"),
                to_date.strftime("%Y-%m-%d")
            )
            if not historical_df.empty:
                st.dataframe(historical_df)
                st.download_button(
                    label="Download Historical Data as CSV",
                    data=historical_df.to_csv(index=False),
                    file_name=f"historical_{instrument_token}_{interval}.csv",
                    mime="text/csv"
                )
            else:
                st.info("No historical data found.")

    # Live Quotes
    if choice == "Live Quotes":
        st.header("Fetch Live Quotes (Upstox)")
        instrument_tokens = st.text_input(
            "Instrument Tokens (comma-separated, e.g., NSE_EQ|INE528G01035,NSE_EQ|INE669E01016)")
        if st.button("Fetch Live Quotes") and upstox_client:
            tokens = [token.strip() for token in instrument_tokens.split(",")]
            quotes_df = get_upstox_live_quotes(upstox_client, tokens)
            if not quotes_df.empty:
                st.dataframe(quotes_df)
            else:
                st.info("No live quotes found.")


if __name__ == "__main__":
    main()