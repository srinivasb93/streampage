import streamlit as st
from jugaad_trader import Zerodha
import pandas as pd
import datetime

# Initialize session state if not already
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

# Login Section
st.sidebar.header("Zerodha Login")
username = st.sidebar.text_input("Username")
password = st.sidebar.text_input("Password", type="password")
totp = st.sidebar.text_input("TOTP")
login_button = st.sidebar.button("Login")

if login_button:
    kite = Zerodha()
    try:
        kite.user_id = username
        kite.password = password
        kite.totp = totp
        kite.login()
        st.session_state.authenticated = True
        st.session_state.kite = kite
        st.sidebar.success("Login successful!")
    except Exception as e:
        st.sidebar.error(f"Login failed: {e}")

# Check authentication before proceeding
if not st.session_state.authenticated:
    st.stop()

kite = st.session_state.kite

# Order Placement
st.header("Place an Order")
order_type = st.selectbox("Order Type", ["BUY", "SELL"])
symbol = st.text_input("Enter Symbol (e.g., NSE:INFY)")
quantity = st.number_input("Quantity", min_value=1, step=1)
price = st.number_input("Price", min_value=0.0, step=0.05, format="%.2f")
order_category = st.radio("Order Category", ["MARKET", "LIMIT", "SL", "SL-M"])
place_order = st.button("Submit Order")

if place_order:
    try:
        order_id = kite.place_order(
            exchange=symbol.split(":")[0],
            tradingsymbol=symbol.split(":")[1],
            transaction_type=order_type,
            quantity=quantity,
            price=price if order_category != "MARKET" else 0,
            order_type=order_category,
            variety="regular"
        )
        st.success(f"Order placed successfully! Order ID: {order_id}")
    except Exception as e:
        st.error(f"Order failed: {e}")

# Order Book
st.header("Order Book")
orders = kite.orders()
if orders:
    df_orders = pd.DataFrame(orders)
    st.dataframe(df_orders[['order_id', 'status', 'tradingsymbol', 'transaction_type', 'quantity', 'price', 'order_type']])
else:
    st.write("No orders found.")

# Portfolio Holdings
st.header("Portfolio Holdings")
holdings = kite.holdings()
if holdings:
    df_holdings = pd.DataFrame(holdings)
    st.dataframe(df_holdings[['tradingsymbol', 'quantity', 'average_price', 'last_price', 'pnl']])
else:
    st.write("No holdings found.")
