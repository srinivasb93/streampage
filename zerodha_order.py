import streamlit as st
from jugaad_trader import Zerodha
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import time

# Title and styling
st.set_page_config(page_title="Advanced Stocks Order Management", layout="wide")
st.title("Advanced Stocks Order Management System")

# Initialize Zerodha client
kite = Zerodha()

# Authentication
if 'access_token' not in st.session_state:
    st.write("Please provide your Zerodha access token.")
    access_token = st.text_input("Enter Access Token", type="password")
    if access_token:
        try:
            st.session_state['access_token'] = access_token
            kite.set_access_token(access_token)
            st.success("Authentication successful!")
        except Exception as e:
            st.error(f"Authentication failed: {str(e)}")
else:
    kite.set_access_token(st.session_state['access_token'])

# Sidebar navigation
page = st.sidebar.selectbox("Choose a page", [
    "Order Placement", "Order Modification", "Order Cancellation",
    "Order Status", "Portfolio", "Positions", "Historical Data", "Live Market Data"
])


# Helper function for error handling
def handle_error(action, e):
    st.error(f"Error during {action}: {str(e)}")
    return None


# Order Placement
if page == "Order Placement":
    st.header("Place an Order")
    with st.form(key="place_order_form"):
        symbol = st.text_input("Stock Symbol (e.g., TCS, RELIANCE)")
        qty = st.number_input("Quantity", min_value=1, step=1)
        order_type = st.selectbox("Order Type", ["MARKET", "LIMIT"])
        transaction_type = st.selectbox("Transaction Type", ["BUY", "SELL"])
        price = st.number_input("Price (for LIMIT)", min_value=0.0, step=0.05) if order_type == "LIMIT" else None
        product = st.selectbox("Product", ["CNC", "MIS"])  # CNC: Delivery, MIS: Intraday
        submit = st.form_submit_button("Place Order")

    if submit:
        try:
            order_id = kite.place_order(
                variety="regular",
                exchange="NSE",
                tradingsymbol=symbol.upper(),
                transaction_type=transaction_type,
                quantity=qty,
                product=product,
                order_type=order_type,
                price=price if order_type == "LIMIT" else None
            )
            st.success(f"Order placed! Order ID: {order_id}")
        except Exception as e:
            handle_error("placing order", e)

# Order Modification
elif page == "Order Modification":
    st.header("Modify an Order")
    orders = kite.orders()
    if orders:
        pending_orders = [o for o in orders if o['status'] in ['OPEN', 'TRIGGER PENDING']]
        if pending_orders:
            order_id = st.selectbox("Select Order to Modify", [o['order_id'] for o in pending_orders])
            selected_order = next(o for o in pending_orders if o['order_id'] == order_id)
            with st.form(key="modify_order_form"):
                qty = st.number_input("New Quantity", value=selected_order['quantity'], min_value=1, step=1)
                price = st.number_input("New Price", value=float(selected_order['price'] or 0), min_value=0.0,
                                        step=0.05)
                submit = st.form_submit_button("Modify Order")
            if submit:
                try:
                    kite.modify_order(
                        variety="regular",
                        order_id=order_id,
                        quantity=qty,
                        price=price if selected_order['order_type'] == "LIMIT" else None
                    )
                    st.success(f"Order {order_id} modified successfully!")
                except Exception as e:
                    handle_error("modifying order", e)
        else:
            st.write("No pending orders to modify.")
    else:
        st.write("Unable to fetch orders.")

# Order Cancellation
elif page == "Order Cancellation":
    st.header("Cancel an Order")
    orders = kite.orders()
    if orders:
        cancellable_orders = [o for o in orders if o['status'] in ['OPEN', 'TRIGGER PENDING']]
        if cancellable_orders:
            order_id = st.selectbox("Select Order to Cancel", [o['order_id'] for o in cancellable_orders])
            if st.button("Cancel Order"):
                try:
                    kite.cancel_order(variety="regular", order_id=order_id)
                    st.success(f"Order {order_id} cancelled successfully!")
                except Exception as e:
                    handle_error("cancelling order", e)
        else:
            st.write("No cancellable orders.")
    else:
        st.write("Unable to fetch orders.")

# Order Status
elif page == "Order Status":
    st.header("Order Status")
    try:
        orders = kite.orders()
        if orders:
            df = pd.DataFrame(orders)
            st.dataframe(
                df[['order_id', 'tradingsymbol', 'status', 'quantity', 'order_type', 'transaction_type', 'price']])
        else:
            st.write("No orders found.")
    except Exception as e:
        handle_error("fetching orders", e)

# Portfolio
elif page == "Portfolio":
    st.header("Portfolio")
    try:
        holdings = kite.holdings()
        if holdings:
            df = pd.DataFrame(holdings)
            st.dataframe(df[['tradingsymbol', 'quantity', 'average_price', 'last_price', 'pnl']])
        else:
            st.write("No holdings found.")
    except Exception as e:
        handle_error("fetching portfolio", e)

# Positions
elif page == "Positions":
    st.header("Positions")
    try:
        positions = kite.positions()['net']
        if positions:
            df = pd.DataFrame(positions)
            st.dataframe(df[['tradingsymbol', 'quantity', 'average_price', 'last_price', 'pnl']])
        else:
            st.write("No positions found.")
    except Exception as e:
        handle_error("fetching positions", e)

# Historical Data
elif page == "Historical Data":
    st.header("Historical Data")
    symbol = st.text_input("Stock Symbol (e.g., TCS)", "RELIANCE")
    days = st.slider("Days of History", 1, 100, 30)
    if st.button("Fetch Historical Data"):
        try:
            to_date = datetime.now()
            from_date = to_date - timedelta(days=days)
            data = kite.historical_data(
                instrument_token=kite.ltp(f"NSE:{symbol.upper()}")[f"NSE:{symbol.upper()}"]["instrument_token"],
                from_date=from_date,
                to_date=to_date,
                interval="day"
            )
            df = pd.DataFrame(data)
            st.dataframe(df[['date', 'open', 'high', 'low', 'close', 'volume']])
            fig = px.line(df, x="date", y="close", title=f"{symbol} Closing Price")
            st.plotly_chart(fig)
        except Exception as e:
            handle_error("fetching historical data", e)

# Live Market Data
elif page == "Live Market Data":
    st.header("Live Market Data")
    symbol = st.text_input("Stock Symbol (e.g., TCS)", "RELIANCE")
    if st.button("Fetch Live Data"):
        try:
            while True:
                quote = kite.ltp(f"NSE:{symbol.upper()}")
                st.write(f"Last Price: {quote[f'NSE:{symbol.upper()}']['last_price']}")
                time.sleep(1)  # Refresh every second
        except Exception as e:
            handle_error("fetching live data", e)