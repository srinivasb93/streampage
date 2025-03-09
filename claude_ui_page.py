import streamlit as st
import pandas as pd
import datetime
import json
import os
from claude_order import UpstoxTradingApp, format_currency, format_percentage

st.set_page_config(page_title="Upstox Trading - Order Management", layout="wide")
# Initialize the app
@st.cache_resource
def get_upstox_app():
    return UpstoxTradingApp()


app = get_upstox_app()

# Page configuration

st.title("Upstox Trading - Order Management System")

# Authentication section
with st.sidebar:
    st.header("Authentication")

    # Load saved configuration if available
    config = app.load_config()

    if config:
        api_key = st.text_input("API Key", value=config.get("api_key", ""), type="password")
        api_secret = st.text_input("API Secret", value=config.get("api_secret", ""), type="password")
        redirect_uri = st.text_input("Redirect URI", value=config.get("redirect_uri", ""))
    else:
        api_key = st.text_input("API Key", type="password")
        api_secret = st.text_input("API Secret", type="password")
        redirect_uri = st.text_input("Redirect URI")

    # Authentication logic
    if not app.authenticated:
        col1, col2 = st.columns(2)

        with col1:
            if st.button("Get Auth URL"):
                if api_key and api_secret and redirect_uri:
                    auth_url = app.authenticate(api_key, api_secret, redirect_uri)
                    if auth_url:
                        st.success("Authorization URL generated")
                        st.markdown(f"[Click here to authorize]({auth_url})")
                    else:
                        st.error("Failed to generate authorization URL")
                else:
                    st.warning("Please enter API credentials")

        auth_code = st.text_input("Auth Code")

        if st.button("Login"):
            if api_key and api_secret and redirect_uri and auth_code:
                if app.get_access_token(api_key, api_secret, redirect_uri, auth_code):
                    st.success("Successfully authenticated!")
                    st.rerun()
                else:
                    st.error("Authentication failed")
            else:
                st.warning("Please enter all required fields")
    else:
        st.success("Authenticated ✓")
        if st.button("Logout"):
            app.authenticated = False
            app.access_token = None
            st.rerun()

    # Display user info if authenticated
    if app.authenticated and app.user_data:
        st.subheader("User Info")
        user = app.user_data
        if hasattr(user, 'email'):
            st.write(f"Email: {user.email}")
        if hasattr(user, 'user_name'):
            st.write(f"Name: {user.user_name}")
        if hasattr(user, 'user_id'):
            st.write(f"User ID: {user.user_id}")

# Main content - only display if authenticated
if app.authenticated:
    # Tabs for different functionalities
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Place Order",
        "Order Book",
        "Trade Book",
        "Order History",
        "Funds & Positions"
    ])

    # Tab 1: Place Order
    with tab1:
        st.header("Place New Order")

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Symbol Selection")

            # Exchange selection
            exchange = st.selectbox("Exchange", app.EXCHANGE_CODES, index=0)

            # Symbol search
            symbol_query = st.text_input("Search Symbol")

            if symbol_query:
                search_results = app.search_instruments(exchange, symbol_query)

                if search_results and "data" in search_results:
                    instruments = search_results["data"].get("instruments", [])

                    if instruments:
                        # Create a dataframe for better display
                        instruments_df = pd.DataFrame([
                            {
                                "Symbol": instr.get("tradingsymbol", ""),
                                "Name": instr.get("name", ""),
                                "Instrument Token": instr.get("instrument_token", ""),
                                "Exchange": instr.get("exchange", "")
                            } for instr in instruments
                        ])

                        st.dataframe(instruments_df)

                        # Allow selecting from results
                        selected_tokens = st.multiselect(
                            "Select Instrument",
                            options=instruments_df["Instrument Token"].tolist(),
                            format_func=lambda x:
                            instruments_df[instruments_df["Instrument Token"] == x]["Symbol"].values[0]
                        )

                        if selected_tokens:
                            selected_token = selected_tokens[0]  # Use first selected token
                            selected_symbol = \
                            instruments_df[instruments_df["Instrument Token"] == selected_token]["Symbol"].values[0]

                            # Store selected instrument in session
                            st.session_state.selected_instrument = {
                                "instrument_token": selected_token,
                                "symbol": selected_symbol,
                                "exchange": exchange
                            }

                            # Add to watchlist button
                            if st.button("Add to Watchlist"):
                                instrument_data = next(
                                    (i for i in instruments if i.get("instrument_token") == selected_token), None)
                                if instrument_data:
                                    if app.add_to_watchlist(instrument_data):
                                        st.success(f"Added {selected_symbol} to watchlist")
                                    else:
                                        st.info("Already in watchlist")
                    else:
                        st.info("No instruments found")
                else:
                    st.warning("Error searching for instruments")

        with col2:
            st.subheader("Order Details")

            # Check if an instrument is selected
            if hasattr(st.session_state, 'selected_instrument'):
                selected = st.session_state.selected_instrument

                st.write(f"Selected: {selected['symbol']} ({selected['exchange']})")

                # Get quote for selected instrument
                quote = app.get_last_traded_price(selected["instrument_token"])

                if quote and "data" in quote:
                    ltp_data = quote["data"].get("ltp", {})
                    if selected["instrument_token"] in ltp_data:
                        current_price = ltp_data[selected["instrument_token"]]
                        st.metric("Current Price", format_currency(current_price))

                # Order form
                order_type = st.selectbox("Order Type", app.ORDER_TYPES)
                transaction_type = st.selectbox("Transaction Type", ["BUY", "SELL"])
                product_type = st.selectbox("Product Type", app.PRODUCT_TYPES)

                quantity = st.number_input("Quantity", min_value=1, step=1, value=1)

                # Price fields based on order type
                if order_type in ["LIMIT", "SL"]:
                    price = st.number_input("Price", min_value=0.05, step=0.05, format="%.2f")

                if order_type in ["SL", "SL-M"]:
                    trigger_price = st.number_input("Trigger Price", min_value=0.05, step=0.05, format="%.2f")

                validity = st.selectbox("Validity", app.DURATION_TYPES)

                disclosed_quantity = st.number_input("Disclosed Quantity", min_value=0, step=1, value=0)

                # Place order button
                if st.button("Place Order"):
                    # Construct order data
                    order_data = {
                        "instrument_token": selected["instrument_token"],
                        "quantity": quantity,
                        "transaction_type": transaction_type,
                        "order_type": order_type,
                        "product": product_type,
                        "validity": validity,
                        "disclosed_quantity": disclosed_quantity if disclosed_quantity > 0 else None,
                    }

                    # Add conditional fields
                    if order_type in ["LIMIT", "SL"]:
                        order_data["price"] = price

                    if order_type in ["SL", "SL-M"]:
                        order_data["trigger_price"] = trigger_price

                    # Place the order
                    response = app.place_order(order_data)

                    if response and "data" in response:
                        order_id = response["data"].get("order_id")
                        st.success(f"Order placed successfully! Order ID: {order_id}")
                    else:
                        st.error("Failed to place order. Please check the details and try again.")
            else:
                st.info("Please select an instrument first")

    # Tab 2: Order Book
    with tab2:
        st.header("Order Book")

        # Refresh button
        if st.button("Refresh Order Book"):
            order_book = app.get_order_book()

            if order_book and "data" in order_book:
                orders = order_book["data"].get("orders", [])

                if orders:
                    # Create DataFrame for better display
                    orders_df = pd.DataFrame([
                        {
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
                            "Order Time": order.get("order_timestamp", "")
                        } for order in orders
                    ])

                    # Allow sorting
                    st.dataframe(orders_df.sort_values(by="Order Time", ascending=False))

                    # Select order for modification/cancellation
                    selected_order_id = st.selectbox(
                        "Select Order to Modify/Cancel",
                        options=[""] + orders_df["Order ID"].tolist()
                    )

                    if selected_order_id:
                        order_data = next((o for o in orders if o.get("order_id") == selected_order_id), None)

                        if order_data:
                            # Display order details
                            with st.expander("Order Details", expanded=True):
                                col1, col2 = st.columns(2)

                                with col1:
                                    st.write(f"Symbol: {order_data.get('tradingsymbol')}")
                                    st.write(f"Exchange: {order_data.get('exchange')}")
                                    st.write(f"Transaction Type: {order_data.get('transaction_type')}")
                                    st.write(f"Order Type: {order_data.get('order_type')}")
                                    st.write(f"Product: {order_data.get('product')}")

                                with col2:
                                    st.write(f"Quantity: {order_data.get('quantity')}")
                                    st.write(f"Status: {order_data.get('status')}")
                                    st.write(f"Price: {format_currency(order_data.get('price', 0))}")
                                    st.write(f"Trigger Price: {format_currency(order_data.get('trigger_price', 0))}")

                            # Action buttons
                            col1, col2 = st.columns(2)

                            with col1:
                                if order_data.get("status") in ["OPEN", "PENDING", "TRIGGER_PENDING"]:
                                    # Modification form
                                    st.subheader("Modify Order")

                                    # Fields that can be modified
                                    new_quantity = st.number_input(
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

                                    new_disclosed_qty = st.number_input(
                                        "New Disclosed Quantity",
                                        min_value=0,
                                        value=order_data.get("disclosed_quantity", 0)
                                    )

                                    if st.button("Modify Order"):
                                        # Construct modification data
                                        modify_data = {
                                            "quantity": new_quantity,
                                            "disclosed_quantity": new_disclosed_qty if new_disclosed_qty > 0 else None,
                                        }

                                        if order_data.get("order_type") in ["LIMIT", "SL"] and "new_price" in locals():
                                            modify_data["price"] = new_price

                                        if order_data.get("order_type") in ["SL",
                                                                            "SL-M"] and "new_trigger_price" in locals():
                                            modify_data["trigger_price"] = new_trigger_price

                                        # Call modify order
                                        response = app.modify_order(selected_order_id, modify_data)

                                        if response and "data" in response:
                                            st.success(
                                                f"Order modified successfully! Order ID: {response['data'].get('order_id')}")
                                            st.rerun()  # Refresh to show updated order
                                        else:
                                            st.error("Failed to modify order. Please check the details and try again.")

                            with col2:
                                # Cancel order button
                                if order_data.get("status") in ["OPEN", "PENDING", "TRIGGER_PENDING"]:
                                    st.subheader("Cancel Order")

                                    if st.button("Cancel Order"):
                                        response = app.cancel_order(selected_order_id)

                                        if response and "data" in response:
                                            st.success(
                                                f"Order cancelled successfully! Order ID: {response['data'].get('order_id')}")
                                            st.rerun()  # Refresh to show updated order
                                        else:
                                            st.error("Failed to cancel order. Please try again.")
                else:
                    st.info("No orders found")
            else:
                st.warning("Failed to fetch order book")

    # Tab 3: Trade Book
    with tab3:
        st.header("Trade Book")

        # Refresh button
        if st.button("Refresh Trade Book"):
            trade_book = app.get_trade_book()

            if trade_book and "data" in trade_book:
                trades = trade_book["data"].get("trades", [])

                if trades:
                    # Create DataFrame for better display
                    trades_df = pd.DataFrame([
                        {
                            "Trade ID": trade.get("trade_id", ""),
                            "Order ID": trade.get("order_id", ""),
                            "Symbol": trade.get("tradingsymbol", ""),
                            "Exchange": trade.get("exchange", ""),
                            "Trans. Type": trade.get("transaction_type", ""),
                            "Product": trade.get("product", ""),
                            "Quantity": trade.get("quantity", 0),
                            "Trade Price": format_currency(trade.get("trade_price", 0)),
                            "Trade Value": format_currency(trade.get("trade_value", 0)),
                            "Trade Time": trade.get("trade_timestamp", "")
                        } for trade in trades
                    ])

                    # Display trades with sorting
                    st.dataframe(trades_df.sort_values(by="Trade Time", ascending=False))

                    # Analysis section
                    with st.expander("Trade Analysis"):
                        # Group trades by transaction type
                        by_trans_type = trades_df.groupby("Trans. Type").agg({
                            "Trade Value": "sum",
                            "Quantity": "sum",
                            "Trade ID": "count"
                        }).reset_index()

                        by_trans_type.columns = ["Transaction Type", "Total Value", "Total Quantity",
                                                 "Number of Trades"]

                        st.subheader("Summary by Transaction Type")
                        st.dataframe(by_trans_type)

                        # Group trades by symbol
                        by_symbol = trades_df.groupby(["Symbol", "Exchange"]).agg({
                            "Trade Value": "sum",
                            "Quantity": "sum",
                            "Trade ID": "count"
                        }).reset_index()

                        by_symbol.columns = ["Symbol", "Exchange", "Total Value", "Total Quantity", "Number of Trades"]

                        st.subheader("Summary by Symbol")
                        st.dataframe(by_symbol)
                else:
                    st.info("No trades found")
            else:
                st.warning("Failed to fetch trade book")

    # Tab 4: Order History
    with tab4:
        st.header("Order History")

        if app.order_history:
            # Convert to DataFrame for display
            history_data = []

            for entry in app.order_history:
                if "order_data" in entry:
                    # Extract relevant fields
                    data = {
                        "Timestamp": entry.get("timestamp", ""),
                        "Action": entry.get("type", ""),
                        "Instrument": entry.get("order_data", {}).get("instrument_token", ""),
                        "Quantity": entry.get("order_data", {}).get("quantity", ""),
                        "Transaction": entry.get("order_data", {}).get("transaction_type", ""),
                        "Order Type": entry.get("order_data", {}).get("order_type", ""),
                        "Status": "Success" if "response" in entry else "Failed"
                    }

                    if "error" in entry:
                        data["Error"] = entry["error"]

                    if "response" in entry and "data" in entry["response"]:
                        data["Order ID"] = entry["response"]["data"].get("order_id", "")

                    history_data.append(data)
                elif "order_id" in entry:
                    # For modify/cancel actions
                    data = {
                        "Timestamp": entry.get("timestamp", ""),
                        "Action": entry.get("type", ""),
                        "Order ID": entry.get("order_id", ""),
                        "Status": "Success" if "response" in entry else "Failed"
                    }

                    if "error" in entry:
                        data["Error"] = entry["error"]

                    history_data.append(data)

            if history_data:
                history_df = pd.DataFrame(history_data)

                # Display with filtering and sorting
                st.dataframe(history_df.sort_values(by="Timestamp", ascending=False))

                # Clear history button
                if st.button("Clear History"):
                    if st.checkbox("Confirm Clear History"):
                        app.order_history = []
                        app.save_order_history()
                        st.success("Order history cleared")
                        st.rerun()
            else:
                st.info("No order history found")
        else:
            st.info("No order history found")

    # Tab 5: Funds & Positions
    with tab5:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Funds")
            if st.button("Refresh Funds"):
                funds = app.get_funds()

                if funds and "data" in funds:
                    fund_data = funds["data"].get("equity", {})

                    if fund_data:
                        # Create a more readable display
                        st.write("Fund Details:")

                        # Layout in columns
                        c1, c2 = st.columns(2)

                        with c1:
                            st.metric("Available Balance", format_currency(fund_data.get("available_balance", 0)))
                            st.metric("Used Margin", format_currency(fund_data.get("used_margin", 0)))
                            st.metric("Available Margin", format_currency(fund_data.get("available_margin", 0)))

                        with c2:
                            st.metric("Net Value", format_currency(fund_data.get("net", 0)))
                            st.metric("Collateral", format_currency(fund_data.get("collateral", 0)))
                            st.metric("Exposure Margin", format_currency(fund_data.get("exposure_margin", 0)))

                        # Detailed breakdown if needed
                        with st.expander("Detailed Breakdown"):
                            for key, value in fund_data.items():
                                if isinstance(value, (int, float)):
                                    st.write(f"{key.replace('_', ' ').title()}: {format_currency(value)}")
                                else:
                                    st.write(f"{key.replace('_', ' ').title()}: {value}")
                    else:
                        st.info("No fund data available")
                else:
                    st.warning("Failed to fetch funds")

        with col2:
            st.subheader("Positions")

            # Add position type selector
            position_type = st.selectbox("Position Type", app.POSITION_TYPES)

            if st.button("Refresh Positions"):
                positions = app.get_positions(position_type)

                if positions and "data" in positions:
                    positions_data = positions["data"].get("positions", [])

                    if positions_data:
                        # Create DataFrame for better display
                        positions_df = pd.DataFrame([
                            {
                                "Symbol": pos.get("tradingsymbol", ""),
                                "Exchange": pos.get("exchange", ""),
                                "Product": pos.get("product", ""),
                                "Quantity": pos.get("quantity", 0),
                                "Avg Price": format_currency(pos.get("average_price", 0)),
                                "LTP": format_currency(pos.get("last_price", 0)),
                                "PnL": format_currency(pos.get("pnl", 0)),
                                "PnL %": format_percentage(pos.get("pnl_percentage", 0)),
                                "Value": format_currency(pos.get("value", 0))
                            } for pos in positions_data
                        ])

                        # Display positions
                        st.dataframe(positions_df)

                        # Summary metrics
                        if not positions_df.empty:
                            total_pnl = sum([pos.get("pnl", 0) for pos in positions_data])
                            st.metric("Total P&L", format_currency(total_pnl),
                                      delta=format_percentage(total_pnl / sum([pos.get("value", 0) for pos in
                                                                               positions_data]) * 100) if total_pnl != 0 else None)
                    else:
                        st.info(f"No {position_type} positions found")
                else:
                    st.warning("Failed to fetch positions")

# Display message if not authenticated
else:
    st.info("Please authenticate to use the Order Management System.")
    st.write("Enter your Upstox API credentials in the sidebar to get started.")