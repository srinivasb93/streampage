import streamlit as st
import pandas as pd
from upstox_client import LoginApi
from upstox_client.rest import ApiException
from upstox_client.api.user_api import UserApi
from upstox_client.api.order_api import OrderApi
from upstox_client.api.market_quote_api import MarketQuoteApi
from upstox_client.api.portfolio_api import PortfolioApi
from upstox_client.api.websocket_api import WebsocketApi
from upstox_client.configuration import Configuration
import json
import logging
import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Upstox API Configuration
def get_configuration():
    configuration = Configuration()
    configuration.access_token = st.session_state.get('access_token', '')
    return configuration


# Authentication Function
def authenticate():
    st.title("Upstox Order Management System")

    with st.expander("Authentication", expanded=not st.session_state.get('authenticated', False)):
        api_key = st.text_input("API Key", value=os.getenv('UPSTOX_API_KEY', ''), type="password")
        api_secret = st.text_input("API Secret", value=os.getenv('UPSTOX_API_SECRET', ''), type="password")
        redirect_uri = st.text_input("Redirect URI",
                                     value=os.getenv('UPSTOX_REDIRECT_URI', 'https://localhost:8501/callback'))

        if 'auth_code' not in st.session_state:
            st.session_state.auth_code = ''

        auth_code = st.text_input("Authorization Code", value=st.session_state.auth_code)

        # Authorization URL generation
        if st.button("Generate Authorization URL"):
            auth_url = f"https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={api_key}&redirect_uri={redirect_uri}"
            st.markdown(f"[Click here to authorize]({auth_url})")
            st.info("Please authorize the application and copy the authorization code from the redirect URL.")

        # Token Generation
        if st.button("Generate Access Token") and auth_code:
            try:
                configuration = Configuration()
                configuration.host = "https://api-v2.upstox.com"

                api_instance = LoginApi(configuration)
                response = api_instance.token(
                    code=auth_code,
                    client_id=api_key,
                    client_secret=api_secret,
                    redirect_uri=redirect_uri,
                    grant_type="authorization_code",
                    api_version="2.0"
                )

                st.session_state.access_token = response.access_token
                st.session_state.authenticated = True
                st.success("Authentication successful!")
                st.session_state.auth_code = auth_code

                # Get user profile for verification
                configuration.access_token = st.session_state.access_token
                user_api = UserApi(configuration)
                profile = user_api.get_profile()
                st.session_state.user_id = profile.data.user_id
                st.session_state.user_name = profile.data.name
                st.success(f"Logged in as: {profile.data.name} (User ID: {profile.data.user_id})")

            except ApiException as e:
                st.error(f"Authentication failed: {e}")
                logger.error(f"Authentication error: {e}")


# Get available instruments
def get_instruments():
    try:
        if not st.session_state.get('authenticated', False):
            return pd.DataFrame()

        configuration = get_configuration()
        market_api = MarketQuoteApi(configuration)
        response = market_api.get_instrument_details(instrument_key=["NSE_INDEX|Nifty Bank", "NSE_INDEX|Nifty 50"])

        # This is a minimal example. In a real app, you would fetch more instruments
        # You can expand this by fetching all NSE instruments and filtering as needed
        return pd.DataFrame(response.data)
    except ApiException as e:
        st.error(f"Failed to fetch instruments: {e}")
        logger.error(f"Instrument fetch error: {e}")
        return pd.DataFrame()


# Get portfolio positions
def get_positions():
    try:
        if not st.session_state.get('authenticated', False):
            return pd.DataFrame()

        configuration = get_configuration()
        portfolio_api = PortfolioApi(configuration)
        response = portfolio_api.get_positions()

        if response.data and hasattr(response.data, 'positions') and response.data.positions:
            return pd.DataFrame([position.to_dict() for position in response.data.positions])
        else:
            return pd.DataFrame()
    except ApiException as e:
        st.error(f"Failed to fetch positions: {e}")
        logger.error(f"Positions fetch error: {e}")
        return pd.DataFrame()


# Get order book
def get_order_book():
    try:
        if not st.session_state.get('authenticated', False):
            return pd.DataFrame()

        configuration = get_configuration()
        order_api = OrderApi(configuration)
        response = order_api.get_order_book()

        if response.data and response.data.orders:
            return pd.DataFrame([order.to_dict() for order in response.data.orders])
        else:
            return pd.DataFrame()
    except ApiException as e:
        st.error(f"Failed to fetch order book: {e}")
        logger.error(f"Order book fetch error: {e}")
        return pd.DataFrame()


# Place a new order
def place_order(instrument_key, quantity, product, order_type, price, validity, disclosed_quantity, trigger_price,
                is_amo):
    try:
        configuration = get_configuration()
        order_api = OrderApi(configuration)

        transaction_type = st.session_state.transaction_type

        response = order_api.place_order(
            variety="regular" if not is_amo else "amo",
            data={
                "instrument_token": instrument_key,
                "quantity": quantity,
                "product": product,
                "validity": validity,
                "price": price,
                "tag": f"order_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}",
                "order_type": order_type,
                "transaction_type": transaction_type,
                "disclosed_quantity": disclosed_quantity,
                "trigger_price": trigger_price,
                "is_amo": is_amo
            }
        )

        return True, response.data.order_id
    except ApiException as e:
        error_body = json.loads(e.body)
        error_message = error_body.get('errors', [{}])[0].get('message', str(e))
        return False, error_message


# Modify an existing order
def modify_order(order_id, quantity, price, trigger_price, disclosed_quantity, validity):
    try:
        configuration = get_configuration()
        order_api = OrderApi(configuration)

        response = order_api.modify_order(
            order_id=order_id,
            variety="regular",
            data={
                "quantity": quantity,
                "price": price,
                "trigger_price": trigger_price,
                "disclosed_quantity": disclosed_quantity,
                "validity": validity
            }
        )

        return True, response.data.order_id
    except ApiException as e:
        error_body = json.loads(e.body)
        error_message = error_body.get('errors', [{}])[0].get('message', str(e))
        return False, error_message


# Cancel an order
def cancel_order(order_id):
    try:
        configuration = get_configuration()
        order_api = OrderApi(configuration)

        response = order_api.cancel_order(
            order_id=order_id,
            variety="regular"
        )

        return True, response.data.order_id
    except ApiException as e:
        error_body = json.loads(e.body)
        error_message = error_body.get('errors', [{}])[0].get('message', str(e))
        return False, error_message


# Main application
def main():
    # Initialize session state
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False

    if 'transaction_type' not in st.session_state:
        st.session_state.transaction_type = "BUY"

    # Authentication section
    authenticate()

    # Only show the main application if authenticated
    if st.session_state.get('authenticated', False):

        # Sidebar navigation
        st.sidebar.title("Navigation")
        page = st.sidebar.radio("Select Page",
                                ["Place Order", "Order Book", "Positions", "Bracket Orders", "Cover Orders"])

        st.sidebar.header("Account Information")
        st.sidebar.text(f"User: {st.session_state.get('user_name', 'Not authenticated')}")

        if st.sidebar.button("Logout"):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.experimental_rerun()

        # Page Router
        if page == "Place Order":
            display_place_order_page()
        elif page == "Order Book":
            display_order_book_page()
        elif page == "Positions":
            display_positions_page()
        elif page == "Bracket Orders":
            display_bracket_order_page()
        elif page == "Cover Orders":
            display_cover_order_page()


# Place Order Page
def display_place_order_page():
    st.header("Place New Order")

    col1, col2 = st.columns(2)

    with col1:
        st.radio("Transaction Type", ["BUY", "SELL"], key="transaction_type")

    with col2:
        is_amo = st.checkbox("After Market Order (AMO)")

    # Instrument selection
    instrument_search = st.text_input("Search Instrument", "")

    # Display sample instruments
    # In a real app, you would implement proper instrument search
    if instrument_search:
        st.info("In a production app, this would show filtered results based on your search.")

    st.subheader("Selected Instrument")
    instrument_key = st.text_input("Instrument Key (e.g., NSE_EQ|INE001A01036)")

    # Order details
    col1, col2, col3 = st.columns(3)

    with col1:
        quantity = st.number_input("Quantity", min_value=1, value=1)
        product = st.selectbox("Product", ["D", "I", "M"])

    with col2:
        order_type = st.selectbox("Order Type", ["MARKET", "LIMIT", "SL", "SL-M"])
        price = st.number_input("Price (₹)", min_value=0.05, value=0.0, step=0.05, disabled=order_type == "MARKET")

    with col3:
        validity = st.selectbox("Validity", ["DAY", "IOC"])
        disclosed_quantity = st.number_input("Disclosed Qty", min_value=0, value=0)
        trigger_price = st.number_input("Trigger Price (₹)", min_value=0.0, value=0.0, step=0.05,
                                        disabled=order_type not in ["SL", "SL-M"])

    # Place order button
    if st.button("Place Order"):
        if not instrument_key:
            st.error("Please enter an instrument key")
        else:
            success, message = place_order(
                instrument_key=instrument_key,
                quantity=quantity,
                product=product,
                order_type=order_type,
                price=price,
                validity=validity,
                disclosed_quantity=disclosed_quantity,
                trigger_price=trigger_price,
                is_amo=is_amo
            )

            if success:
                st.success(f"Order placed successfully! Order ID: {message}")
            else:
                st.error(f"Failed to place order: {message}")


# Order Book Page
def display_order_book_page():
    st.header("Order Book")

    if st.button("Refresh Orders"):
        st.session_state.order_book = get_order_book()

    if 'order_book' not in st.session_state:
        st.session_state.order_book = get_order_book()

    order_book = st.session_state.order_book

    if order_book.empty:
        st.info("No orders found")
    else:
        st.dataframe(order_book)

        # Order modification section
        st.subheader("Modify Order")

        if not order_book.empty and 'order_id' in order_book.columns:
            order_ids = order_book['order_id'].tolist()
            selected_order = st.selectbox("Select Order to Modify", order_ids)

            if selected_order:
                order_row = order_book[order_book['order_id'] == selected_order].iloc[0]

                col1, col2 = st.columns(2)

                with col1:
                    mod_quantity = st.number_input("New Quantity",
                                                   min_value=1,
                                                   value=int(order_row.get('quantity', 1)))
                    mod_price = st.number_input("New Price (₹)",
                                                min_value=0.05,
                                                value=float(order_row.get('price', 0.0)),
                                                step=0.05)

                with col2:
                    mod_trigger = st.number_input("New Trigger Price (₹)",
                                                  min_value=0.0,
                                                  value=float(order_row.get('trigger_price', 0.0)),
                                                  step=0.05)
                    mod_disclosed = st.number_input("New Disclosed Qty",
                                                    min_value=0,
                                                    value=int(order_row.get('disclosed_quantity', 0)))
                    mod_validity = st.selectbox("New Validity",
                                                ["DAY", "IOC"],
                                                index=0 if order_row.get('validity', '') == 'DAY' else 1)

                col1, col2 = st.columns(2)

                with col1:
                    if st.button("Modify Order"):
                        success, message = modify_order(
                            order_id=selected_order,
                            quantity=mod_quantity,
                            price=mod_price,
                            trigger_price=mod_trigger,
                            disclosed_quantity=mod_disclosed,
                            validity=mod_validity
                        )

                        if success:
                            st.success(f"Order modified successfully! Order ID: {message}")
                            st.session_state.order_book = get_order_book()
                        else:
                            st.error(f"Failed to modify order: {message}")

                with col2:
                    if st.button("Cancel Order"):
                        success, message = cancel_order(order_id=selected_order)

                        if success:
                            st.success(f"Order cancelled successfully! Order ID: {message}")
                            st.session_state.order_book = get_order_book()
                        else:
                            st.error(f"Failed to cancel order: {message}")


# Positions Page
def display_positions_page():
    st.header("Current Positions")

    if st.button("Refresh Positions"):
        st.session_state.positions = get_positions()

    if 'positions' not in st.session_state:
        st.session_state.positions = get_positions()

    positions = st.session_state.positions

    if positions.empty:
        st.info("No positions found")
    else:
        st.dataframe(positions)

        # Summary metrics
        if not positions.empty and 'realized_profit' in positions.columns and 'unrealized_profit' in positions.columns:
            total_realized = positions['realized_profit'].sum()
            total_unrealized = positions['unrealized_profit'].sum()

            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Total Realized P&L", f"₹{total_realized:.2f}")

            with col2:
                st.metric("Total Unrealized P&L", f"₹{total_unrealized:.2f}")

            with col3:
                st.metric("Net P&L", f"₹{(total_realized + total_unrealized):.2f}")


# Bracket Order Page
def display_bracket_order_page():
    st.header("Place Bracket Order")

    st.warning(
        "Bracket orders allow you to place a main order with take-profit and stop-loss orders automatically attached.")

    col1, col2 = st.columns(2)

    with col1:
        st.radio("Transaction Type", ["BUY", "SELL"], key="bo_transaction_type")

    # Instrument selection
    instrument_key = st.text_input("Instrument Key", "", key="bo_instrument")

    # Order details
    col1, col2, col3 = st.columns(3)

    with col1:
        quantity = st.number_input("Quantity", min_value=1, value=1, key="bo_quantity")
        product = st.selectbox("Product", ["B"], key="bo_product", disabled=True)

    with col2:
        price = st.number_input("Entry Price (₹)", min_value=0.05, value=0.0, step=0.05, key="bo_price")
        stop_loss = st.number_input("Stop Loss (₹)", min_value=0.05, value=0.0, step=0.05)

    with col3:
        take_profit = st.number_input("Take Profit (₹)", min_value=0.05, value=0.0, step=0.05)
        trailing_sl = st.number_input("Trailing Stop Loss (₹)", min_value=0.0, value=0.0, step=0.05)

    # Place order button
    if st.button("Place Bracket Order"):
        if not instrument_key:
            st.error("Please enter an instrument key")
        elif price <= 0:
            st.error("Please enter a valid price")
        elif stop_loss <= 0:
            st.error("Please enter a valid stop loss price")
        elif take_profit <= 0:
            st.error("Please enter a valid take profit price")
        else:
            st.info("This would place a bracket order with the specified parameters.")
            st.warning(
                "Note: Implementation details for bracket orders depend on Upstox API specifics which may need adjustments based on their latest documentation.")


# Cover Order Page
def display_cover_order_page():
    st.header("Place Cover Order")

    st.warning("Cover orders allow you to place a main order with a mandatory stop-loss order.")

    col1, col2 = st.columns(2)

    with col1:
        st.radio("Transaction Type", ["BUY", "SELL"], key="co_transaction_type")

    # Instrument selection
    instrument_key = st.text_input("Instrument Key", "", key="co_instrument")

    # Order details
    col1, col2 = st.columns(2)

    with col1:
        quantity = st.number_input("Quantity", min_value=1, value=1, key="co_quantity")
        product = st.selectbox("Product", ["C"], key="co_product", disabled=True)

    with col2:
        price = st.number_input("Price (₹)", min_value=0.05, value=0.0, step=0.05, key="co_price")
        stop_loss = st.number_input("Stop Loss Trigger Price (₹)", min_value=0.05, value=0.0, step=0.05, key="co_sl")

    # Place order button
    if st.button("Place Cover Order"):
        if not instrument_key:
            st.error("Please enter an instrument key")
        elif price <= 0:
            st.error("Please enter a valid price")
        elif stop_loss <= 0:
            st.error("Please enter a valid stop loss price")
        else:
            st.info("This would place a cover order with the specified parameters.")
            st.warning(
                "Note: Implementation details for cover orders depend on Upstox API specifics which may need adjustments based on their latest documentation.")


if __name__ == "__main__":
    main()