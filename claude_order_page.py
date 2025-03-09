import streamlit as st
import pandas as pd
import numpy as np
import datetime
import plotly.graph_objects as go
from upstox_client.api.market_quote_api import MarketQuoteApi
from upstox_client.api.order_api import OrderApi
from upstox_client.api.portfolio_api import PortfolioApi
from upstox_client.api.user_api import UserApi
from upstox_client.api.websocket_api import WebsocketApi
from upstox_client.rest import ApiException
import upstox_client.configuration
from upstox_client.rest import ApiException
import lightweight_charts as lwc
import json
import time
import requests
import pandas_ta as ta

st.set_page_config(page_title="Upstox Trading Terminal", layout="wide")

# Initialize session states if not already set
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'configuration' not in st.session_state:
    st.session_state.configuration = None
if 'access_token' not in st.session_state:
    st.session_state.access_token = None
if 'watchlist' not in st.session_state:
    st.session_state.watchlist = []
if 'selected_symbol' not in st.session_state:
    st.session_state.selected_symbol = None
if 'order_history' not in st.session_state:
    st.session_state.order_history = []
if 'positions' not in st.session_state:
    st.session_state.positions = []
if 'holdings' not in st.session_state:
    st.session_state.holdings = []
if 'market_data' not in st.session_state:
    st.session_state.market_data = {}

# Sidebar for authentication and main menu
st.sidebar.title("Menu")

# Authentication section
st.sidebar.header("Authentication")

with st.sidebar.expander("Upstox Authentication", expanded=not st.session_state.authenticated):
    if not st.session_state.authenticated:
        api_key = st.text_input("API Key", type="password")
        api_secret = st.text_input("API Secret", type="password")
        redirect_uri = st.text_input("Redirect URI", value="https://localhost:8501/")

        if st.button("Authenticate"):
            try:
                # Configure OAuth2 access token for authorization
                configuration = upstox_client.configuration.Configuration()
                configuration.access_token = None

                # Create an authorization URL
                auth_url = f"https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={api_key}&redirect_uri={redirect_uri}"
                st.markdown(f"[Click here to authorize]({auth_url})")

                auth_code = st.text_input("Enter the authorization code:")

                if st.button("Authorize"):
                    # Exchange the authorization code for an access token
                    token_url = "https://api-v2.upstox.com/login/authorization/token"
                    payload = {
                        "code": auth_code,
                        "client_id": api_key,
                        "client_secret": api_secret,
                        "redirect_uri": redirect_uri,
                        "grant_type": "authorization_code"
                    }

                    response = requests.post(token_url, data=payload)
                    if response.status_code == 200:
                        token_data = response.json()
                        access_token = token_data.get("access_token")
                        configuration.access_token = access_token
                        st.session_state.configuration = configuration
                        st.session_state.access_token = access_token
                        st.session_state.authenticated = True
                        st.success("Authenticated successfully!")
                        st.experimental_fragment()
                    else:
                        st.error(f"Authentication failed: {response.text}")

            except Exception as e:
                st.error(f"Authentication error: {str(e)}")
    else:
        st.success("Authenticated")
        if st.button("Logout"):
            st.session_state.authenticated = False
            st.session_state.configuration = None
            st.session_state.access_token = None
            st.experimental_rerun()

# Main navigation menu
if st.session_state.authenticated:
    menu_options = ["Dashboard", "Market Watch", "Order Placement", "Order Book", "Positions", "Holdings",
                    "Chart Analysis", "Algo Trading"]
    selected_menu = st.sidebar.selectbox("Navigate", menu_options)

    # Initialize APIs
    market_data_api = MarketQuoteApi(upstox_client.ApiClient(st.session_state.configuration))
    order_api = OrderApi(upstox_client.ApiClient(st.session_state.configuration))
    portfolio_api = PortfolioApi(upstox_client.ApiClient(st.session_state.configuration))
    user_api = UserApi(upstox_client.ApiClient(st.session_state.configuration))


    # Function to fetch market data for a symbol
    def fetch_market_data(symbol_list, interval="1D"):
        data_dict = {}
        for symbol in symbol_list:
            try:
                # Get historical candle data
                end_date = datetime.datetime.now()
                start_date = end_date - datetime.timedelta(days=30)

                response = market_data_api.get_historical_candle_data(
                    instrument_key=symbol,
                    interval=interval,
                    to_date=end_date.strftime("%Y-%m-%d"),
                    from_date=start_date.strftime("%Y-%m-%d")
                )

                if response.status == "success" and response.data:
                    candle_data = response.data.candles
                    df = pd.DataFrame(candle_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df.set_index('timestamp', inplace=True)
                    data_dict[symbol] = df
            except ApiException as e:
                st.error(f"Exception when calling MarketDataApi->get_historical_candle_data: {e}")

        return data_dict


    # Function to place an order
    def place_order(symbol, quantity, price, order_type, transaction_type, product_type):
        try:
            order_request = {
                "instrument_token": symbol,
                "quantity": quantity,
                "price": price,
                "order_type": order_type,
                "transaction_type": transaction_type,
                "product_type": product_type,
                "disclosed_quantity": 0,
                "trigger_price": 0,
                "validity": "DAY",
                "is_amo": False
            }

            response = order_api.place_order(order_request)
            return response
        except ApiException as e:
            st.error(f"Exception when calling OrderApi->place_order: {e}")
            return None


    # Function to fetch order history
    def fetch_order_history():
        try:
            response = order_api.get_order_book()
            if response.status == "success" and response.data:
                orders = response.data
                df = pd.DataFrame([order.__dict__ for order in orders])
                return df
            return pd.DataFrame()
        except ApiException as e:
            st.error(f"Exception when calling OrderApi->get_order_book: {e}")
            return pd.DataFrame()


    # Function to fetch positions
    def fetch_positions():
        try:
            response = portfolio_api.get_positions()
            if response.status == "success" and response.data:
                positions = response.data
                df = pd.DataFrame([position.__dict__ for position in positions])
                return df
            return pd.DataFrame()
        except ApiException as e:
            st.error(f"Exception when calling PortfolioApi->get_positions: {e}")
            return pd.DataFrame()


    # Function to fetch holdings
    def fetch_holdings():
        try:
            response = portfolio_api.get_holdings()
            if response.status == "success" and response.data:
                holdings = response.data
                df = pd.DataFrame([holding.__dict__ for holding in holdings])
                return df
            return pd.DataFrame()
        except ApiException as e:
            st.error(f"Exception when calling PortfolioApi->get_holdings: {e}")
            return pd.DataFrame()


    # Function to search instruments
    def search_instruments(query):
        try:
            response = market_data_api.search_instruments(query)
            if response.status == "success" and response.data:
                instruments = response.data
                return instruments
            return []
        except ApiException as e:
            st.error(f"Exception when calling MarketDataApi->search_instruments: {e}")
            return []


    # Calculating technical indicators
    def calculate_indicators(df):
        # Make a copy to avoid pandas warning
        df_copy = df.copy()

        # Calculate SMA
        df_copy['sma_20'] = ta.sma(df_copy['close'], timeperiod=20)
        df_copy['sma_50'] = ta.sma(df_copy['close'], timeperiod=50)

        # Calculate MACD
        df_copy['macd'], df_copy['macd_signal'], df_copy['macd_hist'] = ta.macd(
            df_copy['close'], fastperiod=12, slowperiod=26, signalperiod=9)

        # Calculate RSI
        df_copy['rsi'] = ta.rsi(df_copy['close'], timeperiod=14)

        # Calculate Bollinger Bands
        df_copy['bb_upper'], df_copy['bb_middle'], df_copy['bb_lower'] = ta.bbands(
            df_copy['close'], timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)

        # Stochastic Oscillator
        df_copy['slowk'], df_copy['slowd'] = ta.stoch(
            df_copy['high'], df_copy['low'], df_copy['close'],
            fastk_period=14, slowk_period=3, slowk_matype=0,
            slowd_period=3, slowd_matype=0)

        return df_copy


    # Function to generate trading signals
    def generate_signals(df):
        signals = []

        # MACD Crossover Signal
        if df['macd'].iloc[-2] < df['macd_signal'].iloc[-2] and df['macd'].iloc[-1] > df['macd_signal'].iloc[-1]:
            signals.append("MACD Bullish Crossover - Consider Buy")
        elif df['macd'].iloc[-2] > df['macd_signal'].iloc[-2] and df['macd'].iloc[-1] < df['macd_signal'].iloc[-1]:
            signals.append("MACD Bearish Crossover - Consider Sell")

        # RSI Signals
        if df['rsi'].iloc[-1] < 30:
            signals.append("RSI Oversold (<30) - Potential Buy")
        elif df['rsi'].iloc[-1] > 70:
            signals.append("RSI Overbought (>70) - Potential Sell")

        # Moving Average Crossover
        if df['sma_20'].iloc[-2] < df['sma_50'].iloc[-2] and df['sma_20'].iloc[-1] > df['sma_50'].iloc[-1]:
            signals.append("Golden Cross (SMA20 crosses above SMA50) - Bullish")
        elif df['sma_20'].iloc[-2] > df['sma_50'].iloc[-2] and df['sma_20'].iloc[-1] < df['sma_50'].iloc[-1]:
            signals.append("Death Cross (SMA20 crosses below SMA50) - Bearish")

        # Bollinger Band Signals
        if df['close'].iloc[-1] < df['bb_lower'].iloc[-1]:
            signals.append("Price below Lower Bollinger Band - Potential Buy")
        elif df['close'].iloc[-1] > df['bb_upper'].iloc[-1]:
            signals.append("Price above Upper Bollinger Band - Potential Sell")

        # Stochastic Oscillator Signals
        if df['slowk'].iloc[-2] < df['slowd'].iloc[-2] and df['slowk'].iloc[-1] > df['slowd'].iloc[-1] and \
                df['slowk'].iloc[-1] < 20:
            signals.append("Stochastic Bullish Crossover in Oversold Region - Consider Buy")
        elif df['slowk'].iloc[-2] > df['slowd'].iloc[-2] and df['slowk'].iloc[-1] < df['slowd'].iloc[-1] and \
                df['slowk'].iloc[-1] > 80:
            signals.append("Stochastic Bearish Crossover in Overbought Region - Consider Sell")

        return signals


    # Display content based on selected menu
    if selected_menu == "Dashboard":
        st.title("Trading Dashboard")

        # Layout with columns
        col1, col2 = st.columns([1, 1])

        with col1:
            # Account summary
            st.subheader("Account Summary")
            try:
                user_profile = user_api.get_profile()
                if user_profile.status == "success" and user_profile.data:
                    user_data = user_profile.data
                    st.write(f"Name: {user_data.name}")
                    st.write(f"Email: {user_data.email}")
                    st.write(f"User ID: {user_data.user_id}")
            except ApiException as e:
                st.error(f"Error fetching profile: {str(e)}")

        with col2:
            # Market overview
            st.subheader("Market Overview")
            market_indices = ["NSE:NIFTY50", "BSE:SENSEX", "NSE:BANKNIFTY"]
            try:
                market_data = market_data_api.get_market_quote_ohlc(market_indices)
                if market_data.status == "success" and market_data.data:
                    indices_data = market_data.data
                    for idx, data in enumerate(indices_data):
                        if hasattr(data, 'ohlc'):
                            st.metric(
                                label=market_indices[idx],
                                value=f"₹{data.ohlc.last_price:.2f}",
                                delta=f"{data.ohlc.change_percent:.2f}%"
                            )
            except ApiException as e:
                st.error(f"Error fetching market data: {str(e)}")

        # Recent orders
        st.subheader("Recent Orders")
        order_df = fetch_order_history()
        if not order_df.empty:
            st.dataframe(order_df.head(5))
        else:
            st.info("No recent orders found.")

        # Current positions
        st.subheader("Current Positions")
        positions_df = fetch_positions()
        if not positions_df.empty:
            st.dataframe(positions_df)
        else:
            st.info("No open positions found.")

    elif selected_menu == "Market Watch":
        st.title("Market Watch")

        # Search and add symbols to watchlist
        col1, col2 = st.columns([3, 1])

        with col1:
            search_query = st.text_input("Search for symbols")
            if search_query:
                search_results = search_instruments(search_query)
                if search_results:
                    symbols_to_add = st.multiselect(
                        "Select symbols to add",
                        options=[f"{result.exchange}:{result.symbol}" for result in search_results]
                    )

                    if st.button("Add to Watchlist"):
                        for symbol in symbols_to_add:
                            if symbol not in st.session_state.watchlist:
                                st.session_state.watchlist.append(symbol)
                        st.success("Added to watchlist!")
                        st.experimental_rerun()

        with col2:
            st.subheader("Watchlist")
            for symbol in st.session_state.watchlist:
                if st.button(f"❌ {symbol}", key=f"remove_{symbol}"):
                    st.session_state.watchlist.remove(symbol)
                    st.success(f"Removed {symbol} from watchlist")
                    st.experimental_rerun()

        # Display live data for watchlist
        if st.session_state.watchlist:
            st.subheader("Live Market Data")

            try:
                market_quotes = market_data_api.get_market_quote_ohlc(st.session_state.watchlist)
                if market_quotes.status == "success" and market_quotes.data:
                    quotes_data = market_quotes.data

                    # Create a DataFrame for better display
                    quotes_list = []
                    for idx, quote in enumerate(quotes_data):
                        if hasattr(quote, 'ohlc'):
                            quotes_list.append({
                                "Symbol": st.session_state.watchlist[idx],
                                "LTP": quote.ohlc.last_price,
                                "Change %": quote.ohlc.change_percent,
                                "High": quote.ohlc.high,
                                "Low": quote.ohlc.low,
                                "Volume": quote.ohlc.volume,
                                "Open": quote.ohlc.open,
                                "Close": quote.ohlc.close
                            })

                    quotes_df = pd.DataFrame(quotes_list)
                    st.dataframe(quotes_df, use_container_width=True)

                    # Add buttons to view charts or place orders for each symbol
                    st.subheader("Actions")
                    for symbol in st.session_state.watchlist:
                        col1, col2 = st.columns([1, 1])
                        with col1:
                            if st.button(f"📈 View Chart for {symbol}", key=f"chart_{symbol}"):
                                st.session_state.selected_symbol = symbol
                                st.experimental_rerun()
                        with col2:
                            if st.button(f"📝 Place Order for {symbol}", key=f"order_{symbol}"):
                                st.session_state.selected_symbol = symbol
                                st.experimental_rerun()
            except ApiException as e:
                st.error(f"Error fetching market quotes: {str(e)}")

    elif selected_menu == "Order Placement":
        st.title("Order Placement")

        if st.session_state.selected_symbol:
            st.info(f"Selected Symbol: {st.session_state.selected_symbol}")
            symbol = st.session_state.selected_symbol
        else:
            symbol = st.selectbox("Select Symbol",
                                  options=st.session_state.watchlist if st.session_state.watchlist else [""])

        if symbol:
            try:
                # Fetch current market data for the symbol
                market_data = market_data_api.get_market_quote_ohlc([symbol])
                if market_data.status == "success" and market_data.data:
                    quote = market_data.data[0]
                    if hasattr(quote, 'ohlc'):
                        current_price = quote.ohlc.last_price
                        st.write(f"Current Price: ₹{current_price:.2f}")

                        # Order form
                        col1, col2 = st.columns(2)

                        with col1:
                            transaction_type = st.radio("Transaction Type", ["BUY", "SELL"])
                            order_type = st.selectbox("Order Type", ["MARKET", "LIMIT", "SL", "SL-M"])
                            product_type = st.selectbox("Product Type", ["DELIVERY", "INTRADAY", "COVER", "BRACKET"])

                        with col2:
                            quantity = st.number_input("Quantity", min_value=1, step=1)
                            price = st.number_input("Price (for LIMIT orders)", value=float(current_price), step=0.05)
                            trigger_price = st.number_input("Trigger Price (for SL/SL-M orders)",
                                                            value=float(current_price) * 0.98, step=0.05)

                        # Advanced order options
                        with st.expander("Advanced Options"):
                            validity = st.selectbox("Validity", ["DAY", "IOC", "GTC"])
                            disclosed_qty = st.number_input("Disclosed Quantity", min_value=0, step=1)
                            is_amo = st.checkbox("After Market Order")

                            if product_type == "BRACKET":
                                stoploss = st.number_input("Stoploss", min_value=0.1, step=0.1, value=1.0)
                                target = st.number_input("Target", min_value=0.1, step=0.1, value=1.0)
                                trailing_sl = st.number_input("Trailing Stoploss", min_value=0.0, step=0.1, value=0.0)

                        # Order preview
                        st.subheader("Order Preview")
                        order_details = {
                            "Symbol": symbol,
                            "Transaction Type": transaction_type,
                            "Order Type": order_type,
                            "Product Type": product_type,
                            "Quantity": quantity,
                            "Price": price if order_type in ["LIMIT", "SL"] else "Market Price",
                            "Trigger Price": trigger_price if order_type in ["SL", "SL-M"] else "N/A"
                        }

                        st.dataframe(pd.DataFrame([order_details]), use_container_width=True)

                        # Place order button
                        if st.button("Place Order", key="place_order_btn"):
                            with st.spinner("Placing order..."):
                                response = place_order(
                                    symbol=symbol,
                                    quantity=quantity,
                                    price=price if order_type in ["LIMIT", "SL"] else 0,
                                    order_type=order_type,
                                    transaction_type=transaction_type,
                                    product_type=product_type
                                )

                                if response and response.status == "success":
                                    st.success(f"Order placed successfully! Order ID: {response.data.order_id}")
                                    # Refresh order history
                                    st.session_state.order_history = fetch_order_history()
                                else:
                                    st.error("Failed to place order. Please check your inputs and try again.")
            except ApiException as e:
                st.error(f"Error fetching market data: {str(e)}")
        else:
            st.warning("Please select a symbol from the watchlist or search for a symbol.")

    elif selected_menu == "Order Book":
        st.title("Order Book")

        if st.button("Refresh Order Book"):
            st.session_state.order_history = fetch_order_history()

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
            date_range = st.date_input(
                "Date Range",
                value=(
                    datetime.datetime.now() - datetime.timedelta(days=7),
                    datetime.datetime.now()
                )
            )

        # Display order history with filters
        if hasattr(st.session_state, 'order_history') and not st.session_state.order_history.empty:
            df = st.session_state.order_history.copy()

            # Apply filters
            if status_filter:
                df = df[df['status'].isin(status_filter)]
            if transaction_filter:
                df = df[df['transaction_type'].isin(transaction_filter)]
            if len(date_range) == 2:
                df['order_date'] = pd.to_datetime(df['order_timestamp']).dt.date
                df = df[(df['order_date'] >= date_range[0]) & (df['order_date'] <= date_range[1])]

            # Display data
            st.dataframe(df, use_container_width=True)

            # Summary statistics
            st.subheader("Order Summary")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Orders", len(df))
            col2.metric("Completed Orders", len(df[df['status'] == 'COMPLETED']))
            col3.metric("Rejected Orders", len(df[df['status'] == 'REJECTED']))
            col4.metric("Pending Orders", len(df[df['status'] == 'PENDING']))

            # Action buttons for orders
            if 'PENDING' in df['status'].values:
                st.subheader("Pending Orders Actions")
                for idx, row in df[df['status'] == 'PENDING'].iterrows():
                    col1, col2, col3 = st.columns([2, 1, 1])
                    col1.write(f"{row['symbol']} - {row['quantity']} @ {row['price']}")
                    if col2.button("Modify", key=f"modify_{row['order_id']}"):
                        try:
                            # Show modify form
                            st.session_state.modify_order_id = row['order_id']
                            st.experimental_rerun()
                        except Exception as e:
                            st.error(f"Error modifying order: {str(e)}")

                    if col3.button("Cancel", key=f"cancel_{row['order_id']}"):
                        try:
                            cancel_response = order_api.cancel_order(row['order_id'])
                            if cancel_response.status == "success":
                                st.success(f"Order {row['order_id']} cancelled successfully!")
                                # Refresh order history
                                st.session_state.order_history = fetch_order_history()
                                st.experimental_rerun()
                            else:
                                st.error("Failed to cancel order.")
                        except ApiException as e:
                            st.error(f"Error cancelling order: {str(e)}")
        else:
            # Fetch order history if not already fetched
            order_df = fetch_order_history()
            if not order_df.empty:
                st.session_state.order_history = order_df
                st.experimental_rerun()
            else:
                st.info("No orders found.")

    elif selected_menu == "Positions":
        st.title("Positions")

        if st.button("Refresh Positions"):
            st.session_state.positions = fetch_positions()

        # Display positions
        positions_df = fetch_positions() if not hasattr(st.session_state,
                                                        'positions') or st.session_state.positions.empty else st.session_state.positions

        if not positions_df.empty:
            st.session_state.positions = positions_df

            # Summary metrics
            total_investment = positions_df[
                'investment_amount'].sum() if 'investment_amount' in positions_df.columns else 0
            total_pnl = positions_df['pnl'].sum() if 'pnl' in positions_df.columns else 0
            total_value = positions_df['current_value'].sum() if 'current_value' in positions_df.columns else 0

            col1, col2, col3 = st.columns(3)
            col1.metric("Total Investment", f"₹{total_investment:.2f}")
            col2.metric("Total P&L", f"₹{total_pnl:.2f}",
                        f"{(total_pnl / total_investment * 100 if total_investment else 0):.2f}%")
            col3.metric("Current Value", f"₹{total_value:.2f}")

            # Display positions table
            st.dataframe(positions_df, use_container_width=True)

            # Square off buttons
            st.subheader("Position Actions")
            for idx, row in positions_df.iterrows():
                col1, col2 = st.columns([3, 1])
                col1.write(f"{row['symbol']} - {row['quantity']} @ {row['average_price']} ({row['product']})")
                if col2.button("Square Off", key=f"squareoff_{idx}"):
                    try:
                        # Create a square off order
                        order_response = place_order(
                            symbol=row['symbol'],
                            quantity=abs(row['quantity']),
                            price=0,  # Market order
                            order_type="MARKET",
                            transaction_type="SELL" if row['quantity'] > 0 else "BUY",
                            product_type=row['product']
                        )

                        if order_response and order_response.status == "success":
                            st.success(f"Square off order placed! Order ID: {order_response.data.order_id}")
                            # Refresh positions
                            st.session_state.positions = fetch_positions()
                            st.experimental_rerun()
                        else:
                            st.error("Failed to place square off order.")
                    except Exception as e:
                        st.error(f"Error placing square off order: {str(e)}")
        else:
            st.info("No open positions found.")

    elif selected_menu == "Holdings":
        st.title("Holdings")

        if st.button("Refresh Holdings"):
            st.session_state.holdings = fetch_holdings()

        # Display holdings
        holdings_df = fetch_holdings() if not hasattr(st.session_state,
                                                      'holdings') or st.session_state.holdings.empty else st.session_state.holdings

        if not holdings_df.empty:
            st.session_state.holdings = holdings_df

            # Summary metrics
            total_investment = holdings_df[
                'investment_amount'].sum() if 'investment_amount' in holdings_df.columns else 0
            total_pnl = holdings_df['pnl'].sum() if 'pnl' in holdings_df.columns else 0
            total_value = holdings_df['current_value'].sum() if 'current_value' in holdings_df.columns else 0

            col1, col2, col3 = st.columns(3)
            col1.metric("Total Investment", f"₹{total_investment:.2f}")