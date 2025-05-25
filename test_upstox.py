import os
from upstox_client.api import (
    LoginApi,
    OrderApi,
    PortfolioApi,
    MarketQuoteApi,
    WebsocketApi
)
from upstox_client.rest import ApiException
from upstox_client import Configuration, ApiClient
import json
import time


class UpstoxTradingFramework:
    def __init__(self, api_key, access_token):
        """
        Initialize the Upstox trading framework

        :param api_key: Your Upstox API key
        :param access_token: Access token obtained after OAuth
        """
        self.api_key = api_key
        self.access_token = access_token

        # Configure API client
        self.config = Configuration()
        self.config.access_token = access_token

        # Initialize API clients
        self.api_client = ApiClient(self.config)
        self.login_api = LoginApi(self.api_client)
        self.order_api = OrderApi(self.api_client)
        self.portfolio_api = PortfolioApi(self.api_client)
        self.market_quote_api = MarketQuoteApi(self.api_client)

        # WebSocket connection (optional)
        self.ws_connected = False
        self.ws_client = None

    def get_profile(self):
        """Get user profile details"""
        try:
            profile = self.login_api.get_profile()
            return profile
        except ApiException as e:
            print(f"Exception when calling LoginApi->get_profile: {e}")
            return None

    def place_order(self, order_params):
        """
        Place an order

        :param order_params: Dictionary containing order parameters
            Example:
            {
                "quantity": 1,
                "product": "D",
                "validity": "DAY",
                "price": 0,
                "tag": "string",
                "instrument_token": "NSE_EQ|INE669E01016",
                "order_type": "MARKET",
                "transaction_type": "BUY",
                "disclosed_quantity": 0,
                "trigger_price": 0,
                "is_amo": False
            }
        """
        try:
            order_response = self.order_api.place_order(
                order_params['quantity'],
                order_params['product'],
                order_params['validity'],
                order_params['price'],
                order_params['tag'],
                order_params['instrument_token'],
                order_params['order_type'],
                order_params['transaction_type'],
                order_params['disclosed_quantity'],
                order_params['trigger_price'],
                order_params['is_amo']
            )
            return order_response
        except ApiException as e:
            print(f"Exception when calling OrderApi->place_order: {e}")
            return None

    def get_order_book(self):
        """Get list of all orders"""
        try:
            order_book = self.order_api.get_order_book()
            return order_book
        except ApiException as e:
            print(f"Exception when calling OrderApi->get_order_book: {e}")
            return None

    def get_order_details(self, order_id):
        """Get details of a specific order"""
        try:
            order_details = self.order_api.get_order_details(order_id)
            return order_details
        except ApiException as e:
            print(f"Exception when calling OrderApi->get_order_details: {e}")
            return None

    def get_positions(self):
        """Get current day's positions"""
        try:
            positions = self.portfolio_api.get_positions().data
            return positions
        except ApiException as e:
            print(f"Exception when calling PortfolioApi->get_positions: {e}")
            return None

    def get_holdings(self):
        """Get user's holdings"""
        try:
            holdings = self.portfolio_api.get_holdings().data
            return holdings
        except ApiException as e:
            print(f"Exception when calling HoldingsApi->get_holdings: {e}")
            return None

    def get_ltp(self, instrument_key):
        """
        Get last traded price for an instrument

        :param instrument_key: Instrument key (e.g., "NSE_EQ|INE669E01016")
        """
        try:
            ltp_data = self.market_quote_api.get_full_market_quote(instrument_key)
            return ltp_data
        except ApiException as e:
            print(f"Exception when calling MarketQuoteApi->get_full_market_quote: {e}")
            return None

    def connect_websocket(self, on_message_callback):
        """
        Connect to Upstox WebSocket for live market data

        :param on_message_callback: Function to handle incoming messages
        """
        try:
            self.ws_client = WebsocketApi(self.api_client)
            self.ws_client.connect(on_message=on_message_callback)
            self.ws_connected = True
            print("WebSocket connected successfully")
        except Exception as e:
            print(f"Exception when connecting to WebSocket: {e}")

    def subscribe_websocket(self, instrument_keys):
        """
        Subscribe to market data for specific instruments

        :param instrument_keys: List of instrument keys to subscribe to
        """
        if not self.ws_connected:
            print("WebSocket not connected")
            return False

        try:
            self.ws_client.subscribe(instrument_keys)
            return True
        except Exception as e:
            print(f"Exception when subscribing to WebSocket: {e}")
            return False

    def disconnect_websocket(self):
        """Disconnect from WebSocket"""
        if self.ws_connected and self.ws_client:
            try:
                self.ws_client.disconnect()
                self.ws_connected = False
                print("WebSocket disconnected successfully")
            except Exception as e:
                print(f"Exception when disconnecting WebSocket: {e}")

    def __del__(self):
        """Clean up on object deletion"""
        self.disconnect_websocket()


# Example usage of the framework
if __name__ == "__main__":
    # Replace these with your actual credentials
    API_KEY = "your_api_key"
    ACCESS_TOKEN = "your_access_token"

    # Initialize framework
    trader = UpstoxTradingFramework(api_key=API_KEY, access_token=ACCESS_TOKEN)

    # Get profile
    profile = trader.get_profile()
    print("User Profile:", profile)

    # Example order parameters (modify as needed)
    order_params = {
        "quantity": 1,
        "product": "D",  # Delivery
        "validity": "DAY",
        "price": 0,  # For market orders
        "tag": "test_order",
        "instrument_token": "NSE_EQ|INE669E01016",  # Example: Reliance
        "order_type": "MARKET",
        "transaction_type": "BUY",
        "disclosed_quantity": 0,
        "trigger_price": 0,
        "is_amo": False
    }


    # Place order
    # order_response = trader.place_order(order_params)
    # print("Order Response:", order_response)

    # Get order book
    # orders = trader.get_order_book()
    # print("Order Book:", orders)

    # Get positions
    # positions = trader.get_positions()
    # print("Positions:", positions)

    # WebSocket example
    def on_message(message):
        print("Received message:", message)


    # Connect to WebSocket
    trader.connect_websocket(on_message_callback=on_message)

    # Subscribe to instruments (example)
    # trader.subscribe_websocket(["NSE_EQ|INE669E01016"])

    # Keep the connection alive for a while
    # time.sleep(30)

    # Disconnect WebSocket
    trader.disconnect_websocket()