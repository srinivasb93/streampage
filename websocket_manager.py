from datetime import datetime, time as date_time
import pytz
import upstox_client
import logging
import time
import threading

# Setup logging
logging.basicConfig(filename='STREAM_DATA.log', level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.info("Market Data Manager: Logging initialized")


class WebSocketManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, access_token=None, thread_registry=None, data_source='REST'):
        """
        Singleton pattern to ensure only one instance of WebSocketManager exists.
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(WebSocketManager, cls).__new__(cls)
                    cls._instance._initialize(access_token, thread_registry, data_source)
        return cls._instance

    def _initialize(self, access_token, thread_registry, data_source):
        """Initialize the WebSocketManager with default values and start WebSocket if token provided."""
        self.live_data = {}  # Dict with instrument_token as key
        self.subscribed_instruments = set()
        self.streamer_instance = None
        self._connected = False
        self._thread = None
        self.thread_registry = thread_registry  # Store registry if provided
        self.data_source = data_source  # Store data source
        if access_token and self.data_source != 'REST':
            self.start_websocket(access_token)

    def is_market_open(self):
        ist = pytz.timezone("Asia/Kolkata")
        now = datetime.now(ist)
        market_open = date_time(9, 15)
        market_close = date_time(15, 30)
        return now.weekday() < 5 and market_open <= now.time() <= market_close

    def on_message(self, message):
        """Handle incoming market data messages."""
        try:
            # logger.info(f"Market Data Manager: Raw message received: {message}")
            logger.info("Market Data Manager: Raw message received")
            if "feeds" in message:
                for token, feed in message["feeds"].items():
                    token_data = {
                        "ltp": feed["fullFeed"]["marketFF"]["ltpc"]["ltp"],
                        "depth": feed["fullFeed"]["marketFF"]["marketLevel"]["bidAskQuote"]
                    }
                    self.live_data[token] = token_data
                # logger.info(f"Market Data Manager: Updated live_data: {self.live_data}")
                logger.info(f"Market Data Manager: Updated live_data")
            else:
                logger.warning(f"Market Data Manager: No 'feeds' in message: {message}")
        except Exception as e:
            logger.error(f"Market Data Manager: Error parsing message: {e}")

    def on_error(self, error):
        """Handle WebSocket errors."""
        logger.error(f"Market Data Manager: Error: {error}")

    def on_close(self, close_status_code=None, close_msg=None):
        """Handle WebSocket closure."""
        self._connected = False
        logger.info(f"Market Data Manager: Connection closed - Status: {close_status_code}, Message: {close_msg}")

    def on_open(self):
        """Handle WebSocket opening and auto-subscribe to instruments."""
        self._connected = True
        logger.info("Market Data Manager: WebSocket connection opened")
        if self.subscribed_instruments and self.streamer_instance:
            self.streamer_instance.subscribe(list(self.subscribed_instruments), "full")
            logger.info(f"Market Data Manager: Auto-subscribed to {self.subscribed_instruments}")

    def start_websocket(self, access_token):
        """Start the WebSocket connection."""
        if not self.is_market_open():
            logger.info("Market is closed; skipping WebSocket connection")
            return False
        if self.streamer_instance and self._connected:
            logger.info("Market Data Manager: WebSocket already running")
            return True
        if not access_token:
            logger.error("Market Data Manager: No access token provided")
            return False
        try:
            configuration = upstox_client.Configuration()
            configuration.access_token = access_token
            api_client = upstox_client.ApiClient(configuration)
            self.streamer_instance = upstox_client.MarketDataStreamerV3(api_client)

            # Bind event handlers
            self.streamer_instance.on("open", self.on_open)
            self.streamer_instance.on("message", self.on_message)
            self.streamer_instance.on("error", self.on_error)
            self.streamer_instance.on("close", self.on_close)

            # Enable auto-reconnect
            self.streamer_instance.auto_reconnect(True, 30, 1)

            stop_event = threading.Event()

            def websocket_runner():
                attempt = 0
                while not stop_event.is_set():
                    if not self.is_market_open():
                        logger.info("Market closed during runtime; stopping WebSocket attempts")
                        break
                    attempt += 1
                    logger.info(f"WebSocket connection attempt {attempt}")
                    self.streamer_instance.connect()
                    if attempt > 1:  # After first failure, respect reconnect delay
                        stop_event.wait(30)  # Align with auto_reconnect(True, 30, 1)
                    else:
                        stop_event.wait(1)  # Initial quick retry
                self.streamer_instance.disconnect()
                logger.info("WebSocket thread stopped")

            self._thread = threading.Thread(target=websocket_runner, daemon=True, name="WebSocketThread")
            self._thread.start()
            if self.thread_registry is not None:  # Register thread only if registry provided
                self.thread_registry["WebSocketThread"] = {"thread": self._thread, "stop_event": stop_event}
            logger.info("Market Data Manager: WebSocket connection initiated")
            return True
        except Exception as e:
            logger.error(f"Market Data Manager: Startup error: {e}")
            return False

    def subscribe(self, instrument_token):
        """Subscribe to an instrument's market data."""
        timeout = 5
        start_time = time.time()
        while not self._connected and (time.time() - start_time) < timeout:
            logger.info("Market Data Manager: Waiting for connection before subscribing...")
            time.sleep(0.1)
        if self._connected and self.streamer_instance:
            try:
                self.streamer_instance.subscribe([instrument_token], "full")
                self.subscribed_instruments.add(instrument_token)
                logger.info(f"Market Data Manager: Subscribed to {instrument_token}")
                logger.info(f"Market Data Manager: Current subscriptions: {self.subscribed_instruments}")
                return True
            except Exception as e:
                logger.error(f"Market Data Manager: Subscription error: {e}")
                return False
        else:
            logger.warning(f"Market Data Manager: Could not connect within {timeout}s or streamer not initialized")
            return False

    def close(self):
        """Close the WebSocket connection."""
        if self.streamer_instance and self._connected:
            try:
                self.streamer_instance.disconnect()
                self._connected = False
                self._thread = None
                logger.info("Market Data Manager: WebSocket connection closed by request")
                return True
            except Exception as e:
                logger.error(f"Market Data Manager: Error closing WebSocket: {e}")
                return False
        else:
            logger.warning("Market Data Manager: No active WebSocket connection to close")
            return False

    def get_data(self, instrument_token=None):
        """Return live data for a specific instrument or all data if None."""
        if instrument_token:
            return self.live_data.get(instrument_token, {"ltp": 0, "depth": {}})
        return self.live_data

    def is_connected(self):
        """Check if WebSocket is connected."""
        return self._connected

    def get_subscribed_instruments(self):
        """Return list of subscribed instruments."""
        return list(self.subscribed_instruments)

    def subscribe_to_instrument(self, instrument_token):
        """Subscribe to an instrument."""
        return self.subscribe(instrument_token)

# Global functions for backward compatibility with existing code
def initialize_websocket(access_token):
    """Initialize the WebSocket connection."""
    return WebSocketManager(access_token).start_websocket(access_token)

def subscribe_to_instrument(instrument_token):
    """Subscribe to an instrument."""
    return WebSocketManager().subscribe(instrument_token)

def close_websocket():
    """Close the WebSocket connection."""
    return WebSocketManager().close()

def get_live_data(instrument_token=None):
    """Get live data for an instrument or all data."""
    return WebSocketManager().get_data(instrument_token)

def is_connected():
    """Check connection status."""
    return WebSocketManager().is_connected()

def get_subscribed_instruments():
    """Get list of subscribed instruments."""
    return WebSocketManager().get_subscribed_instruments()

if __name__ == "__main__":
    access_token = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI4QkFUVVkiLCJqdGkiOiI2N2RhMWRjZTNkZGQ3YTZhM2Q0MDY0MGEiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaWF0IjoxNzQyMzQ3NzI2LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NDI0MjE2MDB9.KMhp9ZyWNAPfcVusFZNMDDJ3SFFSWLekuAO4RSG2R_c"  # Replace with your actual token
    initialize_websocket(access_token)
    time.sleep(1)
    subscribe_to_instrument("NSE_EQ|INE669E01016")
    for _ in range(10):
        print(f"Live data: {get_live_data()}")
        time.sleep(1)
    close_websocket()
    time.sleep(2)
    print(f"Connection status after close: {is_connected()}")