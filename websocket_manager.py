# market_data_manager.py
import upstox_client
import logging
import time
import threading

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.info("Market Data Manager: Logging initialized")

# Shared data store
live_data = {}  # Now a dict with instrument_token as key
subscribed_instruments = set()
streamer_instance = None
_connected = False
_thread = None


def on_message(message):
    """Handle incoming market data messages."""
    global live_data
    try:
        logger.info(f"Market Data Manager: Raw message received: {message}")
        if "feeds" in message:
            for token, feed in message["feeds"].items():
                token_data = {"ltp": feed["fullFeed"]["marketFF"]["ltpc"]["ltp"],
                              "depth": feed["fullFeed"]["marketFF"]["marketLevel"]["bidAskQuote"]}
                live_data[token] = token_data
            logger.info(f"Market Data Manager: Updated live_data: {live_data}")
        else:
            logger.warning(f"Market Data Manager: No 'feeds' in message: {message}")
    except Exception as e:
        logger.error(f"Market Data Manager: Error parsing message: {e}")


def on_error(error):
    logger.error(f"Market Data Manager: Error: {error}")


def on_close(close_status_code=None, close_msg=None):
    global _connected
    _connected = False
    logger.info(f"Market Data Manager: Connection closed - Status: {close_status_code}, Message: {close_msg}")


def on_open():
    global _connected
    _connected = True
    logger.info("Market Data Manager: WebSocket connection opened")
    if subscribed_instruments and streamer_instance:
        streamer_instance.subscribe(list(subscribed_instruments), "full")
        logger.info(f"Market Data Manager: Auto-subscribed to {subscribed_instruments}")


def initialize_websocket(access_token):
    global streamer_instance, _thread
    if not access_token:
        logger.error("Market Data Manager: No access token provided")
        return False
    try:
        configuration = upstox_client.Configuration()
        configuration.access_token = access_token
        api_client = upstox_client.ApiClient(configuration)
        streamer_instance = upstox_client.MarketDataStreamerV3(api_client)

        streamer_instance.on("open", on_open)
        streamer_instance.on("message", on_message)
        streamer_instance.on("error", on_error)
        streamer_instance.on("close", on_close)

        streamer_instance.auto_reconnect(True, 10, 3)

        _thread = threading.Thread(target=streamer_instance.connect, daemon=True)
        _thread.start()
        logger.info("Market Data Manager: WebSocket connection initiated")
        return True
    except Exception as e:
        logger.error(f"Market Data Manager: Startup error: {e}")
        return False


def subscribe_to_instrument(instrument_token):
    global streamer_instance, subscribed_instruments
    timeout = 5
    start_time = time.time()
    while not _connected and (time.time() - start_time) < timeout:
        logger.info("Market Data Manager: Waiting for connection before subscribing...")
        time.sleep(0.1)
    if _connected and streamer_instance:
        try:
            streamer_instance.subscribe([instrument_token], "full")
            subscribed_instruments.add(instrument_token)
            logger.info(f"Market Data Manager: Subscribed to {instrument_token}")
            logger.info(f"Market Data Manager: Current subscriptions: {subscribed_instruments}")
            return True
        except Exception as e:
            logger.error(f"Market Data Manager: Subscription error: {e}")
            return False
    else:
        logger.warning(f"Market Data Manager: Could not connect within {timeout}s or streamer not initialized")
        return False


def close_websocket():
    global streamer_instance, _connected, _thread
    if streamer_instance and _connected:
        try:
            streamer_instance.disconnect()
            _connected = False
            _thread = None
            logger.info("Market Data Manager: WebSocket connection closed by request")
            return True
        except Exception as e:
            logger.error(f"Market Data Manager: Error closing WebSocket: {e}")
            return False
    else:
        logger.warning("Market Data Manager: No active WebSocket connection to close")
        return False


def get_live_data(instrument_token=None):
    """Return live data for a specific instrument or all data if None."""
    if instrument_token:
        return live_data.get(instrument_token, {"ltp": 0, "depth": {}})
    return live_data


def is_connected():
    return _connected


def get_subscribed_instruments():
    return list(subscribed_instruments)


if __name__ == "__main__":
    access_token = "your_access_token_here"  # Replace with your actual token
    initialize_websocket(access_token)
    time.sleep(1)
    subscribe_to_instrument("NSE_EQ|INE669E01016")
    for _ in range(10):
        print(f"Live data: {get_live_data()}")
        time.sleep(1)
    close_websocket()
    time.sleep(2)
    print(f"Connection status after close: {is_connected()}")