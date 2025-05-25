import os
import json
from pprint import pprint
from kiteconnect import KiteConnect
from kiteconnect.exceptions import KiteException
import logging
import datetime # For historical data examples
from dotenv import load_dotenv
load_dotenv()

# Optional: Configure logging to see detailed KiteConnect client activity
# logging.basicConfig(level=logging.DEBUG)

# --- 1. Configuration and Authentication ---
try:
    API_KEY = os.getenv("ZERODHA_API_KEY")
    API_SECRET = os.getenv("ZERODHA_API_SECRET")
    if not API_KEY or not API_SECRET:
        raise ValueError("KITE_API_KEY or KITE_API_SECRET not set as environment variables.")
except ValueError as e:
    print(f"Configuration Error: {e}")
    exit()

kite = KiteConnect(api_key=API_KEY)
ACCESS_TOKEN = os.getenv("ZERODHA_ACCESS_TOKEN")


def authenticate_and_initialize():
    global kite
    access_token = ACCESS_TOKEN
    kite.set_access_token(access_token)
    try:
        profile = kite.profile() # Verify token
        print(f"Successfully initialized. User: {profile.get('user_shortname', 'N/A')}")
        return True
    except KiteException as e:
        print(f"Error verifying access token (it might be expired or invalid): {e.message}")
        print("Please try deleting access_token.txt and re-authenticating.")
        return False

# --- 2. Dynamic API Caller ---
def call_api(method_name, *args, **kwargs):
    global kite
    print(f"\n--- Calling API: {method_name} ---")
    if args:
        print(f"Args: {args}")
    if kwargs:
        print(f"Payload (kwargs):")
        pprint(kwargs)

    try:
        api_method = getattr(kite, method_name)
        response = api_method(*args, **kwargs)
        print("\n--- API Response (Success) ---")
        pprint(response)
        return response
    except KiteException as e:
        print("\n--- API Error ---")
        print(f"Error Message: {e.message}")
        print(f"Error Code: {e.code}")
        print(f"Raw Error: {e}") # To see if there's more detail in the exception object
        return None
    except Exception as e:
        print("\n--- Unexpected Script Error ---")
        print(e)
        return None

# --- 3. API Exploration Sections ---

# == Section A: User Profile and Funds ==
def explore_user_apis():
    call_api("profile")
    call_api("margins")
    # call_api("margins", segment="equity") # Example with arg
    # call_api("margins", segment="commodity")

# == Section B: Portfolio Information ==
def explore_portfolio_apis():
    call_api("holdings")
    call_api("positions")
    # call_api("instruments") # This can be a large response
    # call_api("instruments", exchange=kite.EXCHANGE_NSE)
    # call_api("instruments", exchange=kite.EXCHANGE_NFO)

# == Section C: Order Management (USE AMO for safety) ==
# IMPORTANT: Modify payloads carefully. Always double-check parameters.
# Refer to: https://kite.trade/docs/connect/v3/orders/

TEST_SYMBOL = "INFY" # A relatively liquid stock
TEST_EXCHANGE = kite.EXCHANGE_NSE
TEST_QUANTITY = 1

def explore_place_order_api():
    print("\n" + "="*10 + " EXPLORING place_order " + "="*10)
    print("WARNING: Ensure you understand the order parameters. Using AMO for safety.")

    # --- Payload 1: AMO LIMIT BUY (CNC) ---
    payload_limit_buy_amo = {
        "tradingsymbol": TEST_SYMBOL,
        "exchange": TEST_EXCHANGE,
        "transaction_type": kite.TRANSACTION_TYPE_BUY,
        "quantity": TEST_QUANTITY,
        "product": kite.PRODUCT_CNC,
        "order_type": kite.ORDER_TYPE_LIMIT,
        "price": 1400.00, # Set a price far from LTP to avoid execution
        "variety": kite.VARIETY_AMO,
        "tag": "MyTestAMO_LimitBuy"
    }
    # call_api("place_order", **payload_limit_buy_amo)

    # --- Payload 2: AMO MARKET SELL (MIS) ---
    payload_market_sell_amo = {
        "tradingsymbol": TEST_SYMBOL,
        "exchange": TEST_EXCHANGE,
        "transaction_type": kite.TRANSACTION_TYPE_SELL,
        "quantity": TEST_QUANTITY,
        "product": kite.PRODUCT_MIS,
        "order_type": kite.ORDER_TYPE_MARKET,
        "variety": kite.VARIETY_AMO,
        "tag": "MyTestAMO_MarketSell"
    }
    # call_api("place_order", **payload_market_sell_amo)

    # --- Payload 3: AMO SL-LIMIT BUY (MIS) ---
    # For SL orders, trigger_price < price for BUY SL, trigger_price > price for SELL SL
    payload_sl_limit_buy_amo = {
        "tradingsymbol": TEST_SYMBOL,
        "exchange": TEST_EXCHANGE,
        "transaction_type": kite.TRANSACTION_TYPE_BUY,
        "quantity": TEST_QUANTITY,
        "product": kite.PRODUCT_MIS,
        "order_type": kite.ORDER_TYPE_SL, # SL-LIMIT
        "price": 1005.0, # Limit price
        "trigger_price": 1000.0, # Trigger price
        "variety": kite.VARIETY_AMO,
    }
    # call_api("place_order", **payload_sl_limit_buy_amo)

    # --- Payload 4: Invalid quantity (e.g., 0 or negative) ---
    payload_invalid_quantity = {
        "tradingsymbol": TEST_SYMBOL,
        "exchange": TEST_EXCHANGE,
        "transaction_type": kite.TRANSACTION_TYPE_BUY,
        "quantity": 0, # Invalid
        "product": kite.PRODUCT_CNC,
        "order_type": kite.ORDER_TYPE_MARKET,
        "variety": kite.VARIETY_AMO,
    }
    # call_api("place_order", **payload_invalid_quantity) # Expect an error

    # --- Payload 5: Cover Order (CO) - Market BUY with SL (Requires live market or careful AMO) ---
    # CO has specific rules: MIS product, usually MARKET or LIMIT for first leg.
    # The second leg (SL) price is a trigger_price.
    # Squareoff and stoploss are offsets/absolute values depending on API version/docs
    # Note: Cover Orders might have different behavior/availability. Check current docs.
    # payload_co_buy_amo = {
    #     "tradingsymbol": TEST_SYMBOL,
    #     "exchange": TEST_EXCHANGE,
    #     "transaction_type": kite.TRANSACTION_TYPE_BUY,
    #     "quantity": TEST_QUANTITY,
    #     "product": kite.PRODUCT_MIS, # CO is usually MIS
    #     "order_type": kite.ORDER_TYPE_MARKET, # Main leg
    #     "variety": kite.VARIETY_CO,
    #     "trigger_price": 990.0 # This is the SL trigger for the second leg
    # }
    # call_api("place_order", **payload_co_buy_amo)

    print("Uncomment specific payloads above to test them one by one.")

def explore_order_info_apis():
    print("\n" + "="*10 + " EXPLORING Order Info APIs " + "="*10)
    # --- Get all orders for the day ---
    # call_api("orders")

    # --- Get history for a specific order_id ---
    # Replace with an actual order_id obtained from place_order or your Zerodha terminal
    test_order_id = "250511500003579"
    if test_order_id != "YOUR_ORDER_ID_HERE":
         call_api("order_history", order_id=test_order_id)
    else:
         print("Please set a valid test_order_id to explore order_history.")

    # --- Get trades for a specific order_id (if it executed) ---
    if test_order_id != "YOUR_ORDER_ID_HERE":
         call_api("order_trades", order_id=test_order_id)
    else:
         print("Please set a valid test_order_id to explore order_trades.")

    # --- Get all trades for the day ---
    # call_api("trades")


def explore_modify_cancel_order_api():
    print("\n" + "="*10 + " EXPLORING Modify/Cancel Order APIs " + "="*10)
    print("WARNING: Use order_ids from AMO orders placed for testing.")

    # You need a valid, open AMO order_id for these.
    # First, place an AMO LIMIT order, get its ID, then try to modify/cancel.
    # Example:
    # 1. Place AMO Limit Buy:
    # payload = { "tradingsymbol": TEST_SYMBOL, "exchange": TEST_EXCHANGE, ... "variety": kite.VARIETY_AMO, "price": 1000.0 }
    # response = call_api("place_order", **payload)
    # if response and response.get("data", {}).get("order_id"):
    #     amo_order_id = response["data"]["order_id"]
    #     print(f"Placed AMO order for modification/cancellation test: {amo_order_id}")

    #     # 2. Modify the AMO order (e.g., change price)
    #     modify_payload = {
    #         "variety": kite.VARIETY_AMO,
    #         "order_id": amo_order_id,
    #         "price": 995.0, # New price
    #         # "quantity": TEST_QUANTITY, # If modifying quantity
    #         # "order_type": kite.ORDER_TYPE_LIMIT, # If changing order_type (be careful)
    #         # "trigger_price": new_trigger_price # If modifying an SL order
    #     }
    #     call_api("modify_order", **modify_payload)

    # 3. Cancel the AMO order
    cancel_payload = {
        "variety": kite.VARIETY_AMO,
        "order_id": '250511500003579',
    }
    call_api("cancel_order", **cancel_payload) # Uncomment to test cancellation
    # else:
    #     print("Could not place an AMO order to test modify/cancel.")
    print("Uncomment and structure the place/modify/cancel flow carefully for testing.")


# == Section D: GTT (Good Till Triggered) Orders ==
# Refer to: https://kite.trade/docs/connect/v3/gtt/
def explore_gtt_apis():
    print("\n" + "="*10 + " EXPLORING GTT APIs " + "="*10)
    # --- Payload for Single Leg GTT (e.g., Buy CNC at a trigger price) ---
    # Condition: Price of INFY <= 950
    # Order: Buy 1 INFY CNC at Limit 950.0
    gtt_single_leg_payload = {
        "trigger_type": kite.GTT_TYPE_SINGLE,
        "tradingsymbol": TEST_SYMBOL,
        "exchange": TEST_EXCHANGE,
        "trigger_values": [950.0], # Trigger when LTP hits or goes below this
        "last_price": 1050.0, # Current LTP (must be realistic relative to trigger)
        "orders": [{
            "exchange": TEST_EXCHANGE,
            "tradingsymbol": TEST_SYMBOL,
            "transaction_type": kite.TRANSACTION_TYPE_BUY,
            "quantity": TEST_QUANTITY,
            "order_type": kite.ORDER_TYPE_LIMIT,
            "product": kite.PRODUCT_CNC,
            "price": 950.0
        }]
    }
    # response = call_api("place_gtt", **gtt_single_leg_payload)
    # if response and response.get("data", {}).get("trigger_id"):
    #     gtt_trigger_id = response["data"]["trigger_id"]
    #     print(f"Placed GTT. Trigger ID: {gtt_trigger_id}")
        # call_api("get_gtt", trigger_id=gtt_trigger_id) # Get specific GTT
        # call_api("modify_gtt", trigger_id=gtt_trigger_id, **new_gtt_payload) # Define new_gtt_payload
        # call_api("delete_gtt", trigger_id=gtt_trigger_id)

    # --- Payload for OCO (One Cancels Other) GTT ---
    # Example: Target and Stoploss for an existing CNC holding
    # Condition: If INFY >= 1100 (target) OR INFY <= 900 (stoploss)
    # Order1 (Target): Sell 1 INFY CNC Limit at 1100
    # Order2 (Stoploss): Sell 1 INFY CNC Limit at 900 (or SL with trigger)
    gtt_oco_payload = {
        "trigger_type": kite.GTT_TYPE_OCO,
        "tradingsymbol": TEST_SYMBOL,
        "exchange": TEST_EXCHANGE,
        "trigger_values": [900.0, 1100.0], # [stoploss_trigger, target_trigger]
        "last_price": 1050.0, # Current LTP
        "orders": [
            { # Stoploss order
                "exchange": TEST_EXCHANGE,
                "tradingsymbol": TEST_SYMBOL,
                "transaction_type": kite.TRANSACTION_TYPE_SELL,
                "quantity": TEST_QUANTITY,
                "order_type": kite.ORDER_TYPE_LIMIT, # Could be SL too
                "product": kite.PRODUCT_CNC,
                "price": 900.0
            },
            { # Target order
                "exchange": TEST_EXCHANGE,
                "tradingsymbol": TEST_SYMBOL,
                "transaction_type": kite.TRANSACTION_TYPE_SELL,
                "quantity": TEST_QUANTITY,
                "order_type": kite.ORDER_TYPE_LIMIT,
                "product": kite.PRODUCT_CNC,
                "price": 1100.0
            }
        ]
    }
    # call_api("place_gtt", **gtt_oco_payload)
    # call_api("get_gtts") # Get all GTTs
    print("Uncomment specific GTT payloads/calls to test.")


# == Section E: Market Data ==
def explore_market_data_apis():
    print("\n" + "="*10 + " EXPLORING Market Data APIs " + "="*10)
    instruments_to_quote = [f"{TEST_EXCHANGE}:{TEST_SYMBOL}", f"{kite.EXCHANGE_NSE}:RELIANCE"]
    single_instrument = f"{TEST_EXCHANGE}:{TEST_SYMBOL}"

    call_api("/quote/ltp", instrument=instruments_to_quote)
    # call_api("quote", instruments=instruments_to_quote)
    # call_api("ohlc", instruments=instruments_to_quote)

    # --- Historical Data ---
    # You need instrument_token for historical data.
    # 1. Get instrument_token (do this once, store it)
    # inst_list = call_api("instruments", exchange=TEST_EXCHANGE)
    # instrument_token = None
    # if inst_list:
    #     for inst in inst_list:
    #         if inst["tradingsymbol"] == TEST_SYMBOL:
    #             instrument_token = inst["instrument_token"]
    #             print(f"Instrument token for {TEST_SYMBOL}: {instrument_token}")
    #             break
    instrument_token = 256265 # Example for NIFTY 50, replace with actual token for TEST_SYMBOL

    if instrument_token:
        from_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
        to_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        interval = "day" # other options: "minute", "3minute", "5minute", "10minute", "15minute", "30minute", "60minute"

        historical_payload = {
            "instrument_token": instrument_token,
            "from_date": from_date,
            "to_date": to_date,
            "interval": interval,
            # "continuous": 0, # 0 for false, 1 for true (for futures/options)
            # "oi": 0 # 0 for false, 1 for true (for F&O data)
        }
        # call_api("historical_data", **historical_payload)

        # Example: OI data for F&O
        # fn_instrument_token = 779521 # Example, find a valid F&O token
        # fn_from_date = (datetime.datetime.now() - datetime.timedelta(days=5)).strftime('%Y-%m-%d %H:%M:%S')
        # fn_to_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # fn_interval = "minute"
        # historical_oi_payload = {
        #     "instrument_token": fn_instrument_token,
        #     "from_date": fn_from_date,
        #     "to_date": fn_to_date,
        #     "interval": fn_interval,
        #     "oi": 1
        # }
        # call_api("historical_data", **historical_oi_payload)
    else:
        print("Instrument token not found, skipping historical data test.")
    print("Uncomment market data calls to test.")

# --- Main Execution Logic ---
if __name__ == "__main__":
    if authenticate_and_initialize():
        print("\nKiteConnect API Playground Initialized.")
        print("Edit this script to uncomment and run API exploration sections.")
        print("Always refer to official documentation for parameter details and best practices.")

        # --- CHOOSE WHICH APIs TO EXPLORE ---
        # explore_user_apis()
        # explore_portfolio_apis()
        # explore_place_order_api() # BE CAREFUL, USES AMO
        # explore_order_info_apis()
        explore_modify_cancel_order_api() # Needs an AMO order_id
        # explore_gtt_apis()
        # explore_market_data_apis()

    else:
        print("Failed to initialize. Exiting.")