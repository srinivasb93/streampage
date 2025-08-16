import pandas as pd
from upstox_client.rest import ApiException
from datetime import datetime, timedelta
import logging
import requests
import os
# from dotenv import load_dotenv

# load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True)
logger = logging.getLogger(__name__)

DATABASE = 'nsedata'


def get_historical_data(instrument_token, interval="days", unit="1", sort_data=True, start_date=None, end_date=None):
    try:
        if not start_date and not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=3650)).strftime("%Y-%m-%d")
        headers = {"Authorization": f"Bearer {os.getenv('UPSTOX_ACCESS_TOKEN')}"}
        url = f"https://api.upstox.com/v3/historical-candle/{instrument_token}/{interval}/{unit}/{end_date}/{start_date}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json().get("data", []).get("candles", [])
            required_cols = ["timestamp", "open", "high", "low", "close", "volume"]
            df = pd.DataFrame(data, columns=required_cols + ["oi"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df[required_cols]
            if sort_data:
                df.sort_values(by="timestamp", inplace=True)
            return df
        else:
            logger.error(f"Failed to fetch historical data: {response.text}")
            return None
    except Exception as e:
        logger.error(f"Error fetching historical data: {e}")
        return None


def get_live_data(instrument_token=None):
    try:
        headers = {"Authorization": f"Bearer {os.getenv('UPSTOX_ACCESS_TOKEN')}"}
        url = f"https://api.upstox.com/v3/intra-day-candle-data/{instrument_token}/1minute"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json().get("data", [])
            if data:
                latest = data[-1]
                return {"ltp": latest["close"], "depth": None}
        logger.error(f"Failed to fetch live data: {response.text}")
        return {"ltp": 0, "depth": None}
    except Exception as e:
        logger.error(f"Error fetching live data: {e}")
        return {"ltp": 0, "depth": None}


def get_user_profile_and_funds(api, api_version='2.0', user_profile=False):
    try:
        if user_profile:
            response = api.get_profile(api_version)
        else:
            response = api.get_user_fund_margin(api_version)
        return response.to_dict()
    except ApiException as e:
        logger.error(f"Exception when calling UserApi->get_funds: {e}")
        return None


def fetch_instruments():
    try:
        path = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
        instruments_df = pd.read_json(path)
        instruments_df = instruments_df[['trading_symbol', 'instrument_key']][(instruments_df['segment'] == 'NSE_EQ') & (instruments_df['instrument_type'] == 'EQ')]
        instruments_dict = dict(zip(instruments_df['trading_symbol'].values.tolist(),
                                    instruments_df['instrument_key'].values.tolist()))
        return instruments_dict
    except ApiException as e:
        logger.error(f"Exception when fetching instruments data: {e}")
        return None


def get_symbol_for_instrument(instrument_token):
    """fetch instruments and reverse the key value pair"""
    try:
        instruments_dict = fetch_instruments()
        reversed_dict = {v: k for k, v in instruments_dict.items()}
        return reversed_dict.get(instrument_token)
    except ApiException as e:
        logger.error(f"Exception when fetching instruments data: {e}")
        return None


def get_market_quote(api, instrument_tokens, mode="full"):
    try:
        if mode == "full":
            api_response = api.get_full_market_quote(instrument_tokens, api_version="v2").data
        elif mode == 'ohlc':
            api_response = api.get_market_quote_ohlc(instrument_tokens, interval='1d', api_version="v2").data
        else:
            api_response = api.ltp(instrument_tokens, api_version="v2").data
        latest_data = {}
        for key, data in api_response.items():
            latest_data = data

        if mode == 'full':
            return_data = {"symbol": latest_data.instrument_token,
                           "ltp": latest_data.last_price,
                           "open": latest_data.ohlc.open,
                           "high": latest_data.ohlc.high,
                           "low": latest_data.ohlc.low,
                           "close": latest_data.ohlc.close}
        elif mode == 'ohlc':
            return_data = {
                "symbol": latest_data.symbol,
                "ltp": latest_data.last_price,
                "open": latest_data.ohlc.open,
                "high": latest_data.ohlc.high,
                "low": latest_data.ohlc.low,
                "close": latest_data.ohlc.close,
                "volume": latest_data.volume}
        else:
            return_data = {
                "symbol": latest_data.instrument_token,
                "ltp": latest_data.last_price}
        return return_data
    except ApiException as e:
        logger.error(f"Exception when calling MarketQuoteApi->get_quotes: {e}")
        return None


def calculate_brokerage(api, instrument_token, quantity, price, transaction_type, product_type='D'):
    try:
        response = api.get_brokerage(
            instrument_token=instrument_token,
            quantity=quantity,
            price=price,
            transaction_type=transaction_type,
            product=product_type,
            api_version="v2")
        return response.data.charges.total
    except ApiException as e:
        logger.error(f"Error calculating brokerage: {e}")
        return 0


if __name__ == '__main__':
    # Example usage
    print(get_historical_data(instrument_token='NSE_EQ|INE051B01021'))
    print(fetch_instruments())