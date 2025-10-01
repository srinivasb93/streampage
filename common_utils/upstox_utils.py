import pandas as pd
from upstox_client.rest import ApiException
from datetime import datetime, timedelta
import logging
from .logging_utils import configure_logging
import requests
import os
import configparser
from sqlalchemy import create_engine, text
from typing import Optional

# Note: Do NOT import read_write_sql_data here to avoid circular imports.

configure_logging()
logger = logging.getLogger(__name__)

DATABASE = 'nsedata'


def _pg_engine_from_config(db_name: Optional[str] = None):
    """Create a SQLAlchemy engine using [postgres] from config.ini with env var overrides.

    Avoids importing read_write_sql_data to prevent circular imports.
    """
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    if 'postgres' not in cfg:
        raise RuntimeError("Missing [postgres] section in config.ini")
    
    pg = cfg['postgres']
    
    # Allow overriding [postgres] settings via environment variables
    # Supported env vars: POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DATABASE
    overrides = {
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'database': os.getenv('POSTGRES_DATABASE'),
    }
    for key, value in overrides.items():
        if value:
            pg[key] = value
    
    database = db_name or pg.get('database', 'trading_db')
    user = pg.get('user')
    password = pg.get('password')
    host = pg.get('host', 'localhost')
    port = pg.get('port', '5432')
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url, pool_size=1, max_overflow=2, pool_timeout=30, pool_recycle=1800)


def _detect_column(conn, schema: str, table: str, candidates: list[str]) -> Optional[str]:
    q = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
          AND column_name = ANY(:candidates)
        LIMIT 1
        """
    )
    row = conn.execute(q, {"schema": schema, "table": table, "candidates": candidates}).fetchone()
    return row[0] if row else None


def get_upstox_access_token() -> Optional[str]:
    """Return Upstox access token from env or PostgreSQL trading_db.users.

    Order of precedence:
      1) Env var UPSTOX_ACCESS_TOKEN if present and non-empty
      2) Database lookup in trading_db.public.users, preferring columns
         ['upstox_access_token', 'access_token', 'token'] and ordering by
         most recent timestamp column among ['updated_at','last_updated','modified_at'] if available.
    """
    env_token = os.getenv('UPSTOX_ACCESS_TOKEN')
    if env_token:
        return env_token

    try:
        engine = _pg_engine_from_config('trading_db')
        with engine.connect() as conn:
            schema = 'public'
            table = 'users'
            token_col = _detect_column(conn, schema, table, ['upstox_access_token', 'access_token', 'token'])
            if not token_col:
                logger.error("Could not find a token column in trading_db.public.users")
                return None

            ts_col = _detect_column(conn, schema, table, ['updated_at', 'last_updated', 'modified_at'])
            if ts_col:
                q = text(f"SELECT \"{token_col}\" FROM {schema}.\"{table}\" WHERE \"{token_col}\" IS NOT NULL ORDER BY \"{ts_col}\" DESC LIMIT 1")
            else:
                q = text(f"SELECT \"{token_col}\" FROM {schema}.\"{table}\" WHERE \"{token_col}\" IS NOT NULL LIMIT 1")

            row = conn.execute(q).fetchone()
            if row and row[0]:
                return row[0]
            logger.error("No non-null Upstox token found in DB")
            return None
    except Exception as e:
        logger.error(f"Failed to fetch Upstox token from DB: {e}")
        return None


def get_historical_data(instrument_token, interval="days", unit="1", sort_data=True, start_date=None, end_date=None):
    try:
        if not start_date and not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=3650)).strftime("%Y-%m-%d")
        token = get_upstox_access_token()
        if not token:
            logger.error("Upstox access token not available (env or DB)")
            return None
        headers = {"Authorization": f"Bearer {token}"}
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


def get_intraday_candle_data(instrument_key, unit='days', interval="1"):
    """Fetch raw intraday candles for a given instrument and interval."""
    token = get_upstox_access_token()
    if not token:
        logger.error("Upstox access token not available (env or DB)")
        return pd.DataFrame()

    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.upstox.com/v3/historical-candle/intraday/{instrument_key}/{unit}/{interval}"

    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != 200:
            logger.error("Failed to fetch intraday data for %s: %s", instrument_key, response.text)
            return pd.DataFrame()

        payload = response.json()
        data_section = payload.get('data', {})
        candles = None
        if isinstance(data_section, dict):
            candles = data_section.get('candles')
        if candles is None and isinstance(data_section, list):
            candles = data_section
        if candles is None:
            candles = payload.get('data')
        if not candles:
            return pd.DataFrame()

        if isinstance(candles[0], dict):
            df = pd.DataFrame(candles)
            column_map = {
                'timestamp': 'timestamp',
                'time': 'timestamp',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'volume': 'volume'
            }
            df = df.rename(columns=column_map)
            required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            for col in required_cols:
                if col not in df.columns:
                    df[col] = 0 if col == 'volume' else None
            df = df[required_cols]
        else:
            required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            width = len(candles[0])
            if width < 6:
                logger.error('Unexpected intraday candle structure for %s: %s', instrument_key, candles[:1])
                return pd.DataFrame()
            columns = required_cols + (['oi'] if width > 6 else [])
            df = pd.DataFrame(candles, columns=columns)
            df = df[required_cols]

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.sort_values('timestamp', inplace=True)
        if 'volume' in df:
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
        return df.reset_index(drop=True)
    except Exception as exc:
        logger.exception('Error fetching intraday data for %s: %s', instrument_key, exc)
        return pd.DataFrame()

def get_live_data(instrument_token=None):
    try:
        token = get_upstox_access_token()
        if not token:
            logger.error("Upstox access token not available (env or DB)")
            return {"ltp": 0, "depth": None}
        headers = {"Authorization": f"Bearer {token}"}
        url = f"https://api.upstox.com/v3/historical-candle/intraday/{instrument_token}/minutes/1"
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
    # print(get_historical_data(instrument_token='NSE_EQ|INE051B01021'))
    print(get_intraday_candle_data(instrument_key='NSE_EQ|INE051B01021', unit='minutes', interval='15'))
    # print(get_intraday_daily_bar(instrument_key='NSE_EQ|INE051B01021'))
    # print(fetch_instruments())

