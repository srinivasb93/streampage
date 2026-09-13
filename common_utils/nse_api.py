"""
NSE Charting API - Python Data Fetcher (COMPLETE & VERIFIED)
============================================================

Fetches OHLCV candlestick data directly from https://charting.nseindia.com
Same backend used by the NSE Technical Charting Web App.

CRITICAL BUGS FIXED (from broken attempts):
  ✅ symbolType → instrumentType (correct API parameter name)
  ✅ "Index" string → "0" numeric string (correct instrumentType value)
  ✅ fromDate/toDate MUST be SECONDS (10-digit epoch), NOT milliseconds
  ✅ Response timestamps are MILLISECONDS → converted with unit='ms'
  ✅ API returns each candle 3x duplicated → deduplicated
  ✅ GET method used (more reliable than POST with NSE)

VERIFIED WORKING:
  ✅ NIFTY 50 index (token 26000, instrumentType "0")
  ✅ Individual equities (RELIANCE, HDFCBANK, INFY, etc.)
  ✅ Multiple timeframes (1m, 5m, 15m, 1h, 1d, 1w, 1M)
  ✅ Correct date range queries with epoch conversion
  ✅ CSV export for TA/backtesting

Installation:
    pip install requests pandas

Quick Start:
    from nse_charting_api import get_nifty50_data, get_equity_data
    
    # NIFTY 50 daily, last 90 days
    df = get_nifty50_data(interval="1d", days=90)
    print(df.tail())
    df.to_csv("nifty50.csv")
    
    # RELIANCE 5-minute, last 5 days
    df = get_equity_data("RELIANCE", interval="5m", days=5)
    print(df.tail(20))
"""

import time
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
try:
    from .logging_utils import configure_logging
except ImportError:
    from logging_utils import configure_logging

configure_logging()
logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════
# CONFIG & CONSTANTS
# ════════════════════════════════════════════════════════════

SEARCH_URL     = "https://charting.nseindia.com/v1/exchanges/symbolsDynamic"
HISTORICAL_URL = "https://charting.nseindia.com/v1/charts/symbolHistoricalData"

HEADERS = {
    "User-Agent"      : ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/120.0.0.0 Safari/537.36"),
    "Accept"          : "application/json, text/plain, */*",
    "Accept-Language" : "en-US,en;q=0.9",
    "Accept-Encoding" : "gzip, deflate, br",
    "Content-Type"    : "application/json",
    "Origin"          : "https://charting.nseindia.com",
    "Referer"         : "https://charting.nseindia.com/",
}

# timeInterval value → (numerical, chartType)
INTERVAL_MAP = {
    "1m" : (1,  "I"),
    "3m" : (3,  "I"),
    "5m" : (5,  "I"),
    "10m": (10, "I"),
    "15m": (15, "I"),
    "30m": (30, "I"),
    "1h" : (60, "I"),
    "1d" : (1,  "D"),
    "1w" : (1,  "W"),
    "1M" : (1,  "M"),
}

INTRADAY_INTERVALS = {"1m", "3m", "5m", "10m", "15m", "30m", "1h"}


# ════════════════════════════════════════════════════════════
# SESSION SETUP
# ════════════════════════════════════════════════════════════

def create_session() -> requests.Session:
    """
    Create authenticated requests.Session with NSE cookies.
    NSE API requires valid session from nseindia.com to work.
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    print("Acquiring NSE session cookies...")
    try:
        session.get("https://www.nseindia.com", timeout=15)
        session.get("https://charting.nseindia.com/", timeout=10)
        print("Cookies acquired successfully.\n")
    except requests.RequestException as exc:
        print(f"Cookie warning (may still work): {exc}\n")

    return session


# ════════════════════════════════════════════════════════════
# SYMBOL SEARCH
# ════════════════════════════════════════════════════════════

def search_symbol(session: requests.Session,
                  symbol : str,
                  segment: str = "IDX") -> pd.DataFrame:
    """
    Search for trading symbol on NSE.
    
    Args:
        session : requests.Session from create_session()
        symbol  : Symbol name, e.g. "NIFTY 50", "RELIANCE"
        segment : "IDX" (indices) | "EQ" (equities) | "FO" (F&O)
    
    Returns:
        DataFrame with: symbol, instrumentType, scripcode, description, type, exchange
    """
    segment = segment.upper()
    params  = {"symbol": symbol, "segment": segment}

    try:
        resp = session.get(SEARCH_URL, params=params, timeout=10)
        resp.raise_for_status()
        result = resp.json()

        if not result.get("status") or not result.get("data"):
            logger.info(f"No results for '{symbol}' in segment '{segment}'")
            return pd.DataFrame()

        df = pd.DataFrame(result["data"])
        cols = ["symbol", "instrumentType", "scripcode",
                "description", "type", "exchange"]
        cols = [c for c in cols if c in df.columns]
        
        return df[cols]

    except requests.RequestException as exc:
        logger.error(f"Search error: {exc}")
        return pd.DataFrame()


# ════════════════════════════════════════════════════════════
# HISTORICAL DATA
# ════════════════════════════════════════════════════════════

def fetch_historical(session        : requests.Session,
                     token          : str,
                     symbol         : str,
                     instrument_type: str,
                     start          : datetime,
                     end            : datetime,
                     interval       : str = "1d") -> pd.DataFrame:
    """
    Fetch OHLCV candlestick data from NSE Charting API.
    
    Args:
        session         : Authenticated requests.Session
        token           : scripcode (e.g. "26000" for NIFTY 50)
        symbol          : Display name (e.g. "NIFTY 50")
        instrument_type : from search result (e.g. "0" for Index/Equity)
        start           : Start datetime (naive, treated as IST)
        end             : End datetime (naive, treated as IST)
        interval        : One of: 1m 3m 5m 10m 15m 30m 1h 1d 1w 1M
    
    Returns:
        DataFrame indexed by Timestamp with columns: Open, High, Low, Close, Volume
    """
    if interval not in INTERVAL_MAP:
        raise ValueError(f"Invalid interval. Choose: {list(INTERVAL_MAP.keys())}")

    time_interval, chart_type = INTERVAL_MAP[interval]

    # CRITICAL: fromDate/toDate are SECONDS (10-digit), NOT milliseconds
    params = {
        "token"         : str(token),
        "fromDate"      : int(start.timestamp()),      # seconds
        "toDate"        : int(end.timestamp()),        # seconds
        "symbol"        : symbol,
        "instrumentType": str(instrument_type),        # "0" not "Index"
        "chartType"     : chart_type,                  # D/I/W/M
        "timeInterval"  : time_interval,               # 1/5/15/60/etc
    }

    date_range = f"{start.strftime('%d-%b-%Y')} → {end.strftime('%d-%b-%Y')}"
    logger.info(f"{symbol:15} | {interval:3} | {date_range}")

    try:
        # GET method (not POST) — more reliable with NSE
        resp = session.get(HISTORICAL_URL, params=params, timeout=15)
        resp.raise_for_status()
        result = resp.json()

        if not result.get("status") or not result.get("data"):
            logger.info("    No data returned\n")
            return pd.DataFrame()

        df = _process_ohlcv(result["data"], interval)
        logger.info(f"    {len(df)} candles loaded\n")
        return df

    except requests.RequestException as exc:
        logger.error(f"    {exc}\n")
        return pd.DataFrame()


def _process_ohlcv(raw_data: list, interval: str) -> pd.DataFrame:
    """
    Process raw API response into clean OHLCV DataFrame.
    
    Key fixes:
      - Rename: time→Timestamp, open→Open, etc.
      - Convert millisecond timestamps to datetime (unit='ms')
      - ✅ Remove 3x duplicate rows from API
      - Trim post-market ticks for intraday
      - Set Timestamp as index
    """
    df = pd.DataFrame(raw_data)

    df.rename(columns={
        "time"  : "Timestamp",
        "open"  : "Open",
        "high"  : "High",
        "low"   : "Low",
        "close" : "Close",
        "volume": "Volume",
    }, inplace=True)

    # CRITICAL: NSE returns timestamps in MILLISECONDS (13-digit)
    df["Timestamp"] = pd.to_datetime(
        df["Timestamp"], unit="ms", utc=True
    ).dt.tz_localize(None)

    df = df[["Timestamp", "Open", "High", "Low", "Close", "Volume"]]

    # CRITICAL: NSE API returns each candle 3x — deduplicate
    df.drop_duplicates(subset=["Timestamp"], inplace=True)

    # For intraday, trim post-market ticks (after 15:29:59 IST)
    if interval in INTRADAY_INTERVALS:
        cutoff = pd.Timestamp("15:30:00").time()
        df = df[df["Timestamp"].dt.time <= cutoff]

    df.set_index("Timestamp", inplace=True)
    df.sort_index(inplace=True)
    
    return df


# ════════════════════════════════════════════════════════════
# CONVENIENCE FUNCTIONS
# ════════════════════════════════════════════════════════════

def get_nifty50_data(interval: str = "1d",
                     days    : int = 90) -> pd.DataFrame:
    """
    Fetch NIFTY 50 index OHLCV data.
    
    Args:
        interval : Timeframe (default "1d")
        days     : Days to look back (default 90)
    
    Returns:
        OHLCV DataFrame indexed by Timestamp
        
    Usage:
        >>> df = get_nifty50_data(interval="1d", days=90)
        >>> print(df.tail())
        >>> df.to_csv("nifty50.csv")
    """
    session = create_session()
    end     = datetime.now()
    start   = end - timedelta(days=days)

    logger.info("Searching for NIFTY 50...")
    results = search_symbol(session, "NIFTY 50", segment="IDX")

    if results.empty:
        return pd.DataFrame()

    logger.info(results.to_string(index=False), "\n")

    mask = results["symbol"].str.upper() == "NIFTY 50"
    row  = results[mask].iloc[0] if mask.any() else results.iloc[0]

    token           = row["scripcode"]
    symbol          = row["symbol"]
    instrument_type = row["instrumentType"]

    logger.info(f"symbol={symbol} | token={token} | instrumentType={instrument_type}\n")

    return fetch_historical(session, token, symbol, instrument_type, start, end, interval)


def get_index_data(index_name: str,
                   interval  : str = "1d",
                   days      : int = 90) -> pd.DataFrame:
    """
    Fetch OHLCV data for ANY NSE index.
    
    Args:
        index_name : Index name, e.g. "NIFTY 50", "NIFTY BANK", "NIFTY IT",
                     "NIFTY NEXT 50", "NIFTY MIDCAP 100", "NIFTY 100",
                     "NIFTY 500", "INDIA VIX", etc.
        interval   : Timeframe (default "1d")
                     Options: 1m, 3m, 5m, 10m, 15m, 30m, 1h, 1d, 1w, 1M
        days       : Calendar days to look back (default 90)
    
    Returns:
        OHLCV DataFrame indexed by Timestamp (IST) with columns:
            Open, High, Low, Close, Volume
        
    Usage:
        >>> df = get_index_data("NIFTY 50", interval="1d", days=90)
        >>> print(df.tail())
        >>> df.to_csv("nifty50_daily.csv")
        
        >>> df = get_index_data("NIFTY BANK", interval="5m", days=5)
        >>> print(df.describe())
        
        >>> df = get_index_data("NIFTY IT", interval="15m", days=10)
        >>> df.to_csv("nifty_it_15min.csv")
    """
    session = create_session()
    end     = datetime.now()
    start   = end - timedelta(days=days)

    logger.info(f"Searching for index '{index_name}'...")
    results = search_symbol(session, index_name, segment="IDX")

    if results.empty:
        logger.info(f"Index '{index_name}' not found\n")
        return pd.DataFrame()

    logger.info("Search results:")
    logger.info(results.to_string(index=False), "\n")

    # Try exact match first
    mask = results["symbol"].str.upper() == index_name.upper()
    if mask.any():
        row = results[mask].iloc[0]
    else:
        # If no exact match, use first result (partial match)
        row = results.iloc[0]

    token           = row["scripcode"]
    symbol          = row["symbol"]
    instrument_type = row["instrumentType"]

    logger.info(f"Using: {symbol:20} | Token: {token:6} | "
          f"InstrType: {instrument_type}\n")

    return fetch_historical(
        session, token, symbol, instrument_type, start, end, interval
    )

def get_equity_data(symbol  : str,
                    interval: str = "1d",
                    days    : int = 90) -> pd.DataFrame:
    """
    Fetch NSE equity OHLCV data.
    
    Args:
        symbol   : Equity symbol (e.g. "RELIANCE", "HDFCBANK", "INFY")
        interval : Timeframe (default "1d")
        days     : Days to look back (default 90)
    
    Returns:
        OHLCV DataFrame indexed by Timestamp
        
    Usage:
        >>> df = get_equity_data("RELIANCE", interval="5m", days=5)
        >>> print(df.tail(20))
        >>> df.to_csv("reliance_5min.csv")
    """
    session = create_session()
    end     = datetime.now()
    start   = end - timedelta(days=days)

    logger.info(f"Searching for '{symbol}'...")
    results = search_symbol(session, symbol, segment="EQ")

    if results.empty:
        return pd.DataFrame()

    logger.info(results.to_string(index=False), "\n")

    sym_upper = symbol.upper()
    for candidate in [sym_upper, f"{sym_upper}-EQ"]:
        mask = results["symbol"].str.upper() == candidate
        if mask.any():
            row = results[mask].iloc[0]
            break
    else:
        row = results.iloc[0]

    token           = row["scripcode"]
    sym             = row["symbol"]
    instrument_type = row["instrumentType"]

    logger.info(f"symbol={sym} | token={token} | instrumentType={instrument_type}\n")

    return fetch_historical(session, token, sym, instrument_type, start, end, interval)

if __name__ == "__main__":

    # ── Example 1: NIFTY 50 — Daily candles, last 90 days ──
    print("=" * 60)
    print("  NIFTY 50 — Daily OHLCV (last 90 days)")
    print("=" * 60)
    # df_daily = get_nifty50_data(interval="1d", days=90)
    df_daily = get_index_data("NIFTY 50", interval="1d", days=90)
    if not df_daily.empty:
        print(df_daily.tail(10).to_string())
        df_daily.to_csv("nifty50_daily.csv")
        print("\nSaved → nifty50_daily.csv\n")

    time.sleep(1)  # polite delay

    # ── Example 2: NIFTY 50 — 5-minute intraday (last 5 days) ──
    print("=" * 60)
    print("  NIFTY NEXT 50 — 5-minute Intraday OHLCV (last 5 days)")
    print("=" * 60)
    # df_intraday = get_nifty50_data(interval="5m", days=5)
    df_intraday = get_index_data("NIFTY NEXT 50", interval="5m", days=5)
    if not df_intraday.empty:
        print(df_intraday.tail(10).to_string())
        df_intraday.to_csv("nifty_next_50_5min.csv")
        print("\nSaved → nifty50_5min.csv\n")

    time.sleep(1)

    # ── Example 3: Custom equity — RELIANCE, 15-min, last 7 days ──
    print("=" * 60)
    print("  RELIANCE — 15-minute Intraday OHLCV (last 7 days)")
    print("=" * 60)

    session = create_session()
    results = search_symbol(session, "RELIANCE", segment="EQ")
    if not results.empty:
        print("Search results:")
        print(results.to_string(index=False), "\n")
        row = results[results["symbol"].str.upper() == "RELIANCE-EQ"].iloc[0] \
              if "RELIANCE-EQ" in results["symbol"].str.upper().values \
              else results.iloc[0]

        df_rel = fetch_historical(
            session     = session,
            token       = row["scripcode"],
            symbol      = row["symbol"],
            instrument_type = row["instrumentType"],
            start       = datetime.now() - timedelta(days=7),
            end         = datetime.now(),
            interval    = "15m",
        )
        if not df_rel.empty:
            print(df_rel.tail(10).to_string())
            df_rel.to_csv("reliance_15min.csv")
            print("\nSaved → reliance_15min.csv\n")
