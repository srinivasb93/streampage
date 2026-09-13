"""
This module fetches data from NSE API
"""
import datetime
import json
import pandas as pd
import requests
import datetime as dt
import logging
import sys
import os
import time
import re
import threading
from io import StringIO

# Add the project root and common_utils directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(parent_dir)
common_utils_dir = os.path.join(project_root, 'common_utils')

if common_utils_dir not in sys.path:
    sys.path.insert(0, common_utils_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from common_utils import read_write_sql_data as rd
from common_utils.logging_utils import configure_logging


# Set up logging
configure_logging()
logger = logging.getLogger(__name__)

headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9,en-IN;q=0.8,en-GB;q=0.7",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "sec-ch-ua": '"Microsoft Edge";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0"
        }


# NSE serves its JSON only to a session that has already fetched a page and picked up
# cookies, so a session must be primed before use. Priming costs two extra requests,
# which is why it is done once per thread and reused rather than per call: the chunked
# historicalOR walks issue one request per 90-day window, and re-priming each time
# tripled every walk's request count and runtime.
#
# The session is thread-local because Streamlit runs each script run on its own
# thread; a single shared requests.Session is not guaranteed safe across threads.
_nse_local = threading.local()


def _nse_session(force_new=False):
    """A cookie-primed NSE session for the current thread."""
    session = getattr(_nse_local, "session", None)
    if session is None or force_new:
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        session.get("https://www.nseindia.com/option-chain", headers=headers, timeout=10)
        _nse_local.session = session
    return session


def fetch_nse_data(payload, params=None):
    """GET an NSE JSON endpoint, returning {} on any failure.

    A non-JSON body (an HTML error page, the bot-check interstitial, a transient 503)
    is retried once with a freshly primed session, since the usual cause is expired
    cookies. Only the second failure is logged, with the status and content-type, so
    callers can tell "no data" from a broken endpoint — the original code swallowed a
    bare ValueError, which made a retired endpoint look identical to an empty result.
    """
    last_response = None
    for attempt in (0, 1):
        try:
            session = _nse_session(force_new=(attempt == 1))
            last_response = session.get(payload, headers=headers, params=params,
                                        timeout=10)
            return last_response.json()
        except ValueError:
            if attempt == 0:
                continue
            logger.error("NSE returned non-JSON for %s (HTTP %s, content-type %s): %s",
                         last_response.url, last_response.status_code,
                         last_response.headers.get("content-type"),
                         last_response.text[:200])
        except requests.RequestException as exc:
            if attempt == 0:
                continue
            logger.error("NSE request failed for %s: %s", payload, exc)
    return {}


# NSE's market-watch API keys off an abbreviated index SYMBOL, not the display
# name that /api/allIndices reports: "NIFTY INDIA CONSUMPTION" must be asked for as
# "NIFTY CONSUMPTION", "NIFTY OIL & GAS" as "NIFTY OIL AND GAS", and so on. Asking
# with the display name returns HTTP 200 with an empty `data` list rather than an
# error, which used to surface much later as a bare pandas KeyError. /api/index-names
# publishes the mapping, so resolve through it instead of hard-coding aliases.
_INDEX_SYMBOL_CACHE = {}


def _index_symbol_map():
    """Display-name -> API symbol, from /api/index-names. Cached for the process."""
    if not _INDEX_SYMBOL_CACHE:
        payload = fetch_nse_data("https://www.nseindia.com/api/index-names")
        for pair in (payload or {}).get("nts", []):
            if isinstance(pair, (list, tuple)) and len(pair) == 2 and pair[1]:
                _INDEX_SYMBOL_CACHE[pair[0]] = pair[1]
        if not _INDEX_SYMBOL_CACHE:
            logger.warning("Could not load NSE index-name map; using index names as given")
    return _INDEX_SYMBOL_CACHE


def resolve_index_symbol(index_name):
    """The symbol NSE's market-watch API expects for `index_name`.

    Falls back to the name unchanged when the map is unavailable or has no entry,
    which is correct for the many indices whose name is already the symbol.
    """
    return _index_symbol_map().get(index_name, index_name)


def fetch_index_constituents(index_name):
    """Constituents of `index_name`, normalised to the legacy response shape.

    NSE now serves this through NextApi/marketWatchApi. That endpoint covers both the
    regular indices and the "SECURITIES IN F&O" pseudo-index, which the older
    equity-stock-Indices path no longer returns at all — so it is the one source for
    both callers here.

    NextApi nests everything a level deeper, as
    ``{"data": {"data": [...], "timestamp": ..., "marketStatus": ...}}``. The inner
    object is returned directly so callers keep reading ``payload['data']`` and
    ``payload['timestamp']`` exactly as they did before.

    The index-names map is authoritative for the regular indices but not for every
    entry: it rewrites "SECURITIES IN F&O" to "Securities in F&O", which this API
    then serves empty. So the resolved symbol is tried first and the name as given is
    tried second, which covers both directions without hard-coding either spelling.
    A wrong name yields HTTP 200 with an empty list rather than an error, so an empty
    result is the only signal available to retry on.
    """
    url = "https://www.nseindia.com/api/NextApi/apiClient/marketWatchApi"
    candidates = [resolve_index_symbol(index_name)]
    if index_name not in candidates:
        candidates.append(index_name)

    for candidate in candidates:
        payload = fetch_nse_data(url, params={"functionName": "getIndicesData",
                                             "symbol": candidate})
        inner = (payload or {}).get("data")
        if isinstance(inner, dict) and inner.get("data"):
            return inner
    return {"data": []}


class MarketData:
    def __init__(self):
        self.base_url = "https://www.nseindia.com/api/"
        self.headers = {"User-Agent": "Mozilla/5.0"}
        self.event_type = "dividend"
        self.broad_indices_list = [
            "NIFTY 50", "NIFTY NEXT 50", "NIFTY MIDCAP 50", "NIFTY MIDCAP 100", "NIFTY MIDCAP 150",
            "NIFTY SMALLCAP 250", "NIFTY SMALLCAP 50", "NIFTY SMALLCAP 100", "NIFTY 100", "NIFTY 200",
            "NIFTY 500", "NIFTY MIDSMALLCAP 400", "NIFTY MIDCAP SELECT", "NIFTY LARGEMIDCAP 250"]

        self.sector_indices_list = [
            "NIFTY BANK", "NIFTY FINANCIAL SERVICES", "NIFTY IT", "NIFTY MEDIA", "NIFTY METAL", "NIFTY PHARMA",
            "NIFTY PSU BANK", "NIFTY REALTY", "NIFTY AUTO", "NIFTY HEALTHCARE INDEX", "NIFTY FMCG",
            "NIFTY PRIVATE BANK", "NIFTY CONSUMER DURABLES", "NIFTY OIL & GAS"]

        self.thematic_indices_list = [
            "NIFTY ENERGY", "NIFTY CPSE", "NIFTY INFRASTRUCTURE", "NIFTY100 LIQUID 15", "NIFTY PSE",
            "NIFTY COMMODITIES", "NIFTY MNC", "NIFTY INDIA CONSUMPTION",  "NIFTY MIDCAP LIQUID 15",
            "NIFTY SERVICES SECTOR", "NIFTY INDIA DIGITAL", "NIFTY INDIA MANUFACTURING"]

    @staticmethod
    def fetch_and_load_nse_events():
        output = fetch_nse_data('https://www.nseindia.com/api/event-calendar')
        if not output:
            logger.info("No NSE events found ")
            return "No data found"
        else:
            # Load data to SQL
            df = pd.DataFrame(output)
            df.rename(columns={'symbol': 'Symbol', 'company': 'Company', 'date': 'Event_Date',
                               'purpose': 'Event_Purpose', 'dm_desc': 'Event'}, inplace=True)
            msg = rd.load_sql_data(data_to_load=df, table_name='NSE_EVENTS')
            logger.info(msg)
            return "Success" if "success" in msg else "Failure"

    @staticmethod
    def get_quote(symbol):
        payload = fetch_index_constituents('SECURITIES IN F&O')
        for row in payload.get('data', []):
            if row.get('symbol') == symbol.upper():
                return row
        return {}

    @staticmethod
    def nse_holidays(type="trading", load=False, as_df=True):
        merged_df = pd.DataFrame()
        if type in ["clearing", "all"]:
            payload = fetch_nse_data('https://www.nseindia.com/api/holiday-master?type=clearing')
            clearing_df = pd.DataFrame(payload['CM'])
            clearing_df['type'] = 'clearing'
            clearing_df.rename(columns={'tradingDate': 'trading_date', 'weekDay': 'week_day'}, inplace=True)
            clearing_df['trading_date'] = pd.to_datetime(clearing_df['trading_date'], format='%d-%b-%Y')
            merged_df = pd.concat([merged_df, clearing_df[['trading_date', 'week_day', 'description', 'type']]], ignore_index=True)
        if type in ["trading", "all"]:
            payload = fetch_nse_data('https://www.nseindia.com/api/holiday-master?type=trading')
            trading_df = pd.DataFrame(payload['CM'])
            trading_df['type'] = 'trading'
            trading_df.rename(columns={'tradingDate': 'trading_date', 'weekDay': 'week_day'}, inplace=True)
            trading_df['trading_date'] = pd.to_datetime(trading_df['trading_date'], format='%d-%b-%Y')
            merged_df = pd.concat([merged_df, trading_df[['trading_date', 'week_day', 'description', 'type']]], ignore_index=True)
        if load:
            msg = rd.load_sql_data(merged_df, table_name='NSE_HOLIDAYS')
            logger.info(msg)
            return "Success" if "success" in msg else "Failure"
        return merged_df if as_df else payload

    def equity_history_virgin(self, symbol, series, start_date, end_date):
        url = 'https://www.nseindia.com/api/historical/cm/equity?symbol=' + symbol + '&series=["' + series + '"]&from=' + start_date + '&to=' + end_date

        payload = fetch_nse_data(url)
        return pd.DataFrame.from_records(payload["data"])

    def equity_history(self, symbol, series, start_date, end_date):
        # We are getting the input in text. So it is being converted to Datetime object from String.
        start_date = datetime.datetime.strptime(start_date, "%d-%m-%Y")
        end_date = datetime.datetime.strptime(end_date, "%d-%m-%Y")
        logging.info("Starting Date: " + str(start_date))
        logging.info("Ending Date: " + str(end_date))

        # We are calculating the difference between the days
        diff = end_date - start_date
        logging.info("Total Number of Days: " + str(diff.days))
        logging.info("Total FOR Loops in the program: " + str(int(diff.days / 40)))
        logging.info("Remainder Loop: " + str(diff.days - (int(diff.days / 40) * 40)))

        total = pd.DataFrame()
        for i in range(0, int(diff.days / 40)):
            temp_date = (start_date + datetime.timedelta(days=(40))).strftime("%d-%m-%Y")
            start_date = datetime.datetime.strftime(start_date, "%d-%m-%Y")

            logging.info("Loop = " + str(i))
            logging.info("====")
            logging.info("Starting Date: " + str(start_date))
            logging.info("Ending Date: " + str(temp_date))
            logging.info("====")

            # total=total.append(equity_history_virgin(symbol,series,start_date,temp_date))
            # total=total.concat(equity_history_virgin(symbol,series,start_date,temp_date))
            total = pd.concat([total, self.equity_history_virgin(symbol, series, start_date, temp_date)])

            logging.info("Length of the Table: " + str(len(total)))

            # Preparation for the next loop
            start_date = datetime.datetime.strptime(temp_date, "%d-%m-%Y")

        start_date = datetime.datetime.strftime(start_date, "%d-%m-%Y")
        end_date = datetime.datetime.strftime(end_date, "%d-%m-%Y")

        logging.info("End Loop")
        logging.info("====")
        logging.info("Starting Date: " + str(start_date))
        logging.info("Ending Date: " + str(end_date))
        logging.info("====")

        # total=total.append(equity_history_virgin(symbol,series,start_date,end_date))
        # total=total.concat(equity_history_virgin(symbol,series,start_date,end_date))
        total = pd.concat([total, self.equity_history_virgin(symbol, series, start_date, end_date)])

        logging.info("Finale")
        logging.info("Length of the Total Dataset: " + str(len(total)))
        payload = total.iloc[::-1].reset_index(drop=True)
        return payload

    @staticmethod
    def security_wise_archive(from_date, to_date, symbol, series="ALL"):
        base_url = "https://www.nseindia.com/api/historical/securityArchives"
        url = f"{base_url}?from={from_date}&to={to_date}&symbol={symbol.upper()}&dataType=priceVolumeDeliverable&series={series.upper()}"
        payload = fetch_nse_data(url)
        return pd.DataFrame(payload['data'])

    FNO_SNAPSHOT_COLUMNS = ['Symbol', 'Open', 'High', 'Low', 'Close', 'Prev_Close', 'Day_Change',
                            'Pct_Change', 'Year_High', 'Year_Low', 'Traded_Volume', 'Traded_Value',
                            'Last_Updated']

    @staticmethod
    def nse_get_fno_snapshot_live(mode="pandas"):
        """Live cash-market snapshot of the securities available in F&O.

        In 'pandas' mode this always returns a DataFrame — empty with the right
        columns when NSE returns nothing. It used to fall through a bare `except:`
        and hand back the raw dict instead, so `load_fno_snapshot_live` received the
        wrong type and the real cause never reached the log.
        """
        payload = fetch_index_constituents('SECURITIES IN F&O')
        if mode != "pandas":
            return payload

        rows = payload.get('data', [])
        if not rows:
            logger.warning("No F&O snapshot rows returned by NSE")
            return pd.DataFrame(columns=MarketData.FNO_SNAPSHOT_COLUMNS)

        df = pd.DataFrame(rows).copy()
        df['lastUpdateTime'] = payload.get("timestamp") or datetime.datetime.now()
        df['lastUpdateTime'] = pd.to_datetime(df['lastUpdateTime'], format='%d-%b-%Y %H:%M:%S',
                                              errors='coerce')
        df.rename(columns={'symbol': 'Symbol', 'open': 'Open', 'dayHigh': 'High', 'dayLow': 'Low',
                           'lastPrice': 'Close', 'previousClose': 'Prev_Close', 'change': 'Day_Change',
                           'pChange': 'Pct_Change', 'yearHigh': 'Year_High', 'yearLow': 'Year_Low',
                           'totalTradedVolume': 'Traded_Volume', 'totalTradedValue': 'Traded_Value',
                           'lastUpdateTime': 'Last_Updated'}, inplace=True)
        missing = [c for c in MarketData.FNO_SNAPSHOT_COLUMNS if c not in df.columns]
        if missing:
            logger.warning("F&O snapshot is missing columns %s; they will be empty", missing)
            for column in missing:
                df[column] = None
        return df[MarketData.FNO_SNAPSHOT_COLUMNS]

    def load_fno_snapshot_live(self, mode="pandas"):
        """
        Load FNO snapshot live data to SQL
        :param mode: 'pandas' or 'raw'
        :return: Success or Failure message
        """
        fno_snapshot = self.nse_get_fno_snapshot_live(mode=mode)
        if isinstance(fno_snapshot, pd.DataFrame) and fno_snapshot.empty:
            # Replacing the live snapshot with nothing loses the last good one.
            logger.error("F&O snapshot came back empty; leaving FNO_SNAPSHOT_LIVE untouched")
            return "Failure"
        msg = rd.load_sql_data(data_to_load=fno_snapshot, table_name='FNO_SNAPSHOT_LIVE')
        return "Success" if "success" in msg else "Failure"

    # getETFData uses the market-watch field names (dayHigh/dayLow/totalTradedVolume),
    # not the high/low/qty of the older /api/etf feed.
    ETF_PRICE_COLUMNS = {'symbol': 'Symbol', 'companyName': 'Company_Name', 'open': 'Open',
                         'dayHigh': 'High', 'dayLow': 'Low', 'nav': 'Close',
                         'totalTradedVolume': 'Volume'}

    # Descriptive columns still absent from every live feed, so carried forward.
    # companyName is no longer among them: getETFData supplies it for every row.
    ETF_META_COLUMNS = ['Listing_Date', 'Delisted', 'Suspended']

    ETF_COLUMNS = ['Symbol', 'Open', 'High', 'Low', 'Close', 'Volume',
                   'Company_Name', 'Listing_Date', 'Listed_Years', 'Delisted', 'Suspended']

    @staticmethod
    def _carry_forward_etf_metadata(etf_data):
        """Re-attach ETF descriptive columns from the previous ETF_DATA load.

        NSE's ETF feeds no longer carry the nested `meta` object that supplied listing
        date and the delisted/suspended flags, and per-symbol quote-equity now answers
        403, so there is no live source for them. They are static per symbol, so the
        last known values are carried forward by symbol. Since the table is written
        with if_exists='replace', not doing this would blank out real data on every
        load.
        """
        for column in MarketData.ETF_META_COLUMNS:
            if column not in etf_data.columns:
                etf_data[column] = None

        try:
            previous = rd.get_table_data(selected_table='ETF_DATA')
        except Exception as exc:
            logger.warning("Could not read existing ETF_DATA for metadata carry-forward: %s", exc)
            return etf_data

        if previous is None or previous.empty or 'Symbol' not in previous.columns:
            logger.warning("No previous ETF_DATA metadata to carry forward")
            return etf_data

        keep = ['Symbol'] + [c for c in MarketData.ETF_META_COLUMNS if c in previous.columns]
        previous = previous[keep].drop_duplicates(subset='Symbol')
        merged = etf_data.drop(columns=[c for c in MarketData.ETF_META_COLUMNS
                                        if c in etf_data.columns]).merge(
            previous, on='Symbol', how='left')

        unmatched = int(merged['Listing_Date'].isna().sum()) if 'Listing_Date' in merged else 0
        if unmatched:
            logger.info("%d ETF symbol(s) have no carried-forward metadata (new listings)", unmatched)
        return merged

    def fetch_and_load_etf_data(self, as_df=True, load=True):
        # The older /api/etf feed still responds but has dropped `meta` (and with it
        # companyName); getETFData carries companyName for every row, so it is the
        # better source. `symbol=all` is required — without it the call returns no rows.
        etf_payload = fetch_nse_data(
            "https://www.nseindia.com/api/NextApi/apiClient/marketWatchApi",
            params={"functionName": "getETFData", "symbol": "all"})
        etf_payload = (etf_payload or {}).get("data")
        if not isinstance(etf_payload, dict):
            etf_payload = {}
        if not as_df:
            return etf_payload

        rows = etf_payload.get("data", [])
        if not rows:
            logger.error("No ETF rows returned by NSE; leaving ETF_DATA untouched")
            return "Failure" if load else pd.DataFrame(columns=self.ETF_COLUMNS)

        etf_data = pd.DataFrame(rows)
        missing = [c for c in self.ETF_PRICE_COLUMNS if c not in etf_data.columns]
        if missing:
            logger.warning("ETF response is missing columns %s; they will be empty", missing)
            for column in missing:
                etf_data[column] = None

        etf_data = etf_data[list(self.ETF_PRICE_COLUMNS)].rename(columns=self.ETF_PRICE_COLUMNS)
        etf_data = self._carry_forward_etf_metadata(etf_data)

        # Listing_Date stays as stored (text) so the ETF_DATA column type does not
        # change; the parse is only to derive Listed_Years.
        listing_date = pd.to_datetime(etf_data['Listing_Date'], errors='coerce')
        etf_data['Listed_Years'] = round((dt.datetime.today() - listing_date).dt.days / 365, 1)
        etf_data = etf_data[self.ETF_COLUMNS]

        if load:
            msg = rd.load_sql_data(data_to_load=etf_data, table_name='ETF_DATA')
            logger.info(msg)
            return "Success" if "success" in msg else "Failure"
        return etf_data

    def get_nse_indices_data(self, as_df=True):
        """
        Get nse indices
        :param as_df:
        :return:
        """
        url = self.base_url + "allIndices"
        indices_data = fetch_nse_data(url)
        if as_df:
            indices_data = pd.DataFrame(indices_data.get("data", []))
        return indices_data

    def load_indices_data(self):
        """
        Load indices data
        """
        data_to_load = self.get_nse_indices_data()
        load_msg = rd.load_sql_data(data_to_load, table_name="NSE_INDICES_DATA")
        print(load_msg)
        return "Success" if "success" in load_msg else "Failure"

    @staticmethod
    def get_main_nse_indices_list(all_data=False):
        """
        Get main nse indices list
        :return:
        """
        indices_data = rd.get_table_data(selected_table="NSE_INDICES_DATA")
        req_indices_list = indices_data['index'].to_list()
        return req_indices_list if not all_data else indices_data

    SOURCE_COLUMNS = ['symbol', 'open', 'dayHigh', 'dayLow', 'lastPrice', 'previousClose', 'change',
                      'pChange', 'yearHigh', 'yearLow', 'totalTradedVolume', 'totalTradedValue',
                      'lastUpdateTime', 'nearWKH', 'nearWKL', 'perChange365d', 'date365dAgo',
                      'date30dAgo', 'perChange30d']

    COLUMN_RENAMES = {'symbol': 'Symbol', 'open': 'Open', 'dayHigh': 'High', 'dayLow': 'Low',
                      'lastPrice': 'Close', 'previousClose': 'Prev_Close', 'change': 'Day_Change',
                      'pChange': 'Pct_Change', 'yearHigh': 'Year_High', 'yearLow': 'Year_Low',
                      'totalTradedVolume': 'Traded_Volume', 'totalTradedValue': 'Traded_Value',
                      'lastUpdateTime': 'Last_Updated', 'nearWKH': 'WK_High', 'nearWKL': 'WK_Low',
                      'perChange365d': 'Pct_Change_365d', 'date365dAgo': 'Date_365d_Ago',
                      'date30dAgo': 'Date_30d_Ago', 'perChange30d': 'Pct_Change_30d'}

    def get_index_stocks_data(self, stock_idx='NIFTY 50', as_df=True):
        """
        Get index stocks data. Returns an empty DataFrame (correctly named columns,
        no rows) when NSE reports no constituents for `stock_idx` — true for the
        bond, VIX and dividend-point indices that `allIndices` also lists.
        :return:
        """
        index_data = fetch_index_constituents(stock_idx)
        if not as_df:
            return index_data

        rows = index_data.get("data", [])
        if not rows:
            logger.warning("No constituents returned for index %r (symbol %r)",
                           stock_idx, resolve_index_symbol(stock_idx))
            return pd.DataFrame(columns=[self.COLUMN_RENAMES[c] for c in self.SOURCE_COLUMNS])

        index_data = pd.DataFrame(rows)
        missing = [c for c in self.SOURCE_COLUMNS if c not in index_data.columns]
        if missing:
            logger.warning("Index %r response is missing columns %s; they will be empty",
                           stock_idx, missing)
            for column in missing:
                index_data[column] = None

        index_data = index_data[self.SOURCE_COLUMNS]
        index_data.rename(columns=self.COLUMN_RENAMES, inplace=True)
        return index_data

    def load_index_stocks_data(self, indices_list):
        """
        Load indices data
        :param indices_list:
        :return:
        """
        status_list = []
        skipped = []
        for stock_index in indices_list:
            data_to_load = self.get_index_stocks_data(stock_idx=stock_index)
            if data_to_load.empty:
                # Not a failure: several indices NSE lists (bond, VIX, dividend
                # points) have no equity constituents. Overwriting the existing
                # _REF table with nothing would lose good data, so skip the load.
                skipped.append(stock_index)
                continue
            table_name = stock_index.replace(" ", "_") + "_REF"
            if '&' in table_name:
                table_name = table_name.replace('&', 'AND')
            load_msg = rd.load_sql_data(data_to_load, table_name=table_name)
            if 'success' in load_msg:
                status_list.append(True)
            else:
                status_list.append(False)

        if skipped:
            logger.warning("Skipped %d index/indices with no constituents: %s",
                           len(skipped), ", ".join(skipped))
        return "Success" if status_list and all(status_list) else "Failure"

    def load_all_stocks_table_with_stock_index(self, all_indices_list):
        """
        Load all stocks along with their index details into sql table
        :param all_indices_list:
        :return:
        """
        all_stocks_df = pd.DataFrame()
        indices_ref = self.get_main_nse_indices_list(all_data=True)
        for stock_index in all_indices_list:
            table_name = stock_index.replace(" ", "_") + "_REF"
            if '&' in table_name:
                table_name = table_name.replace('&', 'AND')
            stock_idx_data = rd.get_table_data(selected_table=table_name)
            stock_index_symbol = indices_ref['indexSymbol'][indices_ref['index'] == stock_index].values[0]
            stock_idx_data['STK_INDEX'] = stock_index
            stock_idx_data['STK_INDEX_SYMBOL'] = stock_index_symbol
            stock_idx_data = stock_idx_data[['Symbol', 'STK_INDEX', 'STK_INDEX_SYMBOL']][1:]
            stock_idx_data.rename(columns={'Symbol': 'SYMBOL'}, inplace=True)

            all_stocks_df = pd.concat([all_stocks_df, stock_idx_data], ignore_index=True, axis=0)
        load_msg = rd.load_sql_data(all_stocks_df, table_name='ALL_STOCKS')
        return "Success" if "success" in load_msg else "Failure"

    def get_nse_indices_pe_pb_data(self):
        """
        Get latest PE/PB/Dividend data for all NSE indices
        :return: DataFrame with PE/PB/Dividend data
        """
        try:
            # Get all indices list
            all_indices = self.broad_indices_list + self.sector_indices_list + self.thematic_indices_list
            
            pe_pb_data = []
            current_date = datetime.datetime.now().strftime('%d-%m-%Y')
            
            for index in all_indices:
                try:
                    # Get latest PE/PB data for the index
                    table_name = index.replace('&', 'AND').replace(' ', '_') + '_PE_PB_DIV'
                    latest_data = rd.get_table_data(selected_table=table_name)
                    
                    if not latest_data.empty:
                        # Get the most recent data
                        latest_record = latest_data.iloc[-1]  # Assuming data is ordered by timestamp
                        
                        # Parse and convert numeric values safely
                        def safe_convert_to_float(value, default=0.0):
                            """Safely convert value to float, handling concatenated strings"""
                            if pd.isna(value) or value is None:
                                return default
                            
                            # If it's already a number, return it
                            if isinstance(value, (int, float)):
                                return float(value)
                            
                            # If it's a string, try to extract the first valid number
                            if isinstance(value, str):
                                # Remove any non-numeric characters except decimal point
                                import re
                                # Find the first valid number in the string
                                numbers = re.findall(r'\d+\.?\d*', str(value))
                                if numbers:
                                    try:
                                        return float(numbers[0])
                                    except ValueError:
                                        return default
                                return default
                            
                            return default
                        
                        pe_pb_data.append({
                            'index_name': index,
                            'pe': safe_convert_to_float(latest_record.get('pe', 0)),
                            'pb': safe_convert_to_float(latest_record.get('pb', 0)),
                            'div_yield': safe_convert_to_float(latest_record.get('div_yield', 0)),
                            'timestamp': latest_record.get('timestamp', current_date)
                        })
                except Exception:
                    # If table doesn't exist or error, skip this index
                    continue
            
            if pe_pb_data:
                df = pd.DataFrame(pe_pb_data)
                
                # Ensure all numeric columns are properly typed
                numeric_columns = ['pe', 'pb', 'div_yield']
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                
                # Remove any rows where all numeric values are 0 (likely invalid data)
                df = df[~((df['pe'] == 0) & (df['pb'] == 0) & (df['div_yield'] == 0))]
                
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            print(f"Error fetching PE/PB data: {str(e)}")
            return pd.DataFrame()

    def get_historical_pe_pb_data(self, index_name, days_back=365):
        """
        Get historical PE/PB/Dividend data for a specific index
        :param index_name: Name of the index
        :param days_back: Number of days to look back
        :return: DataFrame with historical data
        """
        try:
            table_name = index_name.replace('&', 'AND').replace(' ', '_') + '_PE_PB_DIV'
            historical_data = rd.get_table_data(selected_table=table_name)
            
            if not historical_data.empty:
                # Convert timestamp to datetime if it's not already
                if 'timestamp' in historical_data.columns:
                    historical_data['timestamp'] = pd.to_datetime(historical_data['timestamp'])
                    
                    # Filter data for the specified period
                    cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days_back)
                    historical_data = historical_data[historical_data['timestamp'] >= cutoff_date]
                
                # Clean and convert numeric columns
                numeric_columns = ['pe', 'pb', 'div_yield']
                for col in numeric_columns:
                    if col in historical_data.columns:
                        historical_data[col] = pd.to_numeric(historical_data[col], errors='coerce')
                
                # Remove rows with all NaN values
                historical_data = historical_data.dropna(subset=numeric_columns, how='all')
                
                return historical_data
            else:
                return pd.DataFrame()
                
        except Exception as e:
            print(f"Error fetching historical data for {index_name}: {str(e)}")
            return pd.DataFrame()

    def get_all_indices_historical_analysis(self, days_back=365):
        """
        Get historical analysis for all indices
        :param days_back: Number of days to look back
        :return: Dictionary with analysis results for each index
        """
        try:
            all_indices = self.broad_indices_list + self.sector_indices_list + self.thematic_indices_list
            analysis_results = {}
            
            for index in all_indices:
                historical_data = self.get_historical_pe_pb_data(index, days_back)
                
                if not historical_data.empty and len(historical_data) > 10:  # Need sufficient data points
                    # Calculate historical statistics
                    current_pe = historical_data['pe'].iloc[-1] if 'pe' in historical_data.columns else 0
                    current_pb = historical_data['pb'].iloc[-1] if 'pb' in historical_data.columns else 0
                    current_div = historical_data['div_yield'].iloc[-1] if 'div_yield' in historical_data.columns else 0
                    
                    # Historical percentiles
                    pe_percentile = (historical_data['pe'] <= current_pe).mean() * 100 if 'pe' in historical_data.columns else 50
                    pb_percentile = (historical_data['pb'] <= current_pb).mean() * 100 if 'pb' in historical_data.columns else 50
                    div_percentile = (historical_data['div_yield'] <= current_div).mean() * 100 if 'div_yield' in historical_data.columns else 50
                    
                    # Historical ranges
                    pe_min, pe_max = historical_data['pe'].min(), historical_data['pe'].max() if 'pe' in historical_data.columns else (0, 0)
                    pb_min, pb_max = historical_data['pb'].min(), historical_data['pb'].max() if 'pb' in historical_data.columns else (0, 0)
                    div_min, div_max = historical_data['div_yield'].min(), historical_data['div_yield'].max() if 'div_yield' in historical_data.columns else (0, 0)
                    
                    # Trend analysis (last 30 days vs previous 30 days)
                    if len(historical_data) >= 60:
                        recent_30 = historical_data.tail(30)
                        previous_30 = historical_data.iloc[-60:-30]
                        
                        pe_trend = recent_30['pe'].mean() - previous_30['pe'].mean() if 'pe' in historical_data.columns else 0
                        pb_trend = recent_30['pb'].mean() - previous_30['pb'].mean() if 'pb' in historical_data.columns else 0
                        div_trend = recent_30['div_yield'].mean() - previous_30['div_yield'].mean() if 'div_yield' in historical_data.columns else 0
                    else:
                        pe_trend = pb_trend = div_trend = 0
                    
                    analysis_results[index] = {
                        'current_pe': current_pe,
                        'current_pb': current_pb,
                        'current_div': current_div,
                        'pe_percentile': pe_percentile,
                        'pb_percentile': pb_percentile,
                        'div_percentile': div_percentile,
                        'pe_min': pe_min,
                        'pe_max': pe_max,
                        'pb_min': pb_min,
                        'pb_max': pb_max,
                        'div_min': div_min,
                        'div_max': div_max,
                        'pe_trend': pe_trend,
                        'pb_trend': pb_trend,
                        'div_trend': div_trend,
                        'data_points': len(historical_data),
                        'historical_data': historical_data
                    }
            
            return analysis_results
            
        except Exception as e:
            print(f"Error in historical analysis: {str(e)}")
            return {}


def index_history(symbol, start_date, end_date):
    """Index OHLC history for `symbol` between the two dates.

    Now served by NSE's historicalOR/indicesHistory (niftyindices.com's Backpage.aspx
    API is retired). `symbol` must be the index display name; see the module notes.

    `volume` carries the real traded quantity from the feed. The niftyindices version
    had no volume field and hard-coded 0.
    """
    frame = _fetch_historical_or('indicesHistory', symbol, start_date, end_date,
                                 'EOD_TIMESTAMP')
    return _shape_historical_or(
        frame, INDEX_HISTORY_COLUMNS,
        {'EOD_OPEN_INDEX_VAL': 'open', 'EOD_HIGH_INDEX_VAL': 'high',
         'EOD_LOW_INDEX_VAL': 'low', 'EOD_CLOSE_INDEX_VAL': 'close',
         'HIT_TRADED_QTY': 'volume'},
        symbol)

# NSE's historicalOR/* endpoints replace niftyindices.com's retired Backpage.aspx API:
#   indicesYield   -> index PE / PB / dividend-yield history
#   indicesHistory -> index OHLC + traded-quantity history
#
# Both share two behaviours that dictate how they must be called:
#
# 1. A response is capped at 70 rows and TRUNCATED SILENTLY past that — a 265-day
#    request returns HTTP 200 with only the OLDEST 70 trading days of the window and
#    no error or flag. So a range has to be walked in chunks. 90 calendar days yields
#    ~63 trading days, comfortably under the cap; the walk additionally resumes from
#    the newest row it actually received, so a truncated chunk cannot drop days.
#
# 2. They key off the index DISPLAY NAME, the opposite of the market-watch API:
#    "NIFTY INDIA CONSUMPTION" works and the resolved "NIFTY CONSUMPTION" returns
#    nothing. Do not put resolve_index_symbol() in front of them.
#
# They differ only in row order — indicesYield ascending, indicesHistory descending —
# which is why the resume point is max() over parsed timestamps rather than the last
# row's position.
HISTORICAL_OR_URL = "https://www.nseindia.com/api/historicalOR/"
HISTORICAL_OR_ROW_CAP = 70
HISTORICAL_OR_CHUNK_DAYS = 90

# indicesYield serves nothing before 2016; indicesHistory reaches back to at least
# 2000. Clamping avoids a wasted request per chunk over the empty years.
INDICES_YIELD_EARLIEST = dt.date(2016, 1, 1)

INDEX_YIELD_COLUMNS = ['timestamp', 'index_name', 'div_yield', 'pe', 'pb']
INDEX_HISTORY_COLUMNS = ['timestamp', 'open', 'high', 'low', 'close', 'volume']


def _as_date(value):
    """Parse the date forms this module passes around.

    Callers use day-first strings ('27-08-2026', '27-Aug-2026'), but the Streamlit
    date pickers hand over ISO. ISO is tried explicitly first so pandas does not warn
    about dayfirst being ignored for it.
    """
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        return value
    if isinstance(value, dt.datetime):
        return value.date()
    text = str(value).strip()
    try:
        return dt.datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return pd.to_datetime(text, dayfirst=True).date()


def _fetch_historical_or(path, index_name, start_date, end_date, date_field,
                         earliest=None):
    """Walk a historicalOR endpoint over [start_date, end_date] in safe chunks.

    Returns the concatenated raw records with an added parsed `timestamp` column,
    de-duplicated and sorted oldest-first, or an empty DataFrame. See the module
    notes above for the row cap and the display-name requirement.
    """
    start = _as_date(start_date)
    if earliest is not None:
        start = max(start, earliest)
    end = _as_date(end_date)
    if start > end:
        return pd.DataFrame()

    frames = []
    cursor = start
    while cursor <= end:
        window_end = min(cursor + dt.timedelta(days=HISTORICAL_OR_CHUNK_DAYS - 1), end)
        payload = fetch_nse_data(HISTORICAL_OR_URL + path, params={
            "indexType": index_name,
            "from": cursor.strftime("%d-%m-%Y"),
            "to": window_end.strftime("%d-%m-%Y"),
        })
        rows = (payload or {}).get("data", [])
        if not rows:
            cursor = window_end + dt.timedelta(days=1)
            continue

        chunk = pd.DataFrame.from_records(rows)
        chunk['timestamp'] = pd.to_datetime(chunk[date_field], format='%d-%b-%Y',
                                            errors='coerce')
        chunk = chunk.dropna(subset=['timestamp'])
        if chunk.empty:
            cursor = window_end + dt.timedelta(days=1)
            continue
        frames.append(chunk)

        newest = chunk['timestamp'].max().date()
        if len(rows) >= HISTORICAL_OR_ROW_CAP and newest < window_end:
            # Capped mid-window: resume the day after the newest row received rather
            # than skipping to window_end, which would lose the remainder.
            cursor = newest + dt.timedelta(days=1)
        else:
            cursor = window_end + dt.timedelta(days=1)

    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True).drop_duplicates(subset='timestamp')
    return combined.sort_values('timestamp').reset_index(drop=True)


def _shape_historical_or(frame, columns, renames, index_label):
    """Apply `renames`, guarantee `columns` exist, and project onto them."""
    if frame.empty:
        return pd.DataFrame(columns=columns)
    frame = frame.rename(columns=renames)
    missing = [c for c in columns if c not in frame.columns]
    if missing:
        logger.warning("historicalOR response for %r is missing %s", index_label, missing)
        for column in missing:
            frame[column] = None
    return frame[columns].reset_index(drop=True)


def index_pe_pb_div(symbol, start_date, end_date):
    """PE / PB / dividend-yield history for `symbol` between the two dates.

    `symbol` must be the index display name (see the module notes above).
    """
    frame = _fetch_historical_or('indicesYield', symbol, start_date, end_date,
                                 'IY_DT', earliest=INDICES_YIELD_EARLIEST)
    return _shape_historical_or(
        frame, INDEX_YIELD_COLUMNS,
        {'IY_INDEX': 'index_name', 'IY_DY': 'div_yield', 'IY_PE': 'pe', 'IY_PB': 'pb'},
        symbol)


# Per-stock PE ratios come from the "PE Ratio" daily report on
# https://www.nseindia.com/all-reports (NSE's JSON feeds carry index PE but not
# stock PE, and per-symbol quote-equity answers 403). The file is a plain CSV,
# SYMBOL / SYMBOL P/E / ADJUSTED P/E, one row per listed stock:
#   https://nsearchives.nseindia.com/content/equities/peDetail/PE_ddmmyy.csv
#
# ADJUSTED P/E differs from SYMBOL P/E where a company's earnings need restating
# (demergers, consolidations); both are kept rather than picking one.
# NSE keeps only a rolling window of this report: the earliest file that still
# resolves is PE_270424.csv (2024-04-27); everything before it 404s, and there is no
# deeper source. quote-equity and equity-meta-info (which carry per-symbol PE) answer
# 403, the peDetail directory has no listing or archive/zip variant, and no other feed
# exposes stock PE. So stock-level PE history starts here, unlike corporate actions
# which reach back to 2007.
STOCKS_PE_EARLIEST = dt.date(2024, 4, 27)

STOCKS_PE_TABLE = 'STOCKS_PE'
STOCKS_PE_PATH = "https://nsearchives.nseindia.com/content/equities/peDetail/"
STOCKS_PE_FILE_KEY = "CM-PE-RATIO-CSV"
STOCKS_PE_COLUMNS = ['timestamp', 'Symbol', 'PE', 'Adjusted_PE']


_TRADING_HOLIDAYS = set()
_HOLIDAYS_LOADED = []


def _trading_holidays():
    """Set of NSE trading-holiday dates, fetched once per process."""
    if not _HOLIDAYS_LOADED:
        _HOLIDAYS_LOADED.append(True)
        try:
            frame = MarketData.nse_holidays(type="trading", as_df=True)
            _TRADING_HOLIDAYS.update(frame['trading_date'].dt.date.tolist())
        except Exception as exc:
            logger.warning("Could not load NSE trading holidays: %s", exc)
    return _TRADING_HOLIDAYS


def is_trading_day(date):
    """Whether NSE traded on `date`.

    Needed because the archive serves a PE file for non-trading days too, holding a
    byte-identical copy of the previous session's numbers — Saturday's and Sunday's
    files are the same bytes. Loading those would file stale rows under dates on
    which nothing traded.

    holiday-master covers the current year, so a backfill into an earlier year is
    only weekend-guarded.
    """
    date = _as_date(date)
    if date.weekday() >= 5:
        return False
    return date not in _trading_holidays()


def latest_stocks_pe_report():
    """(date, url) of the most recently published PE Ratio report, or (None, None).

    Read from the daily-reports index rather than assumed, because the current
    trading day's file only appears after NSE publishes it — asking for today
    before then, or on a holiday, would 404.
    """
    payload = fetch_nse_data("https://www.nseindia.com/api/daily-reports",
                             params={"key": "CM"})
    for day in ("CurrentDay", "PreviousDay"):
        for entry in (payload or {}).get(day, []):
            if entry.get("fileKey") == STOCKS_PE_FILE_KEY and entry.get("fileActlName"):
                path = entry.get("filePath") or STOCKS_PE_PATH
                return _as_date(entry["tradingDate"]), path + entry["fileActlName"]
    return None, None


def _read_stocks_pe_csv(url):
    """Fetch and parse one PE Ratio CSV. Returns None when it is not published.

    Retried once with a fresh session, because a backfill issues hundreds of
    requests and a transient failure is otherwise indistinguishable from a day NSE
    never published - it would silently become a permanent hole in the series. (A
    real case: 2024-04-29 returned 404 mid-sweep and fetched fine moments later.)
    """
    last = None
    for attempt in (0, 1):
        try:
            response = _nse_session(force_new=(attempt == 1)).get(
                url, headers=headers, timeout=30)
        except requests.RequestException as exc:
            last = f"{type(exc).__name__}: {exc}"
            continue
        if response.status_code == 200 and 'csv' in (response.headers.get('content-type') or ''):
            return pd.read_csv(StringIO(response.text))
        last = f"HTTP {response.status_code}, {response.headers.get('content-type')}"
    logger.warning("PE Ratio report not available at %s (%s)", url, last)
    return None


def fetch_stocks_pe(date=None, fallback=True, symbols=None):
    """Per-stock PE for `date`, or for the latest published report if unavailable.

    The returned `timestamp` is the report's own trading date, so it always
    reflects the data actually fetched even when the requested date had no file.

    `fallback=False` disables the substitution and returns an empty frame when the
    requested date has no report. Backfilling needs that: silently substituting the
    latest report for a missing historical date would file today's numbers over and
    over instead of skipping the gap.

    `symbols` restricts the result to those tickers. The report covers ~2 800
    symbols; keeping only the ones actually tracked in STOCKS_IN_DB is what stops a
    daily series from growing by thousands of rows a day.
    """
    frame = None
    report_date = None
    if date is not None:
        report_date = _as_date(date)
        if is_trading_day(report_date):
            frame = _read_stocks_pe_csv(
                STOCKS_PE_PATH + "PE_%s.csv" % report_date.strftime("%d%m%y"))
        else:
            logger.info("%s is not an NSE trading day; using the latest published report",
                        report_date)

    if frame is None and not fallback:
        return pd.DataFrame(columns=STOCKS_PE_COLUMNS)

    if frame is None:
        latest_date, latest_url = latest_stocks_pe_report()
        if latest_url is None:
            logger.error("No PE Ratio report listed in NSE's daily reports")
            return pd.DataFrame(columns=STOCKS_PE_COLUMNS)
        if report_date is not None and latest_date != report_date:
            logger.warning("No PE Ratio report for %s; using the latest published (%s)",
                           report_date, latest_date)
        report_date, frame = latest_date, _read_stocks_pe_csv(latest_url)
        if frame is None:
            return pd.DataFrame(columns=STOCKS_PE_COLUMNS)

    frame.columns = [str(c).strip().upper() for c in frame.columns]
    renames = {'SYMBOL': 'Symbol', 'SYMBOL P/E': 'PE', 'ADJUSTED P/E': 'Adjusted_PE'}
    missing = [c for c in renames if c not in frame.columns]
    if missing:
        logger.error("PE Ratio report layout changed; missing %s (got %s)",
                     missing, list(frame.columns))
        return pd.DataFrame(columns=STOCKS_PE_COLUMNS)

    frame = frame.rename(columns=renames)
    frame['Symbol'] = frame['Symbol'].astype(str).str.strip()
    for column in ('PE', 'Adjusted_PE'):
        frame[column] = pd.to_numeric(frame[column], errors='coerce')
    frame['timestamp'] = pd.Timestamp(report_date)
    frame = frame[frame['Symbol'].ne('') & frame['Symbol'].notna()]
    if symbols is not None:
        wanted = {str(x).strip().upper() for x in symbols}
        frame = frame[frame['Symbol'].str.upper().isin(wanted)]
    return frame[STOCKS_PE_COLUMNS].drop_duplicates(subset='Symbol').reset_index(drop=True)




def _stocks_pe_symbols(use_registry, database):
    """Symbol filter for the PE loaders: the registry, or None for every symbol.

    Returns False when the registry was requested but is unreadable/empty, so the
    caller can refuse rather than silently loading all ~2 800 symbols.
    """
    if not use_registry:
        return None
    symbols = registry_symbols(database)
    if not symbols:
        logger.error("STOCKS_IN_DB gave no symbols; refusing to load unfiltered PE "
                     "data into %s", STOCKS_PE_TABLE)
        return False
    return symbols


def loaded_stocks_pe_dates(database='nsedata'):
    """Report dates already present in STOCKS_PE, so a backfill can resume."""
    exists = rd.get_table_data(
        selected_database=database,
        query="SELECT 1 FROM information_schema.tables "
              f"WHERE table_schema='public' AND table_name='{STOCKS_PE_TABLE}'")
    if exists is None or exists.empty:
        return set()
    frame = rd.get_table_data(
        selected_database=database,
        query=f'SELECT DISTINCT timestamp FROM public."{STOCKS_PE_TABLE}"')
    if frame is None or frame.empty:
        return set()
    return set(pd.to_datetime(frame['timestamp']).dt.date)


def backfill_stocks_pe(from_date=None, to_date=None, database='nsedata', batch_days=20,
                       use_registry=True):
    """Fill STOCKS_PE with every archived PE report between the two dates.

    Defaults to the full available window (STOCKS_PE_EARLIEST to today) and to the
    symbols in STOCKS_IN_DB. Dates already in the table are skipped, so this is
    resumable and safe to re-run, and rows are written every `batch_days` dates so
    an interruption keeps its progress rather than discarding the whole run.

    The registry filter is the difference between ~1.2M rows and ~120k for the same
    date range; pass use_registry=False for every symbol NSE reports.
    """
    symbols = _stocks_pe_symbols(use_registry, database)
    if symbols is False:
        return "Failure"

    start = max(_as_date(from_date), STOCKS_PE_EARLIEST) if from_date else STOCKS_PE_EARLIEST
    end = _as_date(to_date) if to_date else dt.date.today()
    if start > end:
        logger.error("Backfill range is empty: %s > %s", start, end)
        return "Failure"

    already = loaded_stocks_pe_dates(database)
    pending = [d for d in (start + dt.timedelta(days=i) for i in range((end - start).days + 1))
               if is_trading_day(d) and d not in already]
    logger.info("Backfilling STOCKS_PE: %s trading day(s) to fetch between %s and %s "
                "(%s already loaded), filtered to %s symbol(s)",
                len(pending), start, end, len(already),
                len(symbols) if symbols is not None else "all")
    if not pending:
        return "Success"

    batch, written, missing = [], 0, []
    table_exists = bool(already) or bool(loaded_stocks_pe_dates(database))

    def flush(rows):
        nonlocal written, table_exists
        if not rows:
            return True
        frame = pd.concat(rows, ignore_index=True)
        load_type = 'append' if table_exists else 'replace'
        msg = rd.load_sql_data(frame, table_name=STOCKS_PE_TABLE, database=database,
                               load_type=load_type)
        if 'success' not in msg:
            logger.error("Backfill write failed: %s", msg)
            return False
        table_exists = True
        written += len(frame)
        logger.info("Wrote %s rows (%s dates); %s total this run",
                    len(frame), frame['timestamp'].nunique(), written)
        return True

    for day in pending:
        frame = fetch_stocks_pe(day, fallback=False, symbols=symbols)
        if frame.empty:
            missing.append(day)
            continue
        batch.append(frame)
        if len(batch) >= batch_days:
            if not flush(batch):
                return "Failure"
            batch = []
    if not flush(batch):
        return "Failure"

    if missing:
        # A handful of trading days simply have no published report (e.g. 2025-07-21)
        # and never will, so they stay "pending" on every run. That is not a failure
        # and must not be reported as one - only a write error is. An outage is still
        # visible: it shows up here as every pending day missing, at ERROR level.
        # Escalate only when the volume suggests an outage rather than the odd
        # unpublished day: all pending days missing AND more than a few of them.
        outage = len(missing) == len(pending) and len(pending) > 3
        level = logging.ERROR if outage else logging.WARNING
        logger.log(level, "No PE report published for %s of %s trading day(s), e.g. %s",
                   len(missing), len(pending), [str(d) for d in missing[:5]])
    logger.info("Backfill complete: %s rows added", written)
    return "Success"


def load_stocks_pe(date=None, use_registry=True, database='nsedata'):
    """Append one day of per-stock PE to STOCKS_PE, building history over time.

    Appending rather than replacing keeps a dated series; re-running for a date
    already present is a no-op so a repeated daily load cannot duplicate rows.

    Filtered to STOCKS_IN_DB by default, matching backfill_stocks_pe - otherwise the
    daily run would add thousands of untracked symbols a day and undo the point of
    filtering the backfill.
    """
    symbols = _stocks_pe_symbols(use_registry, database)
    if symbols is False:
        return "Failure"
    data_to_load = fetch_stocks_pe(date, symbols=symbols)
    if data_to_load.empty:
        logger.error("No PE Ratio data fetched; leaving %s untouched", STOCKS_PE_TABLE)
        return "Failure"

    report_date = data_to_load['timestamp'].iloc[0]
    load_type = 'append'
    # Probe information_schema rather than selecting from the table: on the first ever
    # run the table is absent, and a failed SELECT logs an ERROR from the shared
    # reader that makes a perfectly good first load look broken.
    exists = rd.get_table_data(
        selected_database=database,
        query="SELECT 1 FROM information_schema.tables "
              f"WHERE table_schema='public' AND table_name='{STOCKS_PE_TABLE}'")
    if exists is None or exists.empty:
        logger.info("%s does not exist yet; creating it", STOCKS_PE_TABLE)
        load_type = 'replace'
    else:
        existing = rd.get_table_data(
            selected_database=database, selected_table=STOCKS_PE_TABLE,
            query=f'SELECT MAX(timestamp) AS timestamp FROM "{STOCKS_PE_TABLE}"')
        latest_loaded = existing.iloc[0]['timestamp'] if not existing.empty else None
        if latest_loaded is not None and pd.notna(latest_loaded)                 and pd.Timestamp(latest_loaded).normalize() >= report_date.normalize():
            logger.info("%s already holds %s; nothing to load",
                        STOCKS_PE_TABLE, report_date.date())
            return "Success"

    msg = rd.load_sql_data(data_to_load, table_name=STOCKS_PE_TABLE, database=database,
                           load_type=load_type)
    logger.info("%s rows for %s: %s", len(data_to_load), report_date.date(), msg)
    return "Success" if "success" in msg else "Failure"


# --------------------------------------------------------------------------------
# Parsing the free-text `Subject` field
# --------------------------------------------------------------------------------
# NSE ships the whole corporate action as one uncontrolled string, and it is messy in
# three ways that dictate the approach below:
#
# 1. Abbreviations are inconsistent: Bonus/Bon, Rights/Rght/Rhts, Dividend/Div,
#    "Face Value Split"/"Fv Split"/"Fv Spl", sometimes with the spaces missing
#    entirely ("Fv Spl-Rs10tore1").
# 2. One row can carry SEVERAL actions: "Agm/Spl/Bon-1:1/Div-20%",
#    "Bonus-1:1 Spl-Rs 5/ To 2/", "Div Int-Rs1.20+Spl-Rs1.30". So a single
#    Action_Type would lose information - Action_Types keeps every type found, and
#    the per-action columns are filled independently of each other.
# 3. "Spl" means BOTH "Split" and "Special" (dividend). "Spl-Rs 5/ To 2/" is a face
#    value split; "Spl-Rs.20" is a special dividend. They are told apart by the
#    presence of a `to` linking two face values - which is why SPLIT_RE requires it.
#
# Ratios are only meaningful for the actions that change the share count, so
# BONUS/SPLIT/RIGHTS get parsed ratios and everything else leaves them NULL.
# DEMERGER is a deliberate exception: NSE publishes the bare word "Demerger" with no
# ratio anywhere in the feed (all 16 occurrences are the single string "Demerger"),
# so its ratio columns stay NULL and Action_Type still says DEMERGER.

# a:b  ->  `a` new shares for every `b` held.
BONUS_RE = re.compile(r'\bbo?n(?:us)?\b[^0-9]{0,8}(\d+)\s*:\s*(\d+)', re.I)
# Requires the `to`, which is what separates a face-value split from "Spl-Rs.20"
# (a special dividend, no `to` anywhere). The gaps are wide but bounded, and exclude
# digits and ':' so a match cannot jump across an adjacent amount or a bonus ratio.
# Handles all of: "Rs.10/- To Re.1/-", "Rs10tore1",
# "From Rs 5/- Per Share To Re 1/- Per Share",
# "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 2/- Per Share".
SPLIT_RE = re.compile(
    r'(?:f\.?v\.?|face\s*value|spl(?:it)?)'
    r'[^0-9:]{0,28}?'
    r'(?:rs|re)?\.?\s*(\d+(?:\.\d+)?)'
    # \bto\b covers "…/- To Re.1/-"; (?<=\d)to covers the squashed "Rs10tore1",
    # where 'to' has word characters on both sides so \b never fires. Anchoring the
    # second form to a preceding digit keeps it from matching 'to' inside a word.
    r'[^0-9:]{0,24}?(?:\bto\b|(?<=\d)to)[^0-9:]{0,14}?'
    r'(?:rs|re)?\.?\s*(\d+(?:\.\d+)?)',
    re.I)
# No trailing \b: the feed writes "Rght1:5" and "Rhts-Eq1:5", where a digit or letter
# follows the abbreviation directly.
RIGHTS_RE = re.compile(r'\b(?:rights?|rght|rhts|rhs)[^0-9]{0,10}(\d+)\s*:\s*(\d+)', re.I)
RIGHTS_PREM_RE = re.compile(r'(?:@|prem\w*)[^0-9]{0,10}(\d+(?:\.\d+)?)', re.I)
# Any "Rs <amount>" that is not the right-hand side of a split.
DIV_AMOUNT_RE = re.compile(r'(?:rs|re)\.?\s*(\d+(?:\.\d+)?)', re.I)
DIV_PCT_RE = re.compile(r'(\d+(?:\.\d+)?)\s*%')

# Three competing constraints:
#  * no trailing \b - the feed writes "Int.Div70%" and "Div-Rs.15";
#  * must still catch NSE's own typos "Interimdividend" and "Specia Ldividend",
#    where "div" has a word character in front, hence the second alternative;
#  * must NOT match "Division", which appears in split wording as
#    "Face Value Split (Sub-Division)" and would otherwise tag splits as dividends.
DIVIDEND_TOKEN_RE = re.compile(r'\bdiv(?!ision)|dividend', re.I)

# Order matters: the first match becomes Action_Type, so capital-structure events
# (which need a price adjustment) outrank income and administrative ones.
ACTION_PATTERNS = [
    ('SPLIT',            SPLIT_RE),
    ('BONUS',            BONUS_RE),
    ('DEMERGER',         re.compile(r'demerg', re.I)),
    ('RIGHTS',           re.compile(r'\b(?:rights?|rght|rhts|rhs)', re.I)),
    ('BUYBACK',          re.compile(r'buy\s*-?\s*back', re.I)),
    # \bmerger\b, not merger: "Demerger" has no word boundary before "merger", so
    # this no longer swallows every demerger into AMALGAMATION as well. The old
    # lookahead could not help - it checks forward, and "demerg" sits behind.
    ('AMALGAMATION',     re.compile(r'\bamalgamat|\bmerger\b|scheme\s+of\s+arr?an', re.I)),
    ('CAPITAL_REDUCTION', re.compile(r'reduction\s+of\s+capital|capital\s+reduction', re.I)),
    ('DELISTING',        re.compile(r'delist', re.I)),
    ('INTEREST_PAYMENT', re.compile(r'interest\s*pay|\bint\.?\s*pay', re.I)),
    ('DIVIDEND',         re.compile(r'\bdiv(?!ision)|dividend', re.I)),
    ('AGM',              re.compile(r'\bagm\b|annual\s+general\s+meeting', re.I)),
    ('EGM',              re.compile(r'\begm\b|extra\s*-?\s*ordinary\s+general\s+meeting', re.I)),
    # "Meeing" is NSE's own typo, present in the data.
    ('AGM',              re.compile(r'annual\s+general\s+mee', re.I)),
    ('BOOK_CLOSURE',     re.compile(r'book\s*closure', re.I)),
    ('MEETING',          re.compile(r'\bmeeting\b|\bboard\s+meeting\b|\be-?\s*voting\b', re.I)),
]

# Dividend flavour, reported separately so DIVIDEND rows stay one type. Every match
# is kept: "Div Int-Rs1.20+Spl-Rs1.30" is genuinely an interim AND a special payout,
# so picking just one would misreport it.
DIVIDEND_KIND_PATTERNS = [
    ('INTERIM', re.compile(r'\binterim\b|\bint\.?\s*div|\bdiv\.?\s*int\b', re.I)),
    ('SPECIAL', re.compile(r'\bspecial\b|\bspl\b(?!\s*[-\s]*(?:rs|re)?\.?\s*\d+(?:\.\d+)?\s*/?\s*-?\s*to)', re.I)),
    ('FINAL',   re.compile(r'\bfinal\b|\bfin\b', re.I)),
]

CORPORATE_ACTION_PARSED_COLUMNS = [
    'Action_Type', 'Action_Types', 'Dividend_Kind',
    'Bonus_New', 'Bonus_Held', 'Bonus_Ratio',
    'Split_From_FV', 'Split_To_FV', 'Split_Ratio',
    'Rights_New', 'Rights_Held', 'Rights_Ratio', 'Rights_Premium',
    'Dividend_Amount', 'Dividend_Percent',
    'Price_Adjustment_Factor', 'Is_Capital_Change',
]


def _empty_parsed():
    parsed = {c: None for c in CORPORATE_ACTION_PARSED_COLUMNS}
    parsed['Action_Type'] = 'OTHER'
    parsed['Action_Types'] = 'OTHER'
    parsed['Price_Adjustment_Factor'] = 1.0
    parsed['Is_Capital_Change'] = False
    return parsed


def parse_corporate_action(subject):
    """Turn one NSE `Subject` string into structured fields.

    Returns a dict with CORPORATE_ACTION_PARSED_COLUMNS.

    `Price_Adjustment_Factor` is the ratio by which the share count changes, expressed
    as a price multiplier: bonus 1:1 -> 0.5, face value Rs10 -> Rs2 -> 0.2, and 1.0
    for any action that leaves the share count alone. Compound actions multiply
    (a 1:1 bonus plus a 10->1 split gives 0.05).

    IMPORTANT for backtesting: do NOT apply this to the per-symbol price tables in
    nsedata. Those series arrive from NSE already back-adjusted for corporate
    actions - HDFCBANK's 1:1 bonus (ex-date 2025-08-26) shows ~994 on both sides of
    the event although it actually traded near 1990 beforehand - so applying the
    factor again would halve prices twice. The factor is here for the cases where
    that adjustment is NOT already done for you:
      * reconstructing historical share counts or position sizes across an event;
      * adjusting a price series from a source that serves unadjusted data;
      * flagging which dates carry a capital change at all (Is_Capital_Change),
        e.g. to exclude them from gap or overnight-return studies.
    Dividend_Amount is unaffected by this and is what a total-return backtest needs,
    since price series are adjusted for splits and bonuses but not for dividends.
    """
    parsed = _empty_parsed()
    if subject is None or (isinstance(subject, float) and pd.isna(subject)):
        return parsed
    text = str(subject).strip()
    if not text:
        return parsed

    types = [name for name, pattern in ACTION_PATTERNS if pattern.search(text)]
    # "Interest Payment/Buyback" style rows match several administrative patterns;
    # keep them all but let ACTION_PATTERNS order pick the headline type.
    if types:
        parsed['Action_Type'] = types[0]
        parsed['Action_Types'] = '|'.join(dict.fromkeys(types))

    factor = 1.0

    bonus = BONUS_RE.search(text)
    if bonus:
        new, held = int(bonus.group(1)), int(bonus.group(2))
        if new > 0 and held > 0:
            parsed['Bonus_New'], parsed['Bonus_Held'] = new, held
            parsed['Bonus_Ratio'] = f"{new}:{held}"
            # `new` extra shares per `held` -> holdings scale by (held+new)/held.
            factor *= held / (held + new)
            parsed['Is_Capital_Change'] = True

    split = SPLIT_RE.search(text)
    if split:
        old_fv, new_fv = float(split.group(1)), float(split.group(2))
        # Guard against a "to" that is not really a split (e.g. amounts that grow).
        if old_fv > 0 and new_fv > 0 and new_fv < old_fv:
            parsed['Split_From_FV'], parsed['Split_To_FV'] = old_fv, new_fv
            parsed['Split_Ratio'] = f"{old_fv:g}:{new_fv:g}"
            factor *= new_fv / old_fv
            parsed['Is_Capital_Change'] = True
        elif 'SPLIT' in types and parsed['Action_Type'] == 'SPLIT':
            # Matched the wording but not a usable ratio; fall back to the next type.
            remaining = [t for t in types if t != 'SPLIT']
            parsed['Action_Type'] = remaining[0] if remaining else 'OTHER'

    rights = RIGHTS_RE.search(text)
    if rights:
        new, held = int(rights.group(1)), int(rights.group(2))
        if new > 0 and held > 0:
            parsed['Rights_New'], parsed['Rights_Held'] = new, held
            parsed['Rights_Ratio'] = f"{new}:{held}"
        prem = RIGHTS_PREM_RE.search(text)
        if prem:
            parsed['Rights_Premium'] = float(prem.group(1))
        # Rights are NOT folded into Price_Adjustment_Factor: the correct adjustment
        # depends on the issue price versus the market price on the ex-date, which
        # this feed does not carry. Ratio and premium are exposed so a caller that
        # has prices can compute it.

    if DIVIDEND_TOKEN_RE.search(text):
        kinds = [kind for kind, pattern in DIVIDEND_KIND_PATTERNS if pattern.search(text)]
        if kinds:
            parsed['Dividend_Kind'] = '|'.join(kinds)
        # Exclude figures that belong to a split ("Rs 5/ To 2/") or a rights premium.
        consumed = set()
        if split:
            consumed |= set(range(split.start(), split.end()))
        prem = RIGHTS_PREM_RE.search(text)
        if prem:
            consumed |= set(range(prem.start(), prem.end()))
        amounts = [float(m.group(1)) for m in DIV_AMOUNT_RE.finditer(text)
                   if not (set(range(m.start(), m.end())) & consumed)]
        if amounts:
            # Compound payouts ("Div Int-Rs1.20+Spl-Rs1.30") are summed: the total
            # cash per share is what a total-return backtest needs.
            parsed['Dividend_Amount'] = round(sum(amounts), 4)
        pcts = [float(m.group(1)) for m in DIV_PCT_RE.finditer(text)]
        if pcts:
            parsed['Dividend_Percent'] = round(sum(pcts), 4)

    parsed['Price_Adjustment_Factor'] = round(factor, 10)
    return parsed


# Corporate actions (dividends, bonuses, splits, buybacks, AGMs, rights, demergers)
# come from NSE's corporates-corporateActions feed. It answers for all symbols at once
# when `symbol` is omitted, so history is pulled by date range rather than per stock —
# 20 requests for 2007-to-date instead of one per symbol per year.
#
# The feed will serve the whole 19-year range in a single call, and that call was
# verified complete (nothing inside the range is missing versus the sum of per-year
# calls). It is still requested year by year: one 40k-row response is a fragile thing
# to depend on, and per-year chunks make a partial failure cost one year, not all of it.
#
# `ind` and `caBroadcastDate` are dropped: both are empty for every row across
# 2007-2026 and for future-dated announcements, so they would only add dead columns.
CORPORATE_ACTIONS_TABLE = 'CORPORATE_ACTIONS'
CORPORATE_ACTIONS_URL = "https://www.nseindia.com/api/corporates-corporateActions"
CORPORATE_ACTIONS_EARLIEST = dt.date(2007, 1, 1)

# Source field -> column. Empty values arrive as the string '-' or as null.
CORPORATE_ACTIONS_FIELDS = {
    'symbol': 'Symbol', 'series': 'Series', 'comp': 'Company_Name', 'isin': 'ISIN',
    'faceVal': 'Face_Value', 'subject': 'Subject', 'exDate': 'Ex_Date',
    'recDate': 'Record_Date', 'bcStartDate': 'Book_Closure_Start',
    'bcEndDate': 'Book_Closure_End', 'ndStartDate': 'No_Delivery_Start',
    'ndEndDate': 'No_Delivery_End',
}
CORPORATE_ACTIONS_DATE_COLUMNS = ['Ex_Date', 'Record_Date', 'Book_Closure_Start',
                                  'Book_Closure_End', 'No_Delivery_Start', 'No_Delivery_End']
# Raw feed columns, then the fields parsed out of `Subject`. Subject itself is kept
# verbatim so nothing is lost if the parsing ever needs revisiting.
CORPORATE_ACTIONS_COLUMNS = (list(CORPORATE_ACTIONS_FIELDS.values())
                             + CORPORATE_ACTION_PARSED_COLUMNS)

# What makes one corporate action distinct. The feed does return a small number of
# exact duplicates (13 in ~39.7k rows), so rows are de-duplicated on this.
CORPORATE_ACTIONS_KEY = ['Symbol', 'Series', 'Subject', 'Ex_Date', 'Record_Date']


def registry_symbols(database='nsedata'):
    """Symbols listed in STOCKS_IN_DB, or an empty list if it cannot be read."""
    try:
        frame = rd.get_table_data(selected_database=database, selected_table='STOCKS_IN_DB')
    except Exception as exc:
        logger.warning("Could not read STOCKS_IN_DB: %s", exc)
        return []
    if frame is None or frame.empty or 'SYMBOL' not in frame.columns:
        return []
    return frame['SYMBOL'].dropna().astype(str).str.strip().unique().tolist()


def fetch_corporate_actions(from_date=None, to_date=None, symbols=None):
    """Corporate actions between the two dates, optionally limited to `symbols`.

    `symbols` of None means every symbol NSE reports, which includes government
    securities and other non-equity series; pass the registry to restrict it.
    """
    start = _as_date(from_date) if from_date else CORPORATE_ACTIONS_EARLIEST
    end = _as_date(to_date) if to_date else dt.date.today()
    if start > end:
        return pd.DataFrame(columns=CORPORATE_ACTIONS_COLUMNS)

    frames = []
    for year in range(start.year, end.year + 1):
        window_start = max(start, dt.date(year, 1, 1))
        window_end = min(end, dt.date(year, 12, 31))
        payload = fetch_nse_data(CORPORATE_ACTIONS_URL, params={
            "index": "equities",
            "from_date": window_start.strftime("%d-%m-%Y"),
            "to_date": window_end.strftime("%d-%m-%Y"),
        })
        if not isinstance(payload, list) or not payload:
            logger.warning("No corporate actions returned for %s..%s",
                           window_start, window_end)
            continue
        frames.append(pd.DataFrame.from_records(payload))
        logger.info("Corporate actions %s: %s rows", year, len(payload))

    if not frames:
        return pd.DataFrame(columns=CORPORATE_ACTIONS_COLUMNS)

    frame = pd.concat(frames, ignore_index=True)
    missing = [f for f in CORPORATE_ACTIONS_FIELDS if f not in frame.columns]
    if missing:
        logger.warning("Corporate actions response is missing fields %s", missing)
        for field in missing:
            frame[field] = None
    frame = frame[list(CORPORATE_ACTIONS_FIELDS)].rename(columns=CORPORATE_ACTIONS_FIELDS)

    for column in ('Symbol', 'Series', 'Company_Name', 'ISIN', 'Subject'):
        frame[column] = frame[column].astype(str).str.strip().replace({'-': None, 'None': None})
    for column in CORPORATE_ACTIONS_DATE_COLUMNS:
        frame[column] = pd.to_datetime(frame[column], format='%d-%b-%Y', errors='coerce')
    frame['Face_Value'] = pd.to_numeric(frame['Face_Value'], errors='coerce')

    if symbols is not None:
        wanted = {str(x).strip().upper() for x in symbols}
        before = len(frame)
        frame = frame[frame['Symbol'].str.upper().isin(wanted)]
        logger.info("Filtered corporate actions to %s registry symbols: %s of %s rows kept",
                    len(wanted), len(frame), before)

    frame = frame.drop_duplicates(subset=CORPORATE_ACTIONS_KEY)

    # Parse `Subject` into typed columns. Done after filtering so the work scales
    # with what is actually kept, and after dedup so identical strings are parsed once.
    unique_subjects = frame['Subject'].drop_duplicates()
    lookup = {subject: parse_corporate_action(subject) for subject in unique_subjects}
    parsed = pd.DataFrame(
        [lookup[subject] for subject in frame['Subject']],
        index=frame.index, columns=CORPORATE_ACTION_PARSED_COLUMNS)
    frame = pd.concat([frame, parsed], axis=1)

    unclassified = int((frame['Action_Type'] == 'OTHER').sum())
    if unclassified:
        logger.info("%s of %s corporate action(s) did not match a known type",
                    unclassified, len(frame))

    return frame[CORPORATE_ACTIONS_COLUMNS].sort_values(
        ['Ex_Date', 'Symbol']).reset_index(drop=True)


def load_corporate_actions(from_date=None, to_date=None, use_registry=True,
                           database='nsedata'):
    """Rebuild CORPORATE_ACTIONS for the given range (default 2007-01-01 to today).

    A full replace rather than an append: NSE revises and back-fills corporate
    actions after the fact, and re-pulling the whole history costs ~20 requests, so
    rebuilding is both cheap and always self-consistent. That also makes a repeated
    daily run safe — no duplicates, and revisions are picked up.
    """
    symbols = registry_symbols(database) if use_registry else None
    if use_registry and not symbols:
        logger.error("STOCKS_IN_DB gave no symbols; refusing to load unfiltered "
                     "corporate actions into %s", CORPORATE_ACTIONS_TABLE)
        return "Failure"

    data_to_load = fetch_corporate_actions(from_date, to_date, symbols=symbols)
    if data_to_load.empty:
        logger.error("No corporate actions fetched; leaving %s untouched",
                     CORPORATE_ACTIONS_TABLE)
        return "Failure"

    msg = rd.load_sql_data(data_to_load, table_name=CORPORATE_ACTIONS_TABLE,
                           database=database, load_type='replace')
    logger.info("%s rows for %s symbols: %s", len(data_to_load),
                data_to_load['Symbol'].nunique(), msg)
    return "Success" if "success" in msg else "Failure"


def get_bhavcopy(date, load=False):
    date = date.replace("-","")
    payload=pd.read_csv("https://archives.nseindia.com/products/content/sec_bhavdata_full_"+date+".csv")
    if load:
        rd.load_sql_data(payload, table_name='BHAVCOPY')
        return "Success"
    else:
        return payload
    return payload

def load_index_pe_pb_div(symbol='NIFTY 50',start_date='1-1-2007',end_date=datetime.datetime.now().strftime('%d-%m-%Y')):
    """
    Load index PE PB Div data to SQL
    :param symbol: Index symbol or list of index symbols
    :param start_date: Start date
    :param end_date: End date
    :return: Success or Failure message
    """
    if isinstance(symbol, list):
        if len(symbol) == 0:
            return "No symbols provided"
        load_msgs = []
        for index in symbol:
            # No name translation here: NSE's indicesYield keys off the display name.
            # The old dict mapped to niftyindices.com's abbreviations ("Nifty100 Liq
            # 15"), which this endpoint answers with zero rows.
            table_name = index.replace('&', 'AND').replace(' ', '_') + '_PE_PB_DIV'

            logger.info("Executing for index: " + index)
            try:
                # Fetch max timestamp from the table_name in the database
                try:
                    start_date = "1-1-2007"
                    max_timestamp = rd.get_table_data(selected_table=table_name, query=f'SELECT MAX(timestamp) as timestamp FROM "{table_name.upper()}"')
                    logger.info("Max timestamp for index: " + index + " is " + str(max_timestamp.iloc[0]['timestamp']))
                    if not max_timestamp.empty:
                        start_date = max_timestamp.iloc[0]['timestamp'] + dt.timedelta(days=1)
                        start_date = pd.to_datetime(start_date).strftime('%d-%m-%Y')

                    # Validate dates before comparison
                    try:
                        start_date_parsed = pd.to_datetime(start_date, format='%d-%m-%Y')
                        end_date_parsed = pd.to_datetime(end_date, format='%d-%m-%Y')
                        
                        if start_date_parsed > end_date_parsed:
                            logger.debug("Skipping data fetch for index: " + index + " as start date is greater than end date")
                            load_msgs.append(True)
                            continue
                    except ValueError as ve:
                        logger.error(f"Invalid date format for index {index}. Start: {start_date}, End: {end_date}. Error: {str(ve)}")
                        load_msgs.append(False)
                        continue
                except Exception as e:
                    logger.error("Error in fetching max timestamp for index: " + index + " Error: " + str(e))

                logger.info("Fetching data for index: " + index + " from " + start_date + " to " + end_date)
                
                # Convert date format from DD-MM-YYYY to DD-MMM-YYYY for the API call
                start_date_formatted = pd.to_datetime(start_date, format='%d-%m-%Y').strftime('%d-%b-%Y')
                end_date_formatted = pd.to_datetime(end_date, format='%d-%m-%Y').strftime('%d-%b-%Y')
                
                # Pass the index name through untouched: the '&' -> 'AND' rewrite was
                # for niftyindices.com and makes NSE's indicesYield return zero rows.
                # table_name above keeps the substitution, since that is a SQL identifier.
                data_to_load = index_pe_pb_div(index, start_date_formatted, end_date_formatted)
                if data_to_load.empty:
                    logger.info("No data found for index or it's already up to date: " + index)
                    load_msgs.append(True)
                    continue
            except Exception as e:
                logger.error("Error in fetching data for index: " + index + " Error: " + str(e))
                load_msgs.append(False)
                continue
            try:
                load_msg = rd.load_sql_data(data_to_load, table_name=table_name, load_type='append' if not max_timestamp.empty else 'replace')
            except Exception as e:
                logger.error("Error in loading data for index: " + index + " Error: " + str(e))
                load_msgs.append(False)
                continue
            logger.debug(load_msg)
            load_msgs.append(True if "success" in load_msg else False)
            time.sleep(1)
        return "Success" if all(load_msgs) else "Failure"
    else:
        data_to_load = index_pe_pb_div(symbol, start_date, end_date)
        load_msg = rd.load_sql_data(data_to_load, table_name=symbol.replace(' ', '_') + '_PE_PB_DIV', load_type='replace')
        logger.debug(load_msg)
        return "Success" if "success" in load_msg else "Failure"

def load_index_and_stocks_data(load_type="Index_data_load", date=None):
    md = MarketData()
    indices_list = md.broad_indices_list + md.sector_indices_list + md.thematic_indices_list
    data_load_msg = ""
    if load_type == "Index_data_load":
        data_load_msg = md.load_indices_data()
    elif load_type == "Index_Stocks_data_load":
        data_load_msg = md.load_index_stocks_data(indices_list)
    elif load_type == "Stocks_Ref_data_load":
        data_load_msg = md.load_all_stocks_table_with_stock_index(indices_list)
    elif load_type == "FnO_snapshot_load":
        data_load_msg = md.load_fno_snapshot_live(mode="pandas")
    elif load_type == "ETF_data_load":
        data_load_msg = md.fetch_and_load_etf_data(load=True)
    elif load_type == "NSE_Events_load":
        data_load_msg = md.fetch_and_load_nse_events()
    elif load_type == "Bhavcopy_data_load":
        data_load_msg = get_bhavcopy(date, load=True)
    elif load_type == "Corporate_Actions_load":
        data_load_msg = load_corporate_actions()
    elif load_type == "Stocks_PE_backfill":
        data_load_msg = backfill_stocks_pe()
    elif load_type == "Stocks_PE_load":
        data_load_msg = load_stocks_pe(date)
    elif load_type == "Index_pe_pb_div_load":
        data_load_msg = load_index_pe_pb_div(symbol=md.broad_indices_list + md.sector_indices_list + md.thematic_indices_list)
    else:
        data_load_msg = "Invalid load type specified"
    return data_load_msg


if __name__ == "__main__":
    broad_indices_list = ["NIFTY 50", "NIFTY NEXT 50", "NIFTY MIDCAP 50", "NIFTY MIDCAP 100", "NIFTY MIDCAP 150",
                          "NIFTY SMALLCAP 250", "NIFTY SMALLCAP 50", "NIFTY SMALLCAP 100", "NIFTY 100", "NIFTY 200",
                          "NIFTY 500", "NIFTY MIDSMALLCAP 400", "NIFTY MIDCAP SELECT", "NIFTY LARGEMIDCAP 250"]

    sector_indices_list = ["NIFTY BANK", "NIFTY FINANCIAL SERVICES", "NIFTY IT", "NIFTY MEDIA", "NIFTY METAL",
                           "NIFTY PHARMA", "NIFTY PSU BANK", "NIFTY REALTY", "NIFTY AUTO", "NIFTY HEALTHCARE INDEX",
                           "NIFTY FMCG", "NIFTY PRIVATE BANK", "NIFTY CONSUMER DURABLES", "NIFTY OIL & GAS"]

    thematic_indices_list = ["NIFTY ENERGY", "NIFTY CPSE", "NIFTY INFRASTRUCTURE", "NIFTY100 LIQUID 15",
                             "NIFTY PSE", "NIFTY COMMODITIES", "NIFTY MNC", "NIFTY INDIA CONSUMPTION",
                             "NIFTY MIDCAP LIQUID 15", "NIFTY SERVICES SECTOR", "NIFTY INDIA DIGITAL",
                             "NIFTY INDIA MANUFACTURING"]
    # sector_indices_list = []
    # broad_indices_list = []
    # thematic_indices_list = []
    # indices_list = broad_indices_list + sector_indices_list + thematic_indices_list
    md = MarketData()
    # print(md.fetch_and_load_etf_data())
    # print(md.get_corporate_actions_data())
    # print(md.nse_events())
    # print(md.load_fno_snapshot_live(mode="pandas"))
    # print(md.get_quote('SBIN'))
    # print(md.fetch_and_load_nse_events())
    # print(md.equity_history('SBIN', 'EQ', '01-01-2023', '01-02-2023'))
    # print(md.security_wise_archive('01-01-2023', '01-01-2024', 'SBIN', series='EQ'))
    # print(md.nse_get_advances_declines())
    # data = index_pe_pb_div("NIFTY INDIA MFG", start_date="1-1-2010",
    #                                      end_date="1-1-2025")
    # print(data)
    print(load_index_pe_pb_div(symbol=md.broad_indices_list + md.sector_indices_list + md.thematic_indices_list))
    # print(md.nse_holidays(type="all", as_df=True, load=True))
    # print(md.load_index_stocks_data(indices_list))
    # print(md.load_all_stocks_table_with_stock_index(indices_list))
    # print(md.get_main_nse_indices_list())
    # print(index_history("NIFTY 50", "01-01-2023", "01-01-2024"))
    # print(index_pe_pb_div("NIFTY 50", "01-01-2023", "01-01-2024"))
    # print(index_total_returns("NIFTY 50", "01-01-2023", "01-01-2024"))
    # print(get_bhavcopy("19-09-2025", True))

