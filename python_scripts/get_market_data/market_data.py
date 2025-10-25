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


def fetch_nse_data(payload):
    try:
        s = requests.Session()
        s.get("https://www.nseindia.com", headers=headers, timeout=10)
        s.get("https://www.nseindia.com/option-chain", headers=headers, timeout=10)
        output = s.get(payload, headers=headers, timeout=10).json()
    except ValueError:
        output = {}
    return output


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
        payload = fetch_nse_data('https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O')
        for m in range(len(payload['data'])):
            if payload['data'][m]['symbol'] == symbol.upper():
                return payload['data'][m]
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

    @staticmethod
    def nse_get_fno_snapshot_live(mode="pandas"):
        try:
            if mode == "pandas":
                positions = fetch_nse_data('https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O')
                df = pd.DataFrame(positions['data']).copy()
                df['lastUpdateTime'] = positions.get("timestamp", datetime.datetime.now())
                df['lastUpdateTime'] = pd.to_datetime(df['lastUpdateTime'], format='%d-%b-%Y %H:%M:%S')
                df.rename(columns={'symbol': 'Symbol', 'open': 'Open', 'dayHigh': 'High', 'dayLow': 'Low',
                                   'lastPrice': 'Close', 'previousClose': 'Prev_Close', 'change': 'Day_Change',
                                   'pChange': 'Pct_Change', 'yearHigh': 'Year_High', 'yearLow': 'Year_Low',
                                   'totalTradedVolume': 'Traded_Volume', 'totalTradedValue': 'Traded_Value',
                                   'lastUpdateTime': 'Last_Updated'}, inplace=True)
                df = df[['Symbol', 'Open', 'High', 'Low', 'Close', 'Prev_Close', 'Day_Change',
                         'Pct_Change', 'Year_High', 'Year_Low', 'Traded_Volume', 'Traded_Value', 'Last_Updated']]
                return df
            else:
                return fetch_nse_data('https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O')
        except:
            return fetch_nse_data('https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O')

    def load_fno_snapshot_live(self, mode="pandas"):
        """
        Load FNO snapshot live data to SQL
        :param mode: 'pandas' or 'raw'
        :return: Success or Failure message
        """
        fno_snapshot = self.nse_get_fno_snapshot_live(mode=mode)
        msg = rd.load_sql_data(data_to_load=fno_snapshot, table_name='FNO_SNAPSHOT_LIVE')
        return "Success" if "success" in msg else "Failure"

    def fetch_and_load_etf_data(self, as_df=True, load=True):
        url = self.base_url + "etf"
        etf_data = fetch_nse_data(url)
        if as_df:
            etf_data = pd.DataFrame(etf_data.get("data", []))
            exp_columns = ['symbol', 'open', 'high', 'low', 'nav', 'qty', 'meta']
            etf_data = etf_data[exp_columns]
            column_mapping = {'symbol': 'Symbol', 'assets': 'Asset_Type', 'open': 'Open', 'high': 'High',
                              'low': 'Low', 'nav': 'Close', 'qty': 'Volume', 'meta': 'Info'}
            etf_data.rename(columns=column_mapping, inplace=True)
            etf_data['Company_Name'] = etf_data['Info'].apply(lambda x: x['companyName'])
            etf_data['Listing_Date'] = etf_data['Info'].apply(lambda x: x['listingDate'])
            etf_data['Listed_Years'] = round(
                (dt.datetime.today() - pd.to_datetime(etf_data['Listing_Date'])).dt.days/365, 1)
            etf_data['Delisted'] = etf_data['Info'].apply(lambda x: x['isDelisted'])
            etf_data['Suspended'] = etf_data['Info'].apply(lambda x: x['isSuspended'])
            etf_data.drop('Info', axis=1, inplace=True)

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

    def get_index_stocks_data(self, stock_idx='NIFTY 50', as_df=True):
        """
        Get index stocks data
        :return:
        """
        url = self.base_url + "equity-stockIndices?index=" + stock_idx.replace("&", "%26") if "&" in stock_idx else \
            self.base_url + "equity-stockIndices?index=" + stock_idx
        index_data = fetch_nse_data(url)
        if as_df:
            index_data = pd.DataFrame(index_data.get("data", []))
            index_data = index_data[['symbol', 'open', 'dayHigh', 'dayLow', 'lastPrice', 'previousClose', 'change',
                                     'pChange', 'yearHigh', 'yearLow', 'totalTradedVolume', 'totalTradedValue',
                                     'lastUpdateTime', 'nearWKH', 'nearWKL', 'perChange365d', 'date365dAgo',
                                     'date30dAgo', 'perChange30d']]
            index_data.rename(columns={'symbol': 'Symbol', 'open': 'Open', 'dayHigh': 'High', 'dayLow': 'Low',
                                       'lastPrice': 'Close', 'previousClose': 'Prev_Close', 'change': 'Day_Change',
                                       'pChange': 'Pct_Change', 'yearHigh': 'Year_High', 'yearLow': 'Year_Low',
                                       'totalTradedVolume': 'Traded_Volume', 'totalTradedValue': 'Traded_Value',
                                       'lastUpdateTime': 'Last_Updated', 'nearWKH': 'WK_High', 'nearWKL': 'WK_Low',
                                       'perChange365d': 'Pct_Change_365d', 'date365dAgo': 'Date_365d_Ago',
                                       'date30dAgo': 'Date_30d_Ago', 'perChange30d': 'Pct_Change_30d'}, inplace=True)
        return index_data

    def load_index_stocks_data(self, indices_list):
        """
        Load indices data
        :param indices_list:
        :return:
        """
        status_list = []
        for stock_index in indices_list:
            data_to_load = self.get_index_stocks_data(stock_idx=stock_index)
            table_name = stock_index.replace(" ", "_") + "_REF"
            if '&' in table_name:
                table_name = table_name.replace('&', 'AND')
            load_msg = rd.load_sql_data(data_to_load, table_name=table_name)
            if 'success' in load_msg:
                status_list.append(True)
            else:
                status_list.append(False)

        return "Success" if all(status_list) else "Failure"

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


# # Nifty Indicies Site

niftyindices_headers = {
    'Connection': 'keep-alive',
    'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'DNT': '1',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua-mobile': '?0',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36',
    'Content-Type': 'application/json; charset=UTF-8',
    'Origin': 'https://niftyindices.com',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
    'Referer': 'https://niftyindices.com/reports/historical-data',
    'Accept-Language': 'en-US,en;q=0.9,hi;q=0.8',
}

def index_history(symbol,start_date,end_date):
    data = {'cinfo': "{'name':'" + symbol + "','startDate':'" + start_date + "','endDate':'" + end_date + "','indexName':'" + symbol + "'}"}
    payload = requests.post('https://niftyindices.com/Backpage.aspx/getHistoricaldatatabletoString', headers=niftyindices_headers,  json=data).json()
    payload = json.loads(payload["d"])
    payload=pd.DataFrame.from_records(payload)
    payload.rename(columns={'Date': 'timestamp', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close'}, inplace=True)
    payload['timestamp'] = pd.to_datetime(payload['timestamp'])
    payload['volume'] = 0
    payload = payload[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()
    return payload

def index_pe_pb_div(symbol,start_date,end_date):
    data = {'cinfo': "{'name':'" + symbol + "','startDate':'" + start_date + "','endDate':'" + end_date + "','indexName':'" + symbol + "'}"}
    payload = requests.post('https://niftyindices.com/Backpage.aspx/getpepbHistoricaldataDBtoString', headers=niftyindices_headers,  json=data).json()
    payload = json.loads(payload["d"])
    payload=pd.DataFrame.from_records(payload)

    if payload.empty:
        return pd.DataFrame()
    payload.rename(columns={'DATE': 'timestamp', 'Index Name': 'index_name', 'divYield': 'div_yield', 'pe': 'pe', 'pb': 'pb'}, inplace=True)
    payload['timestamp'] = pd.to_datetime(payload['timestamp'])
    payload = payload[['timestamp', 'index_name', 'div_yield', 'pe', 'pb']].copy()
    return payload

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
            symbol_mapping = {"NIFTY100 LIQUID 15": "Nifty100 Liq 15",
                            "NIFTY MIDCAP LIQUID 15": "Nifty Mid Liq 15",
                            "NIFTY INDIA DIGITAL": "Nifty Ind Digital",
                            "NIFTY SMALLCAP 250": "NIFTY SMLCAP 250",
                            "NIFTY SMALLCAP 50": "NIFTY SMLCAP 50",
                            "NIFTY SMALLCAP 100": "NIFTY SMLCAP 100",
                            "NIFTY MIDSMALLCAP 400": "NIFTY MIDSML 400",
                            "NIFTY MIDCAP SELECT": "NIFTY MID SELECT",
                            "NIFTY LARGEMIDCAP 250": "NIFTY LARGEMID250",
                            "NIFTY HEALTHCARE INDEX": "NIFTY HEALTHCARE",
                            "NIFTY CONSUMER DURABLES": "NIFTY CONSR DURBL",
                            "NIFTY FINANCIAL SERVICES": "Nifty Fin Service",
                            "NIFTY PRIVATE BANK": "Nifty Pvt Bank",
                            "NIFTY INFRASTRUCTURE": "Nifty Infra",
                            "NIFTY SERVICES SECTOR": "Nifty Serv Sector",
                            "NIFTY INDIA CONSUMPTION": "Nifty Consumption",
                            "NIFTY INDIA MANUFACTURING": "NIFTY INDIA MFG"
                            }
            table_name = index.replace('&', 'AND').replace(' ', '_') + '_PE_PB_DIV'
            if index in symbol_mapping.keys():
                index = symbol_mapping[index]
 
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
                
                data_to_load = index_pe_pb_div(index.replace('&', 'AND'), start_date_formatted, end_date_formatted) 
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

