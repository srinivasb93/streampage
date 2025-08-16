"""
This module fetches data from NSE API
"""
import datetime
import json
import pandas as pd
import requests
from common_utils import read_write_sql_data as rd
import datetime as dt
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
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
            logger.info(f"No NSE events found ")
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
    payload = requests.post('https://niftyindices.com/Backpage.aspx/getHistoricalDBtoString', headers=niftyindices_headers,  json=data).json()
    payload = json.loads(payload["d"])
    payload=pd.DataFrame.from_records(payload)
    return payload

def index_pe_pb_div(symbol,start_date,end_date):
    data = {'cinfo': "{'name':'" + symbol + "','startDate':'" + start_date + "','endDate':'" + end_date + "','indexName':'" + symbol + "'}"}
    payload = requests.post('https://niftyindices.com/Backpage.aspx/getpepbHistoricaldataDBtoString', headers=niftyindices_headers,  json=data).json()
    payload = json.loads(payload["d"])
    payload=pd.DataFrame.from_records(payload)
    return payload

def get_bhavcopy(date):
    date = date.replace("-","")
    payload=pd.read_csv("https://archives.nseindia.com/products/content/sec_bhavdata_full_"+date+".csv")
    return payload


def load_index_and_stocks_data(load_type="Index_data_load"):
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
    indices_list = broad_indices_list + sector_indices_list + thematic_indices_list
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
    # data = md.fetch_index_pe_pb_div_data(start_date="1-1-2020",
    #                                      end_date="1-1-2025")
    # print(data)
    # print(md.load_index_stocks_data(indices_list))
    print(md.load_all_stocks_table_with_stock_index(indices_list))
    # print(md.get_main_nse_indices_list())
    # print(index_history("NIFTY 50", "01-01-2023", "01-01-2024"))
    # print(index_pe_pb_div("NIFTY 50", "01-01-2023", "01-01-2024"))
    # print(index_total_returns("NIFTY 50", "01-01-2023", "01-01-2024"))
    # print(get_bhavcopy("2023-01-01"))
